// Package inboxservice 管理 WhatsApp 收件箱的设备会话。
package inboxservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/database"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inbox"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exsync"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

var (
	ErrNotFound       = errors.New("WhatsApp login not found")
	ErrNotConnected   = errors.New("WhatsApp session is not connected")
	ErrCleanupPending = errors.New("WhatsApp account cleanup is pending")
	ErrNotStarted     = errors.New("WhatsApp session manager has not started")
)

// Config 保存原生会话需要的网络选项，代理等网络设置由 ConfigureClient 提供。
type Config struct {
	LiveEvents           inbox.LiveEventOptions
	InitialAutoReconnect bool
	UseRetryMessageStore bool
	ForceActiveReceipts  bool
	ConfigureClient      func(context.Context, *whatsmeow.Client, bool) error
}

// Manager 按登录 ID 管理会话，连接、换绑和注销按同一登录串行执行。
type Manager struct {
	ctx      context.Context
	cancel   context.CancelFunc
	log      zerolog.Logger
	config   Config
	db       *dbutil.Database
	bridgeID string
	logins   *inboxsession.Store
	devices  *sqlstore.Container
	push     *inbox.PushClient

	life     sync.RWMutex
	started  bool
	byID     *exsync.KeyedMutex[string]
	mu       sync.RWMutex
	sessions map[string]*session
}

// NewManager 使用调用方按 YAML schema 打开的数据库，沿用已部署的登录和设备表。
func NewManager(ctx context.Context, db *dbutil.Database, bridgeID string, log zerolog.Logger, cfg Config, push *inbox.PushClient) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx: ctx, cancel: cancel, log: log.With().Str("component", "inbox-session").Logger(),
		config: cfg, db: db, bridgeID: bridgeID, logins: inboxsession.NewStore(db, bridgeID), devices: database.NewDeviceStore(db, log),
		push: push, byID: exsync.NewKeyedMutex[string](), sessions: make(map[string]*session),
	}
}

// Start 加载设备映射并恢复登录。调用前须完成统一的数据库 migration。
func (m *Manager) Start(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if m.started {
		return nil
	}
	ctx, done := m.operationContext(ctx)
	defer done()
	if err := m.devices.LIDMap.FillCache(ctx); err != nil {
		return fmt.Errorf("load WhatsApp LID mappings: %w", err)
	}
	records, err := m.logins.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("load WhatsApp logins: %w", err)
	}
	// 远端已解绑但本地仍待清理的账号只保留状态，重启后不再尝试连接旧设备。
	rows, err := m.db.Query(ctx, `SELECT user_login_id, device_jid FROM whatsapp_account_cleanup
        WHERE bridge_id=$1 AND remote_logout_completed AND NOT cleanup_completed`, m.bridgeID)
	pending, err := dbutil.NewRowIterWithError(rows, func(row dbutil.Scannable) ([2]string, error) {
		var entry [2]string
		err := row.Scan(&entry[0], &entry[1])
		return entry, err
	}, err).AsList()
	if err != nil {
		return fmt.Errorf("load pending account cleanups: %w", err)
	}
	devices := make(map[string]string, len(pending))
	for _, entry := range pending {
		devices[entry[0]] = entry[1]
	}
	for _, record := range records {
		s := newSession(record)
		m.sessions[record.ID] = s
		if jid := devices[record.ID]; jid != "" && jid == inboxsession.DeviceJID(record.ID, s.metadata.WADeviceID).String() {
			s.setState(StateLoggedOut, "wa-cleanup-pending")
		} else {
			m.connect(s)
		}
	}
	m.started = true
	return nil
}

// Stop 取消拨号和事件请求，等待客户端关闭。登录行和设备凭据留给下次启动恢复。
func (m *Manager) Stop() {
	m.cancel()
	m.life.Lock()
	defer m.life.Unlock()
	for _, s := range m.sessions {
		s.stop()
	}
}

// List 返回指定绑定的会话状态，包括设备丢失但仍有旧登录行的账号。
func (m *Manager) List(owner string) ([]Snapshot, error) {
	m.life.RLock()
	defer m.life.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}
	owner, err := inboxsession.NormalizeBindingKey(owner)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Snapshot, 0)
	for _, s := range m.sessions {
		if s.record.Owner == owner {
			result = append(result, s.snapshot())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LoginID < result[j].LoginID })
	return result, nil
}

// Connect 启动或重试指定绑定下的会话连接。
func (m *Manager) Connect(owner, loginID string) error {
	return m.withSession(owner, loginID, func(s *session) error {
		if s.state.Load().Error == "wa-cleanup-pending" {
			return ErrCleanupPending
		}
		if s.run != nil {
			// 已在拨号、已连接或由 WhatsMeow 自动重连的会话共用当前客户端。
			state := s.state.Load().StateEvent
			if state == StateConnecting || state == StateConnected || state == StateTransientDisconnect {
				return nil
			}
			s.stop()
		}
		m.connect(s)
		return nil
	})
}

// Disconnect 停止一个会话，之后可继续使用原设备重新连接。
func (m *Manager) Disconnect(owner, loginID string) error {
	return m.withSession(owner, loginID, func(s *session) error {
		if s.state.Load().Error == "wa-cleanup-pending" {
			return nil
		}
		s.stop()
		return nil
	})
}

// Logout 在 WhatsApp 确认可用设备已解绑后删除登录。已有设备缺失的记录可直接移除。
// 严格账号清理的进度回执和旧业务表清理由 account-cleanup 接口单独负责。
func (m *Manager) Logout(ctx context.Context, owner, loginID string) error {
	return m.withSession(owner, loginID, func(s *session) error {
		ctx, done := m.operationContext(ctx)
		defer done()
		stage, err := m.getAccountCleanupStage(ctx, loginID)
		if err != nil {
			return err
		}
		if stage != nil && stage.RemoteLogoutCompleted && !stage.CleanupCompleted {
			return ErrCleanupPending
		}
		_, device, err := inboxsession.RestoreDevice(ctx, loginID, &s.metadata, m.devices.GetDevice)
		if err != nil {
			return fmt.Errorf("load device for logout: %w", err)
		}
		if device != nil {
			client := s.connectedClient()
			if client == nil {
				return ErrNotConnected
			}
			if err = client.Logout(ctx); err != nil {
				return fmt.Errorf("unlink WhatsApp device: %w", err)
			}
		}
		s.stop()
		s.setState(StateLoggedOut, "")
		if err = m.logins.DeleteByOwner(ctx, s.record.Owner, s.record.ID); err != nil {
			return fmt.Errorf("delete WhatsApp login: %w", err)
		}
		m.mu.Lock()
		delete(m.sessions, s.record.ID)
		m.mu.Unlock()
		return nil
	})
}

func (m *Manager) withSession(owner, loginID string, fn func(*session) error) error {
	owner, err := inboxsession.NormalizeBindingKey(owner)
	if err != nil {
		return err
	}
	m.life.RLock()
	defer m.life.RUnlock()
	if err := m.ready(); err != nil {
		return err
	}
	defer m.byID.WithLock(loginID)()
	s := m.get(loginID)
	if s == nil || s.record.Owner != owner {
		return ErrNotFound
	}
	return fn(s)
}

func (m *Manager) get(id string) *session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[id]
}

func (m *Manager) ready() error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if !m.started {
		return ErrNotStarted
	}
	return nil
}

// operationContext 让调用方取消和服务关闭都能终止在途的数据库或网络操作。
func (m *Manager) operationContext(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

func copyJSON(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}
