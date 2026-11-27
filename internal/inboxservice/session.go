package inboxservice

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inbox"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// State 沿用 Router 识别的状态值。
type State string

const (
	StateConnecting          State = "CONNECTING"
	StateConnected           State = "CONNECTED"
	StateTransientDisconnect State = "TRANSIENT_DISCONNECT"
	StateBadCredentials      State = "BAD_CREDENTIALS"
	StateUnknownError        State = "UNKNOWN_ERROR"
	StateLoggedOut           State = "LOGGED_OUT"
	StateStopped             State = "STOPPED"
)

// Status 保存可并发读取的原生连接状态。
type Status struct {
	StateEvent State  `json:"state_event"`
	Timestamp  int64  `json:"timestamp"`
	Error      string `json:"error,omitempty"`
}

// Snapshot 返回旧登录资料和当前会话状态，供 HTTP 层组装 Router 响应。
type Snapshot struct {
	LoginID       string
	RemoteName    string
	RemoteProfile json.RawMessage
	Status        Status
}

type session struct {
	record      *inboxsession.LoginRecord
	metadata    inboxsession.UserLoginMetadata
	metadataErr error
	state       atomic.Pointer[Status]
	// run 的替换由 Manager 的同一登录锁保护，事件闭包只访问创建时捕获的 run。
	run *connection
}

type connection struct {
	ctx       context.Context
	cancel    context.CancelFunc
	dialDone  chan struct{}
	client    atomic.Pointer[whatsmeow.Client]
	handlerID uint32
	live      *inbox.LiveEvents
	media     *inbox.MediaRetry
	ownLID    atomic.Pointer[types.JID]
}

func newSession(record *inboxsession.LoginRecord) *session {
	s := &session{record: record}
	s.metadataErr = json.Unmarshal(record.Metadata, &s.metadata)
	s.setState(StateConnecting, "")
	return s
}

func (s *session) snapshot() Snapshot {
	return Snapshot{LoginID: s.record.ID, RemoteName: s.record.RemoteName,
		RemoteProfile: copyJSON(s.record.RemoteProfile), Status: *s.state.Load()}
}

func (s *session) setState(state State, code string) {
	s.state.Store(&Status{StateEvent: state, Error: code, Timestamp: time.Now().Unix()})
}

func (s *session) connectedClient() *whatsmeow.Client {
	if s.run == nil {
		return nil
	}
	client := s.run.client.Load()
	if client != nil && client.IsLoggedIn() {
		return client
	}
	return nil
}

func (m *Manager) connect(s *session) {
	ctx, cancel := context.WithCancel(m.ctx)
	run := &connection{ctx: ctx, cancel: cancel, dialDone: make(chan struct{})}
	s.run = run
	s.setState(StateConnecting, "")
	go m.dial(s, run)
}

func (m *Manager) dial(s *session, run *connection) {
	defer close(run.dialDone)
	log := m.log.With().Str("login_id", s.record.ID).Logger()
	if s.metadataErr != nil {
		log.Err(s.metadataErr).Msg("无法解析 WhatsApp 登录记录")
		s.setState(StateUnknownError, "wa-invalid-login-metadata")
		return
	}
	ownJID, device, err := inboxsession.RestoreDevice(run.ctx, s.record.ID, &s.metadata, m.devices.GetDevice)
	if err != nil {
		log.Err(err).Msg("无法读取 WhatsApp 设备")
		s.setState(StateUnknownError, "wa-device-restore-failed")
		return
	}
	if device == nil {
		s.setState(StateBadCredentials, "wa-not-logged-in")
		return
	}
	client := m.newClient(run.ctx, log, device)
	if m.config.ConfigureClient != nil {
		if err = m.config.ConfigureClient(run.ctx, client, false); err != nil {
			log.Err(err).Msg("无法配置 WhatsApp 连接")
			s.setState(StateUnknownError, "wa-client-config-failed")
			return
		}
	}
	lid := device.GetLID().ToNonAD()
	run.ownLID.Store(&lid)
	run.live = inbox.NewLiveEvents(run.ctx, s.record.ID, log, client, m.push, m.config.LiveEvents)
	run.media = inbox.NewMediaRetry(run.ctx, client, func(jid types.JID) bool {
		jid = jid.ToNonAD()
		return !jid.IsEmpty() && (jid == ownJID.ToNonAD() || jid == *run.ownLID.Load() || slices.Contains(s.metadata.PreviousOwnLIDs, jid))
	})
	// 每次连接只注册一个运行时事件处理器，配对 socket 不参与这里的消息消费。
	run.handlerID = client.AddEventHandlerWithSuccessStatus(func(raw any) bool {
		return s.handleEvent(log.WithContext(run.ctx), run, client, raw)
	})
	run.client.Store(client)
	if err = client.ConnectContext(run.ctx); err != nil && run.ctx.Err() == nil {
		log.Err(err).Msg("WhatsApp 连接失败")
		current := s.state.Load()
		if current.StateEvent == StateConnecting {
			s.state.CompareAndSwap(current, &Status{StateEvent: StateUnknownError, Error: "wa-connection-failed", Timestamp: time.Now().Unix()})
		}
	}
}

func (m *Manager) newClient(ctx context.Context, log zerolog.Logger, device *store.Device) *whatsmeow.Client {
	client := whatsmeow.NewClient(device, waLog.Zerolog(log))
	client.BackgroundEventCtx = log.WithContext(ctx)
	client.InitialAutoReconnect = m.config.InitialAutoReconnect
	client.UseRetryMessageStore = m.config.UseRetryMessageStore
	client.SetForceActiveDeliveryReceipts(m.config.ForceActiveReceipts)
	client.SynchronousAck = true
	client.EnableDecryptedEventBuffer = true
	// 只接收实时消息，禁止 WhatsMeow 自动下载历史消息包。
	client.ManualHistorySyncDownload = true
	client.SendReportingTokens = true
	client.AutomaticMessageRerequestFromPhone = true
	client.UnifiedSessionInterval = 24 * time.Hour
	return client
}

func (s *session) handleEvent(ctx context.Context, run *connection, client *whatsmeow.Client, raw any) bool {
	if ctx.Err() != nil {
		return false
	}
	switch evt := raw.(type) {
	case *events.Connected:
		lid := client.Store.GetLID().ToNonAD()
		run.ownLID.Store(&lid)
		s.setState(StateConnected, "")
	case *events.KeepAliveRestored, *events.OfflineSyncCompleted:
		if client.IsLoggedIn() {
			s.setState(StateConnected, "")
		}
	case *events.LoggedOut:
		s.setState(StateBadCredentials, "wa-logged-out")
		run.cancel()
	case *events.Disconnected, *events.KeepAliveTimeout:
		current := s.state.Load()
		if current.StateEvent == StateConnected || current.StateEvent == StateConnecting {
			s.state.CompareAndSwap(current, &Status{StateEvent: StateTransientDisconnect, Error: "wa-disconnected", Timestamp: time.Now().Unix()})
		}
	case *events.StreamReplaced:
		s.setState(StateUnknownError, "wa-stream-replaced")
	case *events.ClientOutdated:
		s.setState(StateUnknownError, "wa-client-outdated")
	case *events.TemporaryBan:
		s.setState(StateBadCredentials, "wa-temporary-ban")
	case *events.ConnectFailure, *events.StreamError:
		s.setState(StateUnknownError, "wa-connection-failed")
	case *events.MediaRetry:
		run.media.Handle(ctx, evt)
	default:
		if handled, success := run.live.Handle(ctx, raw); handled {
			return success
		}
	}
	return true
}

func (s *session) stop() {
	if run := s.run; run != nil {
		run.cancel()
		<-run.dialDone
		if client := run.client.Load(); client != nil {
			client.Disconnect()
			client.RemoveEventHandler(run.handlerID)
		}
		s.run = nil
	}
	s.setState(StateStopped, "")
}

// withClient 取得同一连接代的客户端。断开或重新配对时会取消当前请求。
func (m *Manager) withClient(ctx context.Context, owner, id string, fn func(context.Context, *whatsmeow.Client, *inbox.MediaRetry) error) error {
	var client *whatsmeow.Client
	var run *connection
	if err := m.withSession(owner, id, func(s *session) error {
		client = s.connectedClient()
		if client == nil {
			return ErrNotConnected
		}
		run = s.run
		return nil
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(run.ctx, cancel)
	defer cancel()
	defer stop()
	if err := run.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx, client, run.media)
}

// Send 通过所属绑定的连接发送 Inbox 消息。
func (m *Manager) Send(ctx context.Context, owner, id string, req inbox.SendRequest) (result *inbox.SendResponse, err error) {
	err = m.withClient(ctx, owner, id, func(ctx context.Context, client *whatsmeow.Client, _ *inbox.MediaRetry) error {
		result, err = inbox.Send(ctx, client, req)
		return err
	})
	return
}

// Download 下载消息媒体，必要时由同一连接向手机申请新路径。
func (m *Manager) Download(ctx context.Context, owner, id string, req inbox.DownloadRequest) (data []byte, mime string, err error) {
	err = m.withClient(ctx, owner, id, func(ctx context.Context, client *whatsmeow.Client, retry *inbox.MediaRetry) error {
		data, mime, err = inbox.Download(ctx, client, req, retry.Wait)
		return err
	})
	return
}

// Contact 返回客户端可识别的联系人 ID 和用于展示的缓存资料。
type Contact struct {
	ID    string
	Phone string
	Info  types.ContactInfo
}

// Contacts 读取 WhatsMeow 联系人和 LID 映射。已有手机号和 LID 指向同一联系人时只返回一条。
func (m *Manager) Contacts(ctx context.Context, owner, id string) (contacts []Contact, err error) {
	err = m.withClient(ctx, owner, id, func(ctx context.Context, client *whatsmeow.Client, _ *inbox.MediaRetry) error {
		entries, err := client.Store.Contacts.GetAllContacts(ctx)
		if err != nil {
			return err
		}
		byID := make(map[string]Contact, len(entries))
		for jid, info := range entries {
			if info.FirstName == "" && info.FullName == "" {
				continue
			}
			jid = jid.ToNonAD()
			row := Contact{Info: info}
			switch jid.Server {
			case types.DefaultUserServer, types.HiddenUserServer:
				alt, err := client.Store.GetAltJID(ctx, jid)
				if err != nil {
					return err
				}
				row.ID = jid.User
				if jid.Server == types.HiddenUserServer {
					row.ID = "lid-" + jid.User
					if !alt.IsEmpty() {
						row.Phone = "+" + alt.User
					}
				} else {
					row.Phone = "+" + jid.User
					if !alt.IsEmpty() {
						row.ID = "lid-" + alt.User
					}
				}
				if info.PushName == "" {
					row.Info.PushName = entries[alt].PushName
				}
				if info.BusinessName == "" {
					row.Info.BusinessName = entries[alt].BusinessName
				}
			case types.BotServer:
				row.ID = "bot-" + jid.User
			default:
				continue
			}
			if _, exists := byID[row.ID]; !exists || jid.Server == types.DefaultUserServer {
				byID[row.ID] = row
			}
		}
		contacts = make([]Contact, 0, len(byID))
		for _, row := range byID {
			contacts = append(contacts, row)
		}
		return nil
	})
	return
}
