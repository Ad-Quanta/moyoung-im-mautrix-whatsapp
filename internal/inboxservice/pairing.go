package inboxservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// NewPairing 为 API 创建扫码或手机号配对，调用方负责取消未完成的配对。
func (m *Manager) NewPairing(phoneCode bool) (*inboxsession.Pairing, error) {
	m.life.RLock()
	defer m.life.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}
	return inboxsession.NewPairing(phoneCode, m.log, m.devices.NewDevice, func(ctx context.Context, client *whatsmeow.Client) error {
		// 配对阶段也禁止下载历史消息包。
		client.ManualHistorySyncDownload = true
		if m.config.ConfigureClient != nil {
			return m.config.ConfigureClient(ctx, client, true)
		}
		return nil
	}), nil
}

// CompletePairing 从已保存的设备创建运行会话，配对状态机已关闭原连接并移除事件处理器。
func (m *Manager) CompletePairing(ctx context.Context, owner string, pairing *inboxsession.Pairing, success *events.PairSuccess) (Snapshot, error) {
	owner, err := inboxsession.NormalizeBindingKey(owner)
	if err != nil {
		return Snapshot{}, err
	}
	if pairing == nil || success == nil || success.ID.Device == 0 || inboxsession.LoginID(success.ID) == "" {
		return Snapshot{}, errors.New("pairing result has no valid WhatsApp device")
	}
	if pairing.Client() == nil {
		return Snapshot{}, errors.New("pairing client is unavailable")
	}
	id := inboxsession.LoginID(success.ID)
	m.life.RLock()
	defer m.life.RUnlock()
	if err = m.ready(); err != nil {
		return Snapshot{}, err
	}
	defer m.byID.WithLock(id)()
	ctx, done := m.operationContext(ctx)
	defer done()
	previous, err := m.logins.GetByID(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	if previous != nil && previous.Owner != owner {
		return Snapshot{}, inboxsession.ErrLoginOwned
	}
	// 未收尾的严格注销仍持有这一账号的设备代次，禁止覆盖它的登录记录。
	stage, err := m.getAccountCleanupStage(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	if stage != nil && stage.RemoteLogoutCompleted && !stage.CleanupCompleted {
		return Snapshot{}, ErrCleanupPending
	}
	device, err := m.devices.GetDevice(ctx, success.ID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("verify paired device: %w", err)
	}
	if device == nil {
		return Snapshot{}, errors.New("paired device is missing from storage")
	}
	record, err := pairedRecord(owner, success, previous)
	if err != nil {
		return Snapshot{}, err
	}
	if err = m.logins.SavePaired(ctx, record); err != nil {
		return Snapshot{}, fmt.Errorf("save paired login: %w", err)
	}
	if existing := m.get(id); existing != nil {
		existing.stop()
	}
	// 配对状态机只负责完成认证，持久化后由独立会话运行时重新创建事件处理器。
	pairing.Cancel()
	s := newSession(record)
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	m.connect(s)
	return s.snapshot(), nil
}

func pairedRecord(owner string, success *events.PairSuccess, previous *inboxsession.LoginRecord) (*inboxsession.LoginRecord, error) {
	id := inboxsession.LoginID(success.ID)
	profile := map[string]json.RawMessage{}
	metadata := map[string]json.RawMessage{}
	if previous != nil {
		// 保留旧版本保存的未知字段和历史 LID，当前配对只更新设备号和配对时间。
		if len(previous.RemoteProfile) > 0 {
			if err := json.Unmarshal(previous.RemoteProfile, &profile); err != nil {
				return nil, fmt.Errorf("decode previous remote profile: %w", err)
			}
		}
		if err := json.Unmarshal(previous.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("decode previous metadata: %w", err)
		}
		if profile == nil {
			profile = map[string]json.RawMessage{}
		}
		if metadata == nil {
			metadata = map[string]json.RawMessage{}
		}
	}
	phone := "+" + id
	profile["phone"], _ = json.Marshal(phone)
	if success.BusinessName != "" {
		profile["name"], _ = json.Marshal(success.BusinessName)
	}
	metadata["wa_device_id"], _ = json.Marshal(success.ID.Device)
	metadata["logged_in_at"], _ = json.Marshal(jsontime.UnixNow())
	delete(metadata, "history_sync_portals_need_creating")
	profileJSON, err := json.Marshal(profile)
	if err != nil {
		return nil, err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	return &inboxsession.LoginRecord{Owner: owner, ID: id, RemoteName: phone, RemoteProfile: profileJSON, Metadata: metadataJSON}, nil
}
