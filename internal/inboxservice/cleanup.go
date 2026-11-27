package inboxservice

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"go.mau.fi/whatsmeow/types"
)

// CleanupReceipt 在远端解绑和本地清理都完成后返回，可按绑定键重试读取。
type CleanupReceipt struct {
	OK       bool             `json:"ok"`
	LoginID  string           `json:"login_id"`
	Removed  map[string]int64 `json:"removed"`
	Verified bool             `json:"verified"`
}

type accountCleanupStage struct {
	UserMXID              string
	DeviceJID             string
	RemoteLogoutCompleted bool
	CleanupCompleted      bool
	RemovedJSON           string
}

// CleanupAccount 先保存远端解绑结果，再事务性删除设备和旧登录数据并保存回执。
func (m *Manager) CleanupAccount(ctx context.Context, owner, id string) (*CleanupReceipt, error) {
	owner, err := inboxsession.NormalizeBindingKey(owner)
	if err != nil {
		return nil, err
	}
	m.life.RLock()
	defer m.life.RUnlock()
	if err = m.ready(); err != nil {
		return nil, err
	}
	defer m.byID.WithLock(id)()
	ctx, done := m.operationContext(ctx)
	defer done()

	stage, err := m.getAccountCleanupStage(ctx, id)
	if err != nil {
		return nil, err
	}
	record, err := m.logins.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if (record != nil && record.Owner != owner) || (stage != nil && stage.UserMXID != owner && (record == nil || !stage.CleanupCompleted)) {
		return nil, ErrNotFound
	}
	if record == nil && (stage == nil || !stage.RemoteLogoutCompleted) {
		return nil, ErrNotFound
	}
	receipt := &CleanupReceipt{OK: true, LoginID: id, Verified: true}
	if record == nil && stage.CleanupCompleted {
		err = json.Unmarshal([]byte(stage.RemovedJSON), &receipt.Removed)
		if err != nil {
			return nil, err
		}
		return receipt, nil
	}
	if record != nil {
		current := newSession(record)
		if current.metadataErr != nil {
			return nil, current.metadataErr
		}
		jid := inboxsession.DeviceJID(id, current.metadata.WADeviceID)
		if jid.Device == 0 {
			return nil, fmt.Errorf("login has no verifiable WhatsApp device")
		}
		if stage == nil || (stage.CleanupCompleted && stage.DeviceJID != jid.String()) {
			stage = &accountCleanupStage{UserMXID: owner, DeviceJID: jid.String()}
		} else if stage.CleanupCompleted || stage.DeviceJID != jid.String() {
			return nil, fmt.Errorf("cleanup receipt conflicts with current device")
		}
	}
	deviceID, err := types.ParseJID(stage.DeviceJID)
	if err != nil || deviceID.Device == 0 || inboxsession.LoginID(deviceID) != id {
		return nil, fmt.Errorf("invalid persisted WhatsApp device identity")
	}
	s := m.get(id)
	if !stage.RemoteLogoutCompleted {
		if s == nil {
			return nil, ErrNotConnected
		}
		client := s.connectedClient()
		if client == nil {
			return nil, ErrNotConnected
		}
		if err = m.saveAccountCleanupStage(ctx, id, stage); err != nil {
			return nil, err
		}
		if err = logoutWhatsAppRemoteOnly(ctx, client, stage.DeviceJID); err != nil {
			return nil, err
		}
		stage.RemoteLogoutCompleted = true
		// 请求断开后仍尽量保存远端结果，避免调用方重试时再次发起解绑。
		persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err = m.saveAccountCleanupStage(persistCtx, id, stage)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("save remote logout stage: %w", err)
		}
	}
	if s != nil {
		s.stop()
		s.setState(StateLoggedOut, "wa-cleanup-pending")
	}
	err = m.db.DoTxn(ctx, nil, func(tx context.Context) error {
		result, err := rowsAffected(m.db.Exec(tx, `DELETE FROM whatsmeow_device WHERE jid=$1`, stage.DeviceJID))
		if err != nil {
			return err
		}
		removed, err := m.deleteAccountData(tx, id, owner, stage.DeviceJID)
		if err != nil {
			return err
		}
		removed["remote_logout"], removed["whatsmeow_device"] = 1, result
		if err = m.verifyAccountDeleted(tx, id, stage.DeviceJID); err != nil {
			return err
		}
		raw, err := json.Marshal(removed)
		if err != nil {
			return err
		}
		stage.CleanupCompleted, stage.RemovedJSON = true, string(raw)
		receipt.Removed = removed
		return m.saveAccountCleanupStage(tx, id, stage)
	})
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
	return receipt, nil
}
