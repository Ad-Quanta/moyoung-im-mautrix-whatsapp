// Package inboxsession 管理 WhatsApp 原生配对步骤与持久化设备身份。
package inboxsession

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"time"

	"go.mau.fi/util/exerrors"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/random"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

// UserLoginMetadata 沿用 user_login.metadata 的 JSON 字段，支持恢复升级前的会话。
type UserLoginMetadata struct {
	WADeviceID      uint16        `json:"wa_device_id"`
	PhoneLastSeen   jsontime.Unix `json:"phone_last_seen"`
	PhoneLastPinged jsontime.Unix `json:"phone_last_pinged"`
	Timezone        string        `json:"timezone"`
	PushKeys        *PushKeys     `json:"push_keys,omitempty"`
	APNSEncPubKey   []byte        `json:"apns_enc_pubkey,omitempty"`
	APNSEncPrivKey  []byte        `json:"apns_enc_privkey,omitempty"`
	LoggedInAt      jsontime.Unix `json:"logged_in_at,omitzero"`

	AppStateRecoveryAttempted map[appstate.WAPatchName]time.Time `json:"app_state_recovery_attempted,omitempty"`
	WASAResynced              bool                               `json:"wasa_resynced,omitzero"`

	HistorySyncPortalsNeedCreating bool      `json:"history_sync_portals_need_creating,omitzero"`
	ReachoutTimelockUntil          time.Time `json:"reachout_timelock_until,omitzero"`

	PreviousOwnLIDs []types.JID `json:"previous_own_lids,omitempty"`

	Blocklist *types.Blocklist `json:"blocklist,omitempty"`

	MData json.RawMessage `json:"mdata,omitempty"`
}

type PushKeys struct {
	P256DH  []byte `json:"p256dh"`
	Auth    []byte `json:"auth"`
	Private []byte `json:"private"`
}

func (m *UserLoginMetadata) GeneratePushKeys() {
	privateKey := exerrors.Must(ecdh.P256().GenerateKey(rand.Reader))
	m.PushKeys = &PushKeys{
		P256DH:  privateKey.Public().(*ecdh.PublicKey).Bytes(),
		Auth:    random.Bytes(16),
		Private: privateKey.Bytes(),
	}
}

func (m *UserLoginMetadata) LoadTimezone() (*time.Location, error) {
	if m == nil || m.Timezone == "" {
		return nil, nil
	}
	return time.LoadLocation(m.Timezone)
}
