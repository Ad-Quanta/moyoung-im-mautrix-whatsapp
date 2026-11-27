package inboxservice

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type portalCleanupKey struct {
	ID             string
	Receiver       string
	ParentID       string
	ParentReceiver string
}

func logoutWhatsAppRemoteOnly(ctx context.Context, client *whatsmeow.Client, deviceJID string) error {
	if client == nil {
		return whatsmeow.ErrClientIsNil
	} else if client.MessengerConfig != nil {
		return fmt.Errorf("can't logout with Messenger credentials")
	}
	storedID, err := types.ParseJID(deviceJID)
	if err != nil || storedID.IsEmpty() {
		return fmt.Errorf("invalid WhatsApp device identity: %q", deviceJID)
	}
	ownID := client.DangerousInternals().GetOwnID()
	if ownID.IsEmpty() || ownID != storedID {
		return fmt.Errorf("connected WhatsApp identity does not match stored device identity")
	}
	_, err = client.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
		Namespace: "md",
		Type:      whatsmeow.DangerousInfoQueryType("set"),
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "remove-companion-device",
			Attrs: waBinary.Attrs{
				"jid":    ownID,
				"reason": "user_initiated",
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("send remove-companion-device request: %w", err)
	}
	return nil
}

func (m *Manager) getAccountCleanupStage(ctx context.Context, loginID string) (*accountCleanupStage, error) {
	stage := &accountCleanupStage{}
	err := m.db.QueryRow(ctx, `
		SELECT user_mxid, device_jid, remote_logout_completed, cleanup_completed, removed_json
		FROM whatsapp_account_cleanup WHERE bridge_id=$1 AND user_login_id=$2
	`, m.bridgeID, loginID).Scan(
		&stage.UserMXID, &stage.DeviceJID, &stage.RemoteLogoutCompleted, &stage.CleanupCompleted, &stage.RemovedJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return stage, err
}

func (m *Manager) saveAccountCleanupStage(ctx context.Context, loginID string, stage *accountCleanupStage) error {
	_, err := m.db.Exec(ctx, `
		INSERT INTO whatsapp_account_cleanup (
			bridge_id, user_login_id, user_mxid, device_jid,
			remote_logout_completed, cleanup_completed, removed_json, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (bridge_id, user_login_id) DO UPDATE SET
			user_mxid=excluded.user_mxid,
			device_jid=excluded.device_jid,
			remote_logout_completed=excluded.remote_logout_completed,
			cleanup_completed=excluded.cleanup_completed,
			removed_json=excluded.removed_json,
			updated_at=excluded.updated_at
	`, m.bridgeID, loginID, stage.UserMXID, stage.DeviceJID,
		stage.RemoteLogoutCompleted, stage.CleanupCompleted, stage.RemovedJSON, time.Now().UnixMilli())
	return err
}

func getDeletablePortalKeys(ctx context.Context, db *dbutil.Database, bridgeID string, loginID string) ([]portalCleanupKey, error) {
	rows, err := db.Query(ctx, `
		SELECT p.id, p.receiver, COALESCE(p.parent_id, ''), p.parent_receiver,
			(
				(p.receiver=$2 OR EXISTS (
					SELECT 1 FROM user_portal current_up
					WHERE current_up.bridge_id=p.bridge_id AND current_up.portal_id=p.id
					  AND current_up.portal_receiver=p.receiver AND current_up.login_id=$2
				)) AND NOT EXISTS (
					SELECT 1 FROM user_portal other_up
					WHERE other_up.bridge_id=p.bridge_id AND other_up.portal_id=p.id
					  AND other_up.portal_receiver=p.receiver AND other_up.login_id<>$2
				)
			) AS owned_exclusively
		FROM portal p WHERE p.bridge_id=$1
	`, bridgeID, loginID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var allKeys []portalCleanupKey
	eligible := make(map[[2]string]bool)
	for rows.Next() {
		var key portalCleanupKey
		var ownedExclusively bool
		if err = rows.Scan(&key.ID, &key.Receiver, &key.ParentID, &key.ParentReceiver, &ownedExclusively); err != nil {
			return nil, err
		}
		allKeys = append(allKeys, key)
		if ownedExclusively {
			eligible[[2]string{key.ID, key.Receiver}] = true
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	// 父 portal 若还有不属于当前账号的子 portal，就必须保留，不能破坏其它账号的层级关系。
	for changed := true; changed; {
		changed = false
		for _, parent := range allKeys {
			parentKey := [2]string{parent.ID, parent.Receiver}
			if !eligible[parentKey] {
				continue
			}
			for _, child := range allKeys {
				if child.ParentID == parent.ID && child.ParentReceiver == parent.Receiver && !eligible[[2]string{child.ID, child.Receiver}] {
					delete(eligible, parentKey)
					changed = true
					break
				}
			}
		}
	}
	keys := make([]portalCleanupKey, 0, len(eligible))
	for _, key := range allKeys {
		if eligible[[2]string{key.ID, key.Receiver}] {
			keys = append(keys, key)
		}
	}
	depthMemo := make(map[[2]string]int)
	var depth func(portalCleanupKey, map[[2]string]bool) (int, error)
	depth = func(key portalCleanupKey, visiting map[[2]string]bool) (int, error) {
		keyID := [2]string{key.ID, key.Receiver}
		if cached, ok := depthMemo[keyID]; ok {
			return cached, nil
		}
		if visiting[keyID] {
			return 0, fmt.Errorf("portal parent cycle at %s", key.ID)
		}
		visiting[keyID] = true
		parentID := [2]string{key.ParentID, key.ParentReceiver}
		if key.ParentID != "" && eligible[parentID] {
			for _, parent := range keys {
				if [2]string{parent.ID, parent.Receiver} == parentID {
					parentDepth, depthErr := depth(parent, visiting)
					delete(visiting, keyID)
					if depthErr != nil {
						return 0, depthErr
					}
					depthMemo[keyID] = parentDepth + 1
					return parentDepth + 1, nil
				}
			}
		}
		delete(visiting, keyID)
		depthMemo[keyID] = 0
		return 0, nil
	}
	depths := make(map[[2]string]int, len(keys))
	for _, key := range keys {
		value, depthErr := depth(key, make(map[[2]string]bool))
		if depthErr != nil {
			return nil, depthErr
		}
		depths[[2]string{key.ID, key.Receiver}] = value
	}
	sort.Slice(keys, func(i, j int) bool {
		left := depths[[2]string{keys[i].ID, keys[i].Receiver}]
		right := depths[[2]string{keys[j].ID, keys[j].Receiver}]
		return left > right
	})
	return keys, nil
}

func rowsAffected(result sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (m *Manager) deleteAccountData(
	ctx context.Context,
	loginID string,
	userMXID string,
	deviceJID string,
) (map[string]int64, error) {
	removed := make(map[string]int64)
	var portalKeys []portalCleanupKey
	// UPDATE 自身字段以在事务内锁定账号行，避免依赖 SQLite 不支持的 FOR UPDATE。
	if _, err := m.db.Exec(ctx, `UPDATE "user" SET mxid=mxid WHERE bridge_id=$1 AND mxid=$2`, m.bridgeID, userMXID); err != nil {
		return nil, fmt.Errorf("lock bridge user row: %w", err)
	}
	var owner string
	ownerErr := m.db.QueryRow(ctx, `SELECT user_mxid FROM user_login WHERE bridge_id=$1 AND id=$2`, m.bridgeID, loginID).Scan(&owner)
	if ownerErr != nil && !errors.Is(ownerErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("verify login owner in cleanup transaction: %w", ownerErr)
	} else if ownerErr == nil && owner != userMXID {
		return nil, fmt.Errorf("login owner changed before cleanup")
	}
	var portalErr error
	portalKeys, portalErr = getDeletablePortalKeys(ctx, m.db, m.bridgeID, loginID)
	if portalErr != nil {
		return nil, fmt.Errorf("load account portals: %w", portalErr)
	}
	queries := []struct {
		name  string
		query string
		args  []any
	}{
		{"whatsmeow_privacy_token", `DELETE FROM whatsmeow_privacy_tokens WHERE our_jid=$1`, []any{deviceJID}},
	}
	for _, item := range queries {
		count, execErr := rowsAffected(m.db.Exec(ctx, item.query, item.args...))
		if execErr != nil {
			return nil, fmt.Errorf("delete %s: %w", item.name, execErr)
		}
		removed[item.name] = count
	}
	backfillCount, execErr := rowsAffected(m.db.Exec(ctx,
		`DELETE FROM backfill_task WHERE bridge_id=$1 AND user_login_id=$2`, m.bridgeID, loginID))
	if execErr != nil {
		return nil, fmt.Errorf("delete backfill tasks: %w", execErr)
	}
	removed["backfill_task"] = backfillCount
	removed["portal"] = 0
	// portalKeys 已按子项优先排序；未独占的父子链路不进入删除集合。
	for _, key := range portalKeys {
		count, execErr := rowsAffected(m.db.Exec(ctx,
			`DELETE FROM portal WHERE bridge_id=$1 AND id=$2 AND receiver=$3`,
			m.bridgeID, key.ID, key.Receiver,
		))
		if execErr != nil {
			return nil, fmt.Errorf("delete portal %s: %w", key.ID, execErr)
		}
		removed["portal"] += count
	}
	count, execErr := rowsAffected(m.db.Exec(ctx,
		`DELETE FROM user_login WHERE bridge_id=$1 AND id=$2`, m.bridgeID, loginID,
	))
	if execErr != nil {
		return nil, fmt.Errorf("delete user_login: %w", execErr)
	}
	removed["user_login"] = count
	if ownerErr != nil && count != 0 {
		return nil, fmt.Errorf("login appeared during cleanup retry")
	}
	var remainingLogins int64
	if execErr = m.db.QueryRow(ctx, `SELECT COUNT(*) FROM user_login WHERE bridge_id=$1 AND user_mxid=$2`, m.bridgeID, userMXID).Scan(&remainingLogins); execErr != nil {
		return nil, fmt.Errorf("count remaining user logins: %w", execErr)
	}
	if remainingLogins == 0 {
		count, execErr = rowsAffected(m.db.Exec(ctx, `DELETE FROM "user" WHERE bridge_id=$1 AND mxid=$2`, m.bridgeID, userMXID))
		if execErr != nil {
			return nil, fmt.Errorf("delete bridge user token: %w", execErr)
		}
		removed["bridge_user"] = count
	}
	return removed, nil
}

func (m *Manager) verifyAccountDeleted(ctx context.Context, loginID string, deviceJID string) error {
	checks := []struct {
		name  string
		query string
		args  []any
	}{
		{"user_login", `SELECT COUNT(*) FROM user_login WHERE bridge_id=$1 AND id=$2`, []any{m.bridgeID, loginID}},
		{"user_portal", `SELECT COUNT(*) FROM user_portal WHERE bridge_id=$1 AND login_id=$2`, []any{m.bridgeID, loginID}},
		{"backfill_task", `SELECT COUNT(*) FROM backfill_task WHERE bridge_id=$1 AND user_login_id=$2`, []any{m.bridgeID, loginID}},
		{"history_sync_conversation", `SELECT COUNT(*) FROM whatsapp_history_sync_conversation WHERE bridge_id=$1 AND user_login_id=$2`, []any{m.bridgeID, loginID}},
		{"history_sync_message", `SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE bridge_id=$1 AND user_login_id=$2`, []any{m.bridgeID, loginID}},
		{"media_backfill_request", `SELECT COUNT(*) FROM whatsapp_media_backfill_request WHERE bridge_id=$1 AND user_login_id=$2`, []any{m.bridgeID, loginID}},
		{"history_sync_notification", `SELECT COUNT(*) FROM whatsapp_history_sync_notification WHERE bridge_id=$1 AND user_login_id=$2`, []any{m.bridgeID, loginID}},
		{"whatsmeow_device", `SELECT COUNT(*) FROM whatsmeow_device WHERE jid=$1`, []any{deviceJID}},
		{"whatsmeow_privacy_token", `SELECT COUNT(*) FROM whatsmeow_privacy_tokens WHERE our_jid=$1`, []any{deviceJID}},
	}
	for _, check := range checks {
		var count int64
		if err := m.db.QueryRow(ctx, check.query, check.args...).Scan(&count); err != nil {
			return fmt.Errorf("verify %s: %w", check.name, err)
		} else if count != 0 {
			return fmt.Errorf("verify %s: %d rows remain", check.name, count)
		}
	}
	return nil
}
