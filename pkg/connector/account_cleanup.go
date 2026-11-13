package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const createAccountCleanupTable = `
CREATE TABLE IF NOT EXISTS whatsapp_account_cleanup (
    bridge_id TEXT NOT NULL,
    user_login_id TEXT NOT NULL,
    user_mxid TEXT NOT NULL,
    device_jid TEXT NOT NULL DEFAULT '',
    remote_logout_completed BOOLEAN NOT NULL DEFAULT false,
    cleanup_completed BOOLEAN NOT NULL DEFAULT false,
    removed_json TEXT NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bridge_id, user_login_id)
)`

type accountCleanupResult struct {
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

type portalCleanupKey struct {
	ID             string
	Receiver       string
	ParentID       string
	ParentReceiver string
}

func (wa *WhatsAppConnector) initAccountCleanup(ctx context.Context) error {
	if _, err := wa.Bridge.DB.Exec(ctx, createAccountCleanupTable); err != nil {
		return err
	}
	provisioning, ok := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return fmt.Errorf("matrix connector does not expose provisioning API")
	}
	wa.accountCleanupRouteOnce.Do(func() {
		provisioning.GetProvisioning().GetRouter().HandleFunc(
			"POST /v3/account-cleanup/{loginID}",
			wa.postAccountCleanup,
		)
	})
	return nil
}

func (wa *WhatsAppConnector) cleanupLock(loginID networkid.UserLoginID) *sync.Mutex {
	lock, _ := wa.accountCleanupLocks.LoadOrStore(loginID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func writeAccountCleanupJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAccountCleanupError(w http.ResponseWriter, statusCode int, err error) {
	writeAccountCleanupJSON(w, statusCode, map[string]string{"error": err.Error()})
}

func (wa *WhatsAppConnector) postAccountCleanup(w http.ResponseWriter, r *http.Request) {
	loginID := networkid.UserLoginID(strings.TrimSpace(r.PathValue("loginID")))
	if loginID == "" || loginID == "all" {
		writeAccountCleanupError(w, http.StatusBadRequest, fmt.Errorf("invalid login ID"))
		return
	}
	provisioning := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning).GetProvisioning()
	user := provisioning.GetUser(r)
	if user == nil {
		writeAccountCleanupError(w, http.StatusUnauthorized, fmt.Errorf("provisioning user is unavailable"))
		return
	}
	lock := wa.cleanupLock(loginID)
	lock.Lock()
	defer lock.Unlock()

	result, err := wa.cleanupAccount(r.Context(), user.MXID, loginID)
	if errors.Is(err, sql.ErrNoRows) {
		writeAccountCleanupError(w, http.StatusNotFound, fmt.Errorf("login not found"))
		return
	} else if err != nil {
		writeAccountCleanupError(w, http.StatusBadGateway, err)
		return
	}
	writeAccountCleanupJSON(w, http.StatusOK, result)
}

func (wa *WhatsAppConnector) cleanupAccount(ctx context.Context, userMXID id.UserID, loginID networkid.UserLoginID) (*accountCleanupResult, error) {
	stage, err := wa.getAccountCleanupStage(ctx, loginID)
	if err != nil {
		return nil, fmt.Errorf("load cleanup stage: %w", err)
	}
	if stage != nil && stage.UserMXID != userMXID.String() {
		return nil, sql.ErrNoRows
	}
	login, err := wa.Bridge.GetExistingUserLoginByID(ctx, loginID)
	if err != nil {
		return nil, fmt.Errorf("load login: %w", err)
	} else if login != nil && login.UserMXID != userMXID {
		return nil, sql.ErrNoRows
	}
	if stage != nil && stage.CleanupCompleted {
		if login == nil {
			removed := make(map[string]int64)
			if err = json.Unmarshal([]byte(stage.RemovedJSON), &removed); err != nil {
				return nil, fmt.Errorf("decode cleanup receipt: %w", err)
			}
			return &accountCleanupResult{OK: true, LoginID: string(loginID), Removed: removed, Verified: true}, nil
		}
		currentClient, ok := login.Client.(*WhatsAppClient)
		if !ok || currentClient.Device == nil || currentClient.Device.ID == nil {
			return nil, fmt.Errorf("completed cleanup receipt conflicts with an unverifiable active login")
		}
		currentDeviceJID := currentClient.Device.ID.String()
		if currentDeviceJID == "" || currentDeviceJID == stage.DeviceJID {
			return nil, fmt.Errorf("completed cleanup receipt conflicts with the same active device")
		}
		// 同一手机号重新配对会复用 UserLoginID，但设备 JID 会变化。旧回执只能证明旧设备，
		// 必须用新设备身份开始新一代清理，禁止把旧回执当成本次注销证明。
		stage = &accountCleanupStage{UserMXID: userMXID.String(), DeviceJID: currentDeviceJID}
		if err = wa.saveAccountCleanupStage(ctx, loginID, stage); err != nil {
			return nil, fmt.Errorf("reset cleanup stage for new device: %w", err)
		}
	}
	var client *WhatsAppClient
	var deviceJID string
	if login != nil {
		var ok bool
		client, ok = login.Client.(*WhatsAppClient)
		if !ok {
			return nil, fmt.Errorf("login has no verifiable WhatsApp device")
		}
		if client.Device != nil && client.Device.ID != nil {
			deviceJID = client.Device.ID.String()
		} else if stage != nil && stage.RemoteLogoutCompleted && stage.DeviceJID != "" {
			deviceJID = stage.DeviceJID
		} else {
			return nil, fmt.Errorf("login has no verifiable WhatsApp device")
		}
	} else if stage != nil && stage.RemoteLogoutCompleted && stage.DeviceJID != "" {
		// 远端解绑完成后即使进程重启，登录行已被部分清理，也必须能继续按持久化设备身份收尾。
		deviceJID = stage.DeviceJID
	} else {
		return nil, sql.ErrNoRows
	}
	if stage == nil {
		stage = &accountCleanupStage{UserMXID: userMXID.String(), DeviceJID: deviceJID}
		if err = wa.saveAccountCleanupStage(ctx, loginID, stage); err != nil {
			return nil, fmt.Errorf("save cleanup stage: %w", err)
		}
	} else if stage.DeviceJID != deviceJID {
		return nil, fmt.Errorf("cleanup device identity changed")
	}

	if !stage.RemoteLogoutCompleted {
		if client == nil || client.Client == nil {
			return nil, fmt.Errorf("WhatsApp client is unavailable for remote logout")
		}
		// 这里只发送 WhatsApp 远端解绑请求，不提前删除本地 store。远端成功阶段先落库，
		// 后续任何本地清理失败或进程重启都能跳过远端操作并安全重试。
		if client.Device == nil || client.Device.ID == nil {
			return nil, fmt.Errorf("WhatsApp client is missing its device identity for remote logout")
		}
		if err = logoutWhatsAppRemoteOnly(ctx, client.Client, deviceJID); err != nil {
			return nil, fmt.Errorf("remote WhatsApp logout: %w", err)
		}
		stage.RemoteLogoutCompleted = true
		if err = wa.saveAccountCleanupStage(ctx, loginID, stage); err != nil {
			return nil, fmt.Errorf("save remote logout stage: %w", err)
		}
	}

	if client != nil {
		client.callStopLoops()
		client.Disconnect()
	}
	deviceID, err := types.ParseJID(deviceJID)
	if err != nil || deviceID.IsEmpty() {
		return nil, fmt.Errorf("invalid persisted WhatsApp device identity %q", deviceJID)
	}
	device, err := wa.DeviceStore.GetDevice(ctx, deviceID)
	deviceRemoved := int64(0)
	if err != nil {
		return nil, fmt.Errorf("load WhatsApp device store: %w", err)
	} else if device != nil {
		err = device.Delete(ctx)
		if err == nil {
			deviceRemoved = 1
		}
	}
	if err != nil {
		return nil, fmt.Errorf("delete WhatsApp device store: %w", err)
	}

	var removed map[string]int64
	var portalKeys []portalCleanupKey
	var clearUser bool
	// 数据库删除与缓存摘除保持同一桥接缓存锁，避免删除期间并发重新创建旧账号对象。
	if err = wa.Bridge.AccountCleanupRunLocked(userMXID, login, func() ([]networkid.PortalKey, bool, error) {
		var deleteErr error
		removed, portalKeys, clearUser, deleteErr = wa.deleteAccountData(ctx, loginID, userMXID, deviceJID)
		if removed != nil {
			removed["remote_logout"] = 1
			removed["whatsmeow_device"] = deviceRemoved
		}
		cacheKeys := make([]networkid.PortalKey, 0, len(portalKeys))
		for _, key := range portalKeys {
			cacheKeys = append(cacheKeys, networkid.PortalKey{
				ID:       networkid.PortalID(key.ID),
				Receiver: networkid.UserLoginID(key.Receiver),
			})
		}
		return cacheKeys, clearUser, deleteErr
	}); err != nil {
		return nil, err
	}
	if err = wa.verifyAccountDeleted(ctx, loginID, deviceJID); err != nil {
		return nil, err
	}

	removedJSON, err := json.Marshal(removed)
	if err != nil {
		return nil, fmt.Errorf("encode cleanup receipt: %w", err)
	}
	stage.CleanupCompleted = true
	// 保留已解绑设备 JID，用于区分同一 UserLoginID 后续产生的新配对代次。
	stage.RemovedJSON = string(removedJSON)
	if err = wa.saveAccountCleanupStage(ctx, loginID, stage); err != nil {
		return nil, fmt.Errorf("save cleanup receipt: %w", err)
	}
	return &accountCleanupResult{OK: true, LoginID: string(loginID), Removed: removed, Verified: true}, nil
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

func (wa *WhatsAppConnector) getAccountCleanupStage(ctx context.Context, loginID networkid.UserLoginID) (*accountCleanupStage, error) {
	stage := &accountCleanupStage{}
	err := wa.Bridge.DB.QueryRow(ctx, `
		SELECT user_mxid, device_jid, remote_logout_completed, cleanup_completed, removed_json
		FROM whatsapp_account_cleanup WHERE bridge_id=$1 AND user_login_id=$2
	`, wa.Bridge.ID, loginID).Scan(
		&stage.UserMXID, &stage.DeviceJID, &stage.RemoteLogoutCompleted, &stage.CleanupCompleted, &stage.RemovedJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return stage, err
}

func (wa *WhatsAppConnector) saveAccountCleanupStage(ctx context.Context, loginID networkid.UserLoginID, stage *accountCleanupStage) error {
	_, err := wa.Bridge.DB.Exec(ctx, `
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
	`, wa.Bridge.ID, loginID, stage.UserMXID, stage.DeviceJID,
		stage.RemoteLogoutCompleted, stage.CleanupCompleted, stage.RemovedJSON, time.Now().UnixMilli())
	return err
}

func getDeletablePortalKeys(ctx context.Context, db *database.Database, bridgeID networkid.BridgeID, loginID networkid.UserLoginID) ([]portalCleanupKey, error) {
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

func (wa *WhatsAppConnector) deleteAccountData(
	ctx context.Context,
	loginID networkid.UserLoginID,
	userMXID id.UserID,
	deviceJID string,
) (map[string]int64, []portalCleanupKey, bool, error) {
	removed := make(map[string]int64)
	var portalKeys []portalCleanupKey
	clearUser := false
	err := wa.Bridge.DB.DoTxn(ctx, nil, func(txCtx context.Context) error {
		// UPDATE 自身字段以在事务内锁定账号行，避免依赖 SQLite 不支持的 FOR UPDATE。
		if _, err := wa.Bridge.DB.Exec(txCtx, `UPDATE "user" SET mxid=mxid WHERE bridge_id=$1 AND mxid=$2`, wa.Bridge.ID, userMXID); err != nil {
			return fmt.Errorf("lock bridge user row: %w", err)
		}
		var owner id.UserID
		ownerErr := wa.Bridge.DB.QueryRow(txCtx, `SELECT user_mxid FROM user_login WHERE bridge_id=$1 AND id=$2`, wa.Bridge.ID, loginID).Scan(&owner)
		if ownerErr != nil && !errors.Is(ownerErr, sql.ErrNoRows) {
			return fmt.Errorf("verify login owner in cleanup transaction: %w", ownerErr)
		} else if ownerErr == nil && owner != userMXID {
			return fmt.Errorf("login owner changed before cleanup")
		}
		var portalErr error
		portalKeys, portalErr = getDeletablePortalKeys(txCtx, wa.Bridge.DB, wa.Bridge.ID, loginID)
		if portalErr != nil {
			return fmt.Errorf("load account portals: %w", portalErr)
		}
		queries := []struct {
			name  string
			query string
			args  []any
		}{
			{"whatsmeow_privacy_token", `DELETE FROM whatsmeow_privacy_tokens WHERE our_jid=$1`, []any{deviceJID}},
		}
		for _, item := range queries {
			count, execErr := rowsAffected(wa.Bridge.DB.Exec(txCtx, item.query, item.args...))
			if execErr != nil {
				return fmt.Errorf("delete %s: %w", item.name, execErr)
			}
			removed[item.name] = count
		}
		backfillCount, execErr := rowsAffected(wa.Bridge.DB.Exec(txCtx,
			`DELETE FROM backfill_task WHERE bridge_id=$1 AND user_login_id=$2`, wa.Bridge.ID, loginID))
		if execErr != nil {
			return fmt.Errorf("delete backfill tasks: %w", execErr)
		}
		removed["backfill_task"] = backfillCount
		removed["portal"] = 0
		// portalKeys 已按子项优先排序；未独占的父子链路不进入删除集合。
		for _, key := range portalKeys {
			count, execErr := rowsAffected(wa.Bridge.DB.Exec(txCtx,
				`DELETE FROM portal WHERE bridge_id=$1 AND id=$2 AND receiver=$3`,
				wa.Bridge.ID, key.ID, key.Receiver,
			))
			if execErr != nil {
				return fmt.Errorf("delete portal %s: %w", key.ID, execErr)
			}
			removed["portal"] += count
		}
		count, execErr := rowsAffected(wa.Bridge.DB.Exec(txCtx,
			`DELETE FROM user_login WHERE bridge_id=$1 AND id=$2`, wa.Bridge.ID, loginID,
		))
		if execErr != nil {
			return fmt.Errorf("delete user_login: %w", execErr)
		}
		removed["user_login"] = count
		if ownerErr != nil && count != 0 {
			return fmt.Errorf("login appeared during cleanup retry")
		}
		var remainingLogins int64
		if execErr = wa.Bridge.DB.QueryRow(txCtx, `SELECT COUNT(*) FROM user_login WHERE bridge_id=$1 AND user_mxid=$2`, wa.Bridge.ID, userMXID).Scan(&remainingLogins); execErr != nil {
			return fmt.Errorf("count remaining user logins: %w", execErr)
		}
		if remainingLogins == 0 {
			clearUser = true
			count, execErr = rowsAffected(wa.Bridge.DB.Exec(txCtx, `DELETE FROM "user" WHERE bridge_id=$1 AND mxid=$2`, wa.Bridge.ID, userMXID))
			if execErr != nil {
				return fmt.Errorf("delete bridge user token: %w", execErr)
			}
			removed["bridge_user"] = count
		}
		return nil
	})
	if err != nil {
		return nil, nil, false, fmt.Errorf("delete account data transaction: %w", err)
	}
	return removed, portalKeys, clearUser, nil
}

func (wa *WhatsAppConnector) verifyAccountDeleted(ctx context.Context, loginID networkid.UserLoginID, deviceJID string) error {
	checks := []struct {
		name  string
		query string
		args  []any
	}{
		{"user_login", `SELECT COUNT(*) FROM user_login WHERE bridge_id=$1 AND id=$2`, []any{wa.Bridge.ID, loginID}},
		{"user_portal", `SELECT COUNT(*) FROM user_portal WHERE bridge_id=$1 AND login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"backfill_task", `SELECT COUNT(*) FROM backfill_task WHERE bridge_id=$1 AND user_login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"history_sync_conversation", `SELECT COUNT(*) FROM whatsapp_history_sync_conversation WHERE bridge_id=$1 AND user_login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"history_sync_message", `SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE bridge_id=$1 AND user_login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"media_backfill_request", `SELECT COUNT(*) FROM whatsapp_media_backfill_request WHERE bridge_id=$1 AND user_login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"history_sync_notification", `SELECT COUNT(*) FROM whatsapp_history_sync_notification WHERE bridge_id=$1 AND user_login_id=$2`, []any{wa.Bridge.ID, loginID}},
		{"whatsmeow_device", `SELECT COUNT(*) FROM whatsmeow_device WHERE jid=$1`, []any{deviceJID}},
		{"whatsmeow_privacy_token", `SELECT COUNT(*) FROM whatsmeow_privacy_tokens WHERE our_jid=$1`, []any{deviceJID}},
	}
	for _, check := range checks {
		var count int64
		if err := wa.Bridge.DB.QueryRow(ctx, check.query, check.args...).Scan(&count); err != nil {
			return fmt.Errorf("verify %s: %w", check.name, err)
		} else if count != 0 {
			return fmt.Errorf("verify %s: %d rows remain", check.name, count)
		}
	}
	return nil
}
