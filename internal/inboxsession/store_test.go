//go:build cgo

package inboxsession

import (
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
)

// 旧版会话可从原表读取并保留 JSON 原始内容，更新必须匹配登录归属。
func TestStoreLegacyLoginAndOwnerScope(t *testing.T) {
	raw, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	raw.SetMaxOpenConns(1)
	ctx := t.Context()
	_, err = raw.ExecContext(ctx, `CREATE TABLE user_login (
        bridge_id TEXT NOT NULL, user_mxid TEXT NOT NULL, id TEXT NOT NULL,
        remote_name TEXT NOT NULL, remote_profile JSONB, space_room TEXT,
        metadata JSONB NOT NULL, PRIMARY KEY (bridge_id, id)
    )`)
	if err != nil {
		t.Fatal(err)
	}
	const owner = "@binding_legacy:localhost"
	const legacyMeta = `{"wa_device_id":42,"legacy_field":{"preserve":true}}`
	_, err = raw.ExecContext(ctx, `INSERT INTO user_login VALUES ('', $1, '12345', '+12345', $2, NULL, $3)`,
		owner, `{"phone":"+12345","name":"Legacy"}`, legacyMeta)
	if err != nil {
		t.Fatal(err)
	}
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	store := NewStore(db, "")
	records, err := store.ListByOwner(ctx, owner)
	if err != nil || len(records) != 1 {
		t.Fatalf("ListByOwner got %+v, err=%v", records, err)
	}
	login := records[0]
	var metadata UserLoginMetadata
	if err = json.Unmarshal(login.Metadata, &metadata); err != nil || metadata.WADeviceID != 42 || login.SpaceRoom.Valid {
		t.Fatalf("unexpected legacy login %+v, metadata=%+v, err=%v", login, metadata, err)
	}
	if owners, err := store.ListOwners(ctx); err != nil || len(owners) != 1 || owners[0] != owner {
		t.Fatalf("ListOwners got %v, err=%v", owners, err)
	}
	wrongOwner := *login
	wrongOwner.Owner = "wa-bind:00000000-0000-0000-0000-000000000000"
	wrongOwner.RemoteName = "unexpected"
	if err = store.Update(ctx, &wrongOwner); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetByID(ctx, login.ID)
	if err != nil || current == nil || current.RemoteName != "+12345" || current.Owner != owner {
		t.Fatalf("wrong owner changed login %+v, err=%v", current, err)
	}
	login.RemoteName = "Updated"
	if err = store.Update(ctx, login); err != nil {
		t.Fatal(err)
	}
	current, err = store.GetByID(ctx, login.ID)
	if err != nil || current == nil || current.RemoteName != "Updated" || string(current.Metadata) != legacyMeta || string(current.RemoteProfile) != string(login.RemoteProfile) {
		t.Fatalf("updated login %+v, err=%v", current, err)
	}
	other := NewStore(db, "other")
	if err = other.Insert(ctx, &LoginRecord{Owner: owner, ID: login.ID, RemoteName: "Other", Metadata: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err = store.Delete(ctx, login.ID); err != nil {
		t.Fatal(err)
	}
	if current, err = store.GetByID(ctx, login.ID); err != nil || current != nil {
		t.Fatalf("deleted login %+v, err=%v", current, err)
	}
	if current, err = other.GetByID(ctx, login.ID); err != nil || current == nil || current.RemoteName != "Other" || current.RemoteProfile != nil {
		t.Fatalf("other bridge login %+v, err=%v", current, err)
	}
}
