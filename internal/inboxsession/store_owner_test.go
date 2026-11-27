package inboxsession

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
)

func TestSavePairedOwnerIsolation(t *testing.T) {
	raw, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetMaxOpenConns(1)
	ctx := context.Background()
	_, err = raw.ExecContext(ctx, `CREATE TABLE "user" (bridge_id TEXT NOT NULL, mxid TEXT NOT NULL, PRIMARY KEY (bridge_id, mxid));
		CREATE TABLE user_login (bridge_id TEXT NOT NULL, user_mxid TEXT NOT NULL, id TEXT NOT NULL, remote_name TEXT NOT NULL, remote_profile JSONB, space_room TEXT, metadata JSONB NOT NULL, PRIMARY KEY (bridge_id, id));`)
	if err != nil {
		t.Fatal(err)
	}
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	store := NewStore(db, "wa")
	first := &LoginRecord{Owner: "wa-bind:11111111-1111-1111-1111-111111111111", ID: "15550001", RemoteName: "+15550001", Metadata: json.RawMessage(`{"wa_device_id":1}`)}
	if err = store.SavePaired(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := *first
	second.Owner = "wa-bind:22222222-2222-2222-2222-222222222222"
	if err = store.SavePaired(ctx, &second); err != ErrLoginOwned {
		t.Fatalf("error=%v, want ErrLoginOwned", err)
	}
	current, err := store.GetByID(ctx, first.ID)
	if err != nil || current == nil || current.Owner != first.Owner {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	if err = store.DeleteByOwner(ctx, second.Owner, first.ID); err != nil {
		t.Fatal(err)
	}
	if current, err = store.GetByID(ctx, first.ID); err != nil || current == nil {
		t.Fatalf("wrong owner deleted current=%+v err=%v", current, err)
	}
	if err = store.DeleteByOwner(ctx, first.Owner, first.ID); err != nil {
		t.Fatal(err)
	}
	if current, err = store.GetByID(ctx, first.ID); err != nil || current != nil {
		t.Fatalf("owner delete current=%+v err=%v", current, err)
	}
}
