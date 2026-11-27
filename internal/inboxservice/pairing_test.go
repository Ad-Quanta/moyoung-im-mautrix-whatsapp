package inboxservice

import (
	"encoding/json"
	"testing"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestPairedRecordPreservesMetadataWithoutHistorySync(t *testing.T) {
	previous := &inboxsession.LoginRecord{
		Owner: "wa-bind:11111111-1111-1111-1111-111111111111", ID: "15550001",
		RemoteProfile: json.RawMessage(`{"name":"旧名","custom":true}`),
		Metadata:      json.RawMessage(`{"previous_own_lids":[{"server":"lid","user":"77"}],"history_sync_portals_need_creating":true,"custom":true}`),
	}
	record, err := pairedRecord(previous.Owner, &events.PairSuccess{ID: types.NewJID("15550001", types.DefaultUserServer), BusinessName: "新名"}, previous)
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]any
	if err = json.Unmarshal(record.RemoteProfile, &profile); err != nil {
		t.Fatal(err)
	}
	if profile["name"] != "新名" || profile["custom"] != true {
		t.Fatalf("profile=%v", profile)
	}
	var metadata map[string]any
	if err = json.Unmarshal(record.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["custom"] != true || metadata["history_sync_portals_need_creating"] != nil {
		t.Fatalf("metadata=%v", metadata)
	}
	if metadata["wa_device_id"] != float64(0) {
		t.Fatalf("metadata=%v", metadata)
	}
}
