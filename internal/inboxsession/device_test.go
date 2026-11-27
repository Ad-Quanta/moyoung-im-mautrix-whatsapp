package inboxsession

import (
	"context"
	"encoding/json"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// 用既有登录记录的 JSON 和 ID 恢复设备，并保证解绑记录不触发设备查询。
func TestRestoreDeviceFromLegacyMetadata(t *testing.T) {
	var metadata UserLoginMetadata
	if err := json.Unmarshal([]byte(`{"wa_device_id":7,"timezone":"UTC","logged_in_at":1700000000,"wasa_resynced":true}`), &metadata); err != nil {
		t.Fatal(err)
	}
	if !metadata.WASAResynced || metadata.LoggedInAt.Unix() != 1700000000 {
		t.Fatalf("metadata: %+v", metadata)
	}
	const loginID = "15555550123"
	want := types.JID{User: loginID, Server: types.DefaultUserServer, Device: 7}
	device := &store.Device{ID: &want}
	calls := 0
	lookup := func(_ context.Context, jid types.JID) (*store.Device, error) {
		calls++
		if jid != want {
			t.Fatalf("device JID = %s, want %s", jid, want)
		}
		return device, nil
	}
	jid, restored, err := RestoreDevice(t.Context(), loginID, &metadata, lookup)
	if err != nil || jid != want || restored != device || calls != 1 || LoginID(jid) != loginID {
		t.Fatalf("unexpected restore result: jid=%s restored=%t calls=%d err=%v", jid, restored != nil, calls, err)
	}
	metadata.WADeviceID = 0
	jid, restored, err = RestoreDevice(t.Context(), loginID, &metadata, lookup)
	if err != nil || !jid.IsEmpty() || restored != nil || calls != 1 {
		t.Fatalf("unexpected unlinked restore result: jid=%s restored=%t calls=%d err=%v", jid, restored != nil, calls, err)
	}
}
