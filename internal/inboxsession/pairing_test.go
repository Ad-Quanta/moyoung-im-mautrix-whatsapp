package inboxsession

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// 手机号输入步骤可以在不连接 WhatsApp 的情况下创建，取消可以重复调用。
func TestPhonePairingStartAndCancel(t *testing.T) {
	pairing := NewPairing(true, zerolog.Nop(), func() *store.Device { return &store.Device{} }, nil)
	step, err := pairing.Start(t.Context(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if step.Type != StepUserInput || step.StepID != LoginStepIDPhoneNumber {
		t.Fatalf("unexpected step: %+v", step)
	}
	pairing.Cancel()
	pairing.Cancel()
	if _, err = pairing.Start(t.Context(), "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("start after cancel: %v", err)
	}
}

// 初始化失败后仍能调用正常取消路径，且不会要求建立网络连接。
func TestCancelAfterClientSetupError(t *testing.T) {
	want := errors.New("proxy unavailable")
	pairing := NewPairing(false, zerolog.Nop(), func() *store.Device { return &store.Device{} }, func(context.Context, *whatsmeow.Client) error { return want })
	if _, err := pairing.Start(t.Context(), "", nil); !errors.Is(err, want) {
		t.Fatalf("start: %v", err)
	}
	pairing.Cancel()
}

// 模拟 WhatsApp 事件，验证轮换二维码和配对结束时的设备身份交接。
func TestPairingQRRotationAndComplete(t *testing.T) {
	pairing := NewPairing(false, zerolog.Nop(), nil, nil)
	ctx := t.Context()
	evt := &events.QR{Codes: []string{"code-old-secret"}}
	pairing.handleEvent(evt)
	pairing.handleEvent(&events.RotateADVSecret{OldSecret: "old-secret", NewSecret: "new-secret"})
	step, err := pairing.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if step.Type != StepDisplayAndWait || step.Display.Data != "code-new-secret" || evt.Codes[0] != "code-old-secret" {
		t.Fatalf("unexpected QR step: %+v, source codes: %v", step, evt.Codes)
	}
	jid := DeviceJID("15555550123", 7)
	pairing.handleEvent(&events.PairSuccess{ID: jid, BusinessName: "Example"})
	pairing.handleEvent(&events.ManualLoginReconnect{})
	step, err = pairing.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if step.Type != StepComplete || step.Success.ID != jid || step.Success.ID.Server != types.DefaultUserServer {
		t.Fatalf("unexpected completion step: %+v", step)
	}
}
