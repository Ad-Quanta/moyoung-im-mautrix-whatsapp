package inboxsession

import (
	"encoding/json"
	"fmt"

	"go.mau.fi/whatsmeow/types/events"
)

const (
	StepUserInput      = "user_input"
	StepDisplayAndWait = "display_and_wait"
	StepWebAuthn       = "webauthn"
	StepComplete       = "complete"
	DisplayQR          = "qr"
	DisplayCode        = "code"
)

// Step 描述 WhatsApp 配对步骤，HTTP 适配器负责序列化及保存已配对账号。
type Step struct {
	Type         string
	StepID       string
	Instructions string
	Display      *DisplayParams
	WebAuthn     *WebAuthnParams
	Success      *events.PairSuccess
}

type DisplayParams struct {
	Type string
	Data string
}
type WebAuthnParams struct {
	URL       string
	PublicKey json.RawMessage
}

// PairingError 携带配对失败原因与兼容接口使用的稳定错误码。
type PairingError struct {
	ErrCode    string
	Err        string
	StatusCode int
}

func (err PairingError) Error() string { return fmt.Sprintf("%s: %s", err.ErrCode, err.Err) }

// RequestError 让调用方沿用 WhatsApp 请求失败的响应映射。
type RequestError struct{ Cause error }

func (err *RequestError) Error() string { return err.Cause.Error() }
func (err *RequestError) Unwrap() error { return err.Cause }
