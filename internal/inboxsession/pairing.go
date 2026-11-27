package inboxsession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	LoginStepIDQR          = "fi.mau.whatsapp.login.qr"
	LoginStepIDPhoneNumber = "fi.mau.whatsapp.login.phone"
	LoginStepIDCode        = "fi.mau.whatsapp.login.code"
	LoginStepIDPasskey     = "fi.mau.whatsapp.login.passkey"
	LoginStepIDComplete    = "fi.mau.whatsapp.login.complete"

	LoginFlowIDQR    = "qr"
	LoginFlowIDPhone = "phone"
)

var (
	ErrLoginClientOutdated = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.CLIENT_OUTDATED",
		Err:        "Got client outdated error while waiting for QRs. The bridge must be updated to continue.",
		StatusCode: http.StatusInternalServerError,
	}
	ErrLoginMultideviceNotEnabled = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.MULTIDEVICE_NOT_ENABLED",
		Err:        "Please enable WhatsApp web multidevice and scan the QR code again.",
		StatusCode: http.StatusBadRequest,
	}
	ErrLoginTimeout = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.LOGIN_TIMEOUT",
		Err:        "Entering code or scanning QR timed out. Please try again.",
		StatusCode: http.StatusBadRequest,
	}
	ErrUnexpectedDisconnect = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.LOGIN_UNEXPECTED_EVENT",
		Err:        "Unexpected event while waiting for login",
		StatusCode: http.StatusInternalServerError,
	}
	ErrPhoneNumberTooShort = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.PHONE_NUMBER_TOO_SHORT",
		Err:        "Phone number too short",
		StatusCode: http.StatusBadRequest,
	}
	ErrPhoneNumberIsNotInternational = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.PHONE_NUMBER_NOT_INTERNATIONAL",
		Err:        "Phone number must be in international format",
		StatusCode: http.StatusBadRequest,
	}
	ErrRateLimitedByWhatsApp = PairingError{
		ErrCode:    "FI.MAU.WHATSAPP.RATE_LIMITED",
		Err:        "Rate limited by WhatsApp. Try again later.",
		StatusCode: http.StatusBadRequest,
	}
)

// Pairing 独立处理 WhatsApp 的扫码、配对码和 Passkey，完成后返回配对结果。
type Pairing struct {
	PhoneCode       bool
	Log             zerolog.Logger
	newDevice       func() *store.Device
	configureClient func(context.Context, *whatsmeow.Client) error
	clientLock      sync.Mutex
	client          *whatsmeow.Client
	socketCtx       context.Context
	cancelSocket    context.CancelFunc
	stateLock       sync.Mutex
	QRs             []string
	StartTime       time.Time
	LoginError      error
	LoginSuccess    *events.PairSuccess
	WaitForQRs      *exsync.Event
	LoginComplete   *exsync.Event
	Received515     *exsync.Event
	PrevQRIndex     atomic.Int32

	PasskeyRequest          *exsync.Event
	PasskeyRequestData      *events.PairPasskeyRequest
	PasskeyConfirmation     *exsync.Event
	PasskeyConfirmationData *events.PairPasskeyConfirmation

	ADVRotate *exsync.Event

	closed         atomic.Bool
	eventHandlerID uint32
}

// NewPairing 由调用方提供设备存储和网络配置，配对过程无需桥用户对象。
func NewPairing(phoneCode bool, log zerolog.Logger, newDevice func() *store.Device, configureClient func(context.Context, *whatsmeow.Client) error) *Pairing {
	return &Pairing{
		PhoneCode: phoneCode, Log: log, newDevice: newDevice, configureClient: configureClient,
		WaitForQRs: exsync.NewEvent(), LoginComplete: exsync.NewEvent(), Received515: exsync.NewEvent(),
		PasskeyRequest: exsync.NewEvent(), PasskeyConfirmation: exsync.NewEvent(), ADVRotate: exsync.NewEvent(),
	}
}

func (wl *Pairing) startClient(ctx context.Context, transport http.RoundTripper) error {
	wl.clientLock.Lock()
	defer wl.clientLock.Unlock()
	if wl.closed.Load() {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if wl.client != nil {
		return errors.New("WhatsApp pairing has already started")
	}
	client := whatsmeow.NewClient(wl.newDevice(), waLog.Zerolog(wl.Log))
	client.EnableAutoReconnect = false
	client.DisableLoginAutoReconnect = true
	if wl.configureClient != nil {
		if err := wl.configureClient(ctx, client); err != nil {
			return err
		}
	}
	if transport != nil {
		client.SetPreLoginHTTPClient(&http.Client{Transport: transport})
	}
	// 配对连接跨多个 HTTP 步骤，关闭时由 Cancel 统一清理。
	wl.socketCtx, wl.cancelSocket = context.WithCancel(context.WithoutCancel(ctx))
	wl.client = client
	wl.eventHandlerID = client.AddEventHandler(wl.handleEvent)
	return nil
}

const LoginConnectWait = 30 * time.Second
const LoginPairPhoneWait = 30 * time.Second

// Start 初始化配对客户端，并返回第一个扫码或手机号步骤。
func (wl *Pairing) Start(ctx context.Context, overrideID string, transport http.RoundTripper) (*Step, error) {
	if err := wl.startClient(ctx, transport); err != nil {
		return nil, err
	}

	if wl.PhoneCode {
		if overrideID != "" {
			phoneNumber := fmt.Sprintf("+%s", overrideID)
			wl.Log.Debug().
				Str("phone_number", phoneNumber).
				Msg("Auto-submitting phone number for relogin")
			return wl.SubmitUserInput(ctx, map[string]string{
				"phone_number": phoneNumber,
			})
		}
		return &Step{
			Type:   StepUserInput,
			StepID: LoginStepIDPhoneNumber,
		}, nil
	}
	err := wl.client.ConnectContext(wl.socketCtx)
	if err != nil {
		wl.Log.Err(err).Msg("Failed to connect to WhatsApp for QR login")
		return nil, &RequestError{Cause: err}
	}
	ctx, cancel := context.WithTimeout(ctx, LoginConnectWait)
	defer cancel()
	err = wl.WaitForQRs.Wait(ctx)
	if err != nil {
		wl.Log.Warn().Err(err).Msg("Timed out waiting for first QR")
		wl.Cancel()
		return nil, err
	}
	return wl.qrStep(0)
}

func (wl *Pairing) SubmitUserInput(ctx context.Context, input map[string]string) (*Step, error) {
	err := wl.client.ConnectContext(wl.socketCtx)
	if err != nil {
		wl.Log.Err(err).Msg("Failed to connect to WhatsApp for phone code login")
		return nil, &RequestError{Cause: err}
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, LoginConnectWait)
	err = wl.WaitForQRs.Wait(connectCtx)
	cancelConnect()
	if err != nil {
		wl.Log.Warn().Err(err).Msg("Timed out waiting for connection")
		return nil, fmt.Errorf("failed to wait for connection: %w", err)
	}
	pairCtx, cancelPair := context.WithTimeout(ctx, LoginPairPhoneWait)
	defer cancelPair()
	pairingCode, err := wl.client.PairPhone(pairCtx, input["phone_number"], true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		wl.Log.Err(err).Msg("Failed to request phone code login")
		if errors.Is(err, whatsmeow.ErrPhoneNumberTooShort) {
			return nil, ErrPhoneNumberTooShort
		} else if errors.Is(err, whatsmeow.ErrPhoneNumberIsNotInternational) {
			return nil, ErrPhoneNumberIsNotInternational
		} else if errors.Is(err, whatsmeow.ErrIQRateOverLimit) {
			return nil, ErrRateLimitedByWhatsApp
		}
		return nil, &RequestError{Cause: err}
	}
	wl.Log.Debug().Msg("Phone code login started")
	return &Step{
		Type:         StepDisplayAndWait,
		StepID:       LoginStepIDCode,
		Instructions: "Input the pairing code in the WhatsApp mobile app to log in",
		Display: &DisplayParams{
			Type: DisplayCode,
			Data: pairingCode,
		},
	}, nil
}

func makeQRStep(qr string) *Step {
	return &Step{
		Type:         StepDisplayAndWait,
		StepID:       LoginStepIDQR,
		Instructions: "Scan the QR code with the WhatsApp mobile app to log in",
		Display: &DisplayParams{
			Type: DisplayQR,
			Data: qr,
		},
	}
}

var qrIntervals = []time.Duration{1 * time.Minute, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second}

func (wl *Pairing) getQRIndex() (currentIndex int, timeUntilNext time.Duration) {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	timeSinceStart := time.Since(wl.StartTime)
	var sum time.Duration
	for i, interval := range qrIntervals {
		if timeSinceStart > sum+interval {
			sum += interval
		} else {
			return i, interval - (timeSinceStart - sum)
		}
	}
	return -1, 0
}

// qrStep 对 QR 轮换事件和 HTTP 读取使用同一把锁，避免读取被替换的码。
func (wl *Pairing) qrStep(index int) (*Step, error) {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	if index < 0 || index >= len(wl.QRs) {
		return nil, ErrLoginTimeout
	}
	return makeQRStep(wl.QRs[index]), nil
}

func (wl *Pairing) handleEvent(rawEvt any) {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	if wl.closed.Load() {
		return
	}
	switch evt := rawEvt.(type) {
	case *events.QR:
		wl.Log.Debug().Int("code_count", len(evt.Codes)).Msg("Received QR codes")
		wl.QRs = append([]string(nil), evt.Codes...)
		wl.StartTime = time.Now()
		wl.WaitForQRs.Set()
		return
	case *events.RotateADVSecret:
		wl.Log.Debug().Msg("Rotating ADV secret in all QRs")
		for i, code := range wl.QRs {
			wl.QRs[i] = strings.Replace(code, evt.OldSecret, evt.NewSecret, 1)
		}
		wl.ADVRotate.Set()
		return
	case *events.QRScannedWithoutMultidevice:
		wl.Log.Error().Msg("QR code scanned without multidevice enabled")
		wl.LoginError = ErrLoginMultideviceNotEnabled
	case *events.ClientOutdated:
		wl.Log.Error().Msg("Got client outdated error")
		wl.LoginError = ErrLoginClientOutdated
	case *events.PairPasskeyRequest:
		wl.PasskeyRequestData = evt
		wl.PasskeyRequest.Set()
		return
	case *events.PairPasskeyConfirmation:
		wl.PasskeyConfirmationData = evt
		wl.PasskeyConfirmation.Set()
		return
	case *events.PairPasskeyError:
		wl.Log.Err(evt.Error).Msg("Got passkey error")
		wl.LoginError = evt.Error
	case *events.PairSuccess:
		wl.Log.Info().Any("event_data", evt).Msg("Got pair successful event")
		wl.LoginSuccess = evt
	case *events.PairError:
		wl.Log.Error().Any("event_data", evt).Msg("Got pair error event")
		wl.LoginError = PairingError{
			ErrCode:    "FI.MAU.WHATSAPP.PAIR_ERROR",
			Err:        evt.Error.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	case *events.Disconnected:
		wl.Log.Warn().Msg("Got disconnected event (login timed out)")
		wl.LoginError = ErrLoginTimeout
	case *events.Connected, *events.ConnectFailure, *events.LoggedOut, *events.TemporaryBan:
		wl.Log.Warn().Any("event_data", evt).Type("event_type", evt).Msg("Got unexpected disconnect event")
		wl.LoginError = ErrUnexpectedDisconnect
	case *events.ManualLoginReconnect:
		wl.Received515.Set()
		return
	default:
		wl.Log.Warn().Type("event_type", evt).Msg("Got unexpected event")
		return
	}
	wl.LoginComplete.Set()
}

func (wl *Pairing) Wait(ctx context.Context) (*Step, error) {
	if wl.PhoneCode {
		select {
		case <-ctx.Done():
			wl.Cancel()
			return nil, ctx.Err()
		case <-wl.PasskeyRequest.GetChan():
			return wl.makePasskeyStep()
		case <-wl.LoginComplete.GetChan():
			// continue
		}
	} else {
		prevIndex := int(wl.PrevQRIndex.Load())
		currentIndex, timeUntilNext := wl.getQRIndex()
		// 已超过所有二维码的有效期，结束流程，避免重新返回首个已失效二维码。
		if currentIndex < 0 {
			wl.Cancel()
			return nil, ErrLoginTimeout
		}
		nextIndex := currentIndex + 1
		logEvt := wl.Log.Debug().
			Int("prev_index", prevIndex).
			Int("current_index", currentIndex)
		if currentIndex > prevIndex {
			logEvt.Msg("Returning new QR immediately")
			wl.ADVRotate.Clear()
			wl.PrevQRIndex.Store(int32(currentIndex))
			return wl.qrStep(currentIndex)
		}
		logEvt.Int("next_index", nextIndex).Stringer("time_until_next", timeUntilNext).Msg("Waiting for next QR")
		select {
		case <-time.After(timeUntilNext):
			step, err := wl.qrStep(nextIndex)
			if err != nil {
				wl.Log.Debug().Msg("No more QRs to return")
				wl.Cancel()
				return nil, ErrLoginTimeout
			}
			wl.ADVRotate.Clear()
			wl.PrevQRIndex.Store(int32(nextIndex))
			return step, nil
		case <-ctx.Done():
			wl.Cancel()
			return nil, ctx.Err()
		case <-wl.PasskeyRequest.GetChan():
			return wl.makePasskeyStep()
		case <-wl.ADVRotate.GetChan():
			wl.ADVRotate.Clear()
			step, err := wl.qrStep(currentIndex)
			if err != nil {
				wl.Log.Debug().Msg("ADV secret was rotated, but no QRs are left to return")
				wl.Cancel()
				return nil, ErrLoginTimeout
			}
			wl.Log.Debug().Msg("ADV secret was rotated, returning current QR immediately")
			return step, nil
		case <-wl.LoginComplete.GetChan():
			// continue
		}
	}
	return wl.onLoginComplete(ctx)
}

func (wl *Pairing) onLoginComplete(ctx context.Context) (*Step, error) {
	wl.stateLock.Lock()
	loginErr := wl.LoginError
	wl.stateLock.Unlock()
	if loginErr != nil {
		wl.Log.Debug().Err(loginErr).Msg("Login completed with error")
		wl.Cancel()
		return nil, loginErr
	}
	wl.Log.Debug().Msg("Login completed without error, waiting for 515 event")
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := wl.Received515.Wait(ctxTimeout)
	cancel()
	wl.Cancel()
	if err != nil {
		return nil, fmt.Errorf("timed out waiting for 515: %w", err)
	}

	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	if wl.LoginSuccess == nil {
		return nil, ErrUnexpectedDisconnect
	}
	return &Step{Type: StepComplete, StepID: LoginStepIDComplete, Success: wl.LoginSuccess}, nil
}

func (wl *Pairing) makePasskeyStep() (*Step, error) {
	wl.stateLock.Lock()
	request := wl.PasskeyRequestData
	wl.stateLock.Unlock()
	pubkeyData, err := json.Marshal(request.PublicKey)
	if err != nil {
		wl.Log.Err(err).Msg("Failed to marshal public key data")
		return nil, err
	}
	return &Step{
		Type:   StepWebAuthn,
		StepID: LoginStepIDPasskey,
		WebAuthn: &WebAuthnParams{
			URL:       "https://web.whatsapp.com",
			PublicKey: pubkeyData,
		},
	}, nil
}

func (wl *Pairing) SubmitWebAuthnResponse(ctx context.Context, rawResp json.RawMessage) (*Step, error) {
	var resp types.WebAuthnResponse
	err := json.Unmarshal(rawResp, &resp)
	if err != nil {
		wl.Log.Err(err).Msg("Failed to unmarshal WebAuthn response")
		wl.Cancel()
		return nil, err
	}
	err = wl.client.SendPasskeyResponse(ctx, &resp)
	if err != nil {
		wl.Log.Err(err).Msg("Failed to send WebAuthn response")
		wl.Cancel()
		return nil, &RequestError{Cause: err}
	}
	select {
	case <-ctx.Done():
		wl.Cancel()
		return nil, ctx.Err()
	case <-wl.PasskeyConfirmation.GetChan():
		wl.stateLock.Lock()
		skipHandoff := wl.PasskeyConfirmationData.SkipHandoffUX
		wl.stateLock.Unlock()
		if !skipHandoff {
			wl.Cancel()
			return nil, PairingError{
				ErrCode:    "FI.MAU.WHATSAPP.NOT_IMPLEMENTED",
				Err:        "Displaying WebAuthn pairing confirmation codes is not yet implemented",
				StatusCode: http.StatusBadRequest,
			}
		}
		err = wl.client.SendPasskeyConfirmation(ctx)
		if err != nil {
			wl.Log.Err(err).Msg("Failed to send WebAuthn confirmation")
			wl.Cancel()
			return nil, &RequestError{Cause: err}
		}
	case <-wl.LoginComplete.GetChan():
	}
	select {
	case <-ctx.Done():
		wl.Cancel()
		return nil, ctx.Err()
	case <-wl.LoginComplete.GetChan():
	}
	return wl.onLoginComplete(ctx)
}

// Client 返回已创建的 WhatsMeow 配对客户端，登录完成后由独立 Inbox 运行时接管。
func (wl *Pairing) Client() *whatsmeow.Client {
	wl.clientLock.Lock()
	defer wl.clientLock.Unlock()
	return wl.client
}

// Cancel 结束配对连接，可在初始化失败时或 HTTP 重复取消时调用。
func (wl *Pairing) Cancel() {
	wl.clientLock.Lock()
	if wl.closed.Swap(true) {
		wl.clientLock.Unlock()
		return
	}
	client, handlerID, cancel := wl.client, wl.eventHandlerID, wl.cancelSocket
	wl.clientLock.Unlock()
	if client != nil {
		cancel()
		client.RemoveEventHandler(handlerID)
		client.Disconnect()
	}
}
