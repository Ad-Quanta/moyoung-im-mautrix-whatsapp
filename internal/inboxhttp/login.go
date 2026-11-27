package inboxhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxservice"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"github.com/google/uuid"
)

type flow struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

var loginFlows = []flow{
	{inboxsession.LoginFlowIDQR, "QR", "Scan a QR code to pair WhatsApp"},
	{inboxsession.LoginFlowIDPhone, "Pairing code", "Pair WhatsApp with a phone code"},
}

func (s *Server) flows(w http.ResponseWriter, _ *http.Request) {
	jsonWrite(w, 200, map[string]any{"flows": loginFlows})
}

// attempt 将配对流程绑定到发起的账号，并串行推进同一流程的步骤。
type attempt struct {
	owner          string
	override       string
	pairing        *inboxsession.Pairing
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	step           *inboxsession.Step
	txn            string
	previousTxn    string
	previousStepID string
	running        *stepRun
}

type stepRun struct {
	done     chan struct{}
	response map[string]any
	err      error
}

func (p *attempt) response(id string) map[string]any {
	step := p.step
	out := map[string]any{"login_id": id, "txn_id": p.txn, "type": step.Type, "step_id": step.StepID, "instructions": step.Instructions}
	switch step.Type {
	case inboxsession.StepUserInput:
		out["user_input"] = map[string]any{"fields": []map[string]any{{"id": "phone_number", "name": "Phone number", "type": "phone_number", "required": true, "description": "Your WhatsApp phone number in international format"}}}
	case inboxsession.StepDisplayAndWait:
		out["display_and_wait"] = map[string]string{"type": step.Display.Type, "data": step.Display.Data}
	case inboxsession.StepWebAuthn:
		out["webauthn"] = map[string]any{"url": step.WebAuthn.URL, "publicKey": step.WebAuthn.PublicKey}
	case inboxsession.StepComplete:
		out["complete"] = map[string]string{"user_login_id": inboxsession.LoginID(step.Success.ID)}
	}
	return out
}

func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	key := owner(r)
	flowID := r.PathValue("flowID")
	if !slices.ContainsFunc(loginFlows, func(f flow) bool { return f.ID == flowID }) {
		jsonError(w, 404, errors.New("login flow not found"))
		return
	}
	rows, err := s.manager.List(key)
	if err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	override := r.URL.Query().Get("login_id")
	if override != "" && !slices.ContainsFunc(rows, func(row inboxservice.Snapshot) bool { return row.LoginID == override }) {
		jsonError(w, 404, inboxservice.ErrNotFound)
		return
	}
	if s.config.LoginPermission != nil && override == "" {
		_, max := s.config.LoginPermission(key)
		if max > 0 && len(rows) >= max {
			loginError(w, inboxsession.PairingError{StatusCode: 403, ErrCode: "FI.MAU.BRIDGE.TOO_MANY_LOGINS", Err: "Maximum number of logins exceeded"})
			return
		}
	}
	pairing, err := s.manager.NewPairing(flowID == inboxsession.LoginFlowIDPhone)
	if err != nil {
		loginError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Minute)
	p := &attempt{owner: key, override: override, pairing: pairing, ctx: ctx, cancel: cancel}
	id := uuid.NewString()
	stop := context.AfterFunc(r.Context(), cancel)
	step, err := pairing.Start(ctx, override, nil)
	stop()
	if err != nil || ctx.Err() != nil {
		cancel()
		pairing.Cancel()
		if err == nil {
			err = ctx.Err()
		}
		loginError(w, err)
		return
	}
	p.step, p.txn = step, newTxn()
	response := p.response(id)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		pairing.Cancel()
		jsonError(w, 503, errors.New("service is stopping"))
		return
	}
	s.pairings[id] = p
	s.mu.Unlock()
	context.AfterFunc(ctx, func() { s.drop(id, p) })
	jsonWrite(w, 200, response)
}

func newTxn() string { return "bls_" + uuid.NewString() }

func (s *Server) lookup(id, key string) *attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pairings[id]
	if p == nil || p.owner != key || p.ctx.Err() != nil {
		return nil
	}
	return p
}

func (s *Server) drop(id string, p *attempt) {
	s.mu.Lock()
	delete(s.pairings, id)
	s.mu.Unlock()
	p.cancel()
	p.pairing.Cancel()
}

// Close 中止仍在等待配对的连接，HTTP 服务关闭时调用。
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	pending := s.pairings
	s.pairings = nil
	s.mu.Unlock()
	for _, p := range pending {
		p.cancel()
		p.pairing.Cancel()
	}
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	key := owner(r)
	id := r.PathValue("processID")
	if p := s.lookup(id, key); p != nil {
		if stepID := r.PathValue("stepID"); stepID != "" {
			p.mu.Lock()
			match := stepID == p.step.StepID && (r.URL.Query().Get("txn_id") == "" || r.URL.Query().Get("txn_id") == p.txn)
			p.mu.Unlock()
			if !match {
				jsonError(w, 400, errors.New("login step has changed"))
				return
			}
		}
		s.drop(id, p)
	}
	jsonWrite(w, 200, map[string]any{})
}

func (s *Server) step(w http.ResponseWriter, r *http.Request) {
	key := owner(r)
	id, stepID, kind := r.PathValue("processID"), r.PathValue("stepID"), r.PathValue("stepType")
	p := s.lookup(id, key)
	if p == nil {
		loginError(w, inboxsession.PairingError{StatusCode: 410, ErrCode: "FI.MAU.BRIDGE.LOGIN_TIMED_OUT", Err: "Login process is unavailable"})
		return
	}
	var fields map[string]string
	var webauthn json.RawMessage
	switch kind {
	case inboxsession.StepUserInput:
		if !readJSON(w, r, 1<<20, &fields) {
			return
		}
	case inboxsession.StepWebAuthn:
		if !readJSON(w, r, 1<<20, &webauthn) {
			return
		}
	case inboxsession.StepDisplayAndWait:
	default:
		jsonError(w, 400, errors.New("unsupported login step"))
		return
	}
	txn := r.URL.Query().Get("txn_id")
	p.mu.Lock()
	if (stepID == p.previousStepID && (txn == p.previousTxn || (txn == "" && stepID != p.step.StepID))) || p.step.Type == inboxsession.StepComplete {
		response := p.response(id)
		p.mu.Unlock()
		jsonWrite(w, 200, response)
		return
	}
	if stepID != p.step.StepID || kind != p.step.Type || (txn != "" && txn != p.txn) {
		p.mu.Unlock()
		jsonError(w, 400, errors.New("login step has changed"))
		return
	}
	run := p.running
	if run == nil {
		run = &stepRun{done: make(chan struct{})}
		p.running = run
		// HTTP 长轮询断开时保留运行中的步骤，下一次请求可以继续等待。
		go s.runStep(id, p, run, kind, fields, webauthn)
	}
	p.mu.Unlock()
	select {
	case <-r.Context().Done():
	case <-run.done:
		if run.err != nil {
			loginError(w, run.err)
			return
		}
		jsonWrite(w, 200, run.response)
	}
}

func (s *Server) runStep(id string, p *attempt, run *stepRun, kind string, fields map[string]string, webauthn json.RawMessage) {
	var next *inboxsession.Step
	var err error
	switch kind {
	case inboxsession.StepUserInput:
		next, err = p.pairing.SubmitUserInput(p.ctx, fields)
	case inboxsession.StepDisplayAndWait:
		next, err = p.pairing.Wait(p.ctx)
	case inboxsession.StepWebAuthn:
		next, err = p.pairing.SubmitWebAuthnResponse(p.ctx, webauthn)
	}
	if err == nil && next.Type == inboxsession.StepWebAuthn && s.config.FailOnWebAuthn {
		err = inboxsession.PairingError{StatusCode: 400, ErrCode: "COM.BEEPER.WEBAUTHN_UNSUPPORTED", Err: "Logging in with a passkey is not yet supported"}
	}
	if err == nil && next.Type == inboxsession.StepComplete {
		if p.override != "" && p.override != inboxsession.LoginID(next.Success.ID) {
			err = inboxsession.PairingError{StatusCode: 409, ErrCode: "FI.MAU.BRIDGE.LOGIN_MISMATCH", Err: "Paired account differs from requested login"}
		} else {
			_, err = s.manager.CompletePairing(p.ctx, p.owner, p.pairing, next.Success)
		}
	}
	p.mu.Lock()
	if err == nil {
		p.previousTxn, p.previousStepID = p.txn, p.step.StepID
		p.step, p.txn = next, newTxn()
		run.response = p.response(id)
	}
	run.err = err
	p.running = nil
	close(run.done)
	p.mu.Unlock()
	if err != nil {
		var pairingErr inboxsession.PairingError
		if kind != inboxsession.StepUserInput || !errors.As(err, &pairingErr) || pairingErr.StatusCode != 400 {
			s.drop(id, p)
		}
	}
}

func loginError(w http.ResponseWriter, err error) {
	var failure inboxsession.PairingError
	if errors.As(err, &failure) {
		jsonWrite(w, failure.StatusCode, map[string]string{"error": failure.Err, "errcode": failure.ErrCode})
		return
	}
	if errors.Is(err, inboxsession.ErrLoginOwned) {
		jsonError(w, 409, err)
		return
	}
	jsonError(w, statusFor(err), err)
}
