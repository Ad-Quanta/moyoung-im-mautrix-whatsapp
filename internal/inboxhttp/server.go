// Package inboxhttp 提供独立 WhatsApp Inbox 的 HTTP 协议适配层。
package inboxhttp

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inbox"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxservice"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
)

// Config 保存 Router 密钥和既有登录权限策略。
type Config struct {
	SharedSecret    string
	FailOnWebAuthn  bool
	LoginPermission func(owner string) (allowed bool, maxLogins int)
	ContactName     func(inboxservice.Contact) (string, error)
}
type Server struct {
	manager  *inboxservice.Manager
	config   Config
	mux      *http.ServeMux
	mu       sync.Mutex
	closed   bool
	pairings map[string]*attempt
}

func New(manager *inboxservice.Manager, cfg Config) *Server {
	s := &Server{manager: manager, config: cfg, mux: http.NewServeMux(), pairings: map[string]*attempt{}}
	s.routes()
	return s
}
func (s *Server) Handler() http.Handler { return s.auth(s.mux) }
func (s *Server) routes() {
	s.mux.HandleFunc("GET /_matrix/provision/v3/whoami", s.whoami)
	s.mux.HandleFunc("GET /_matrix/provision/v3/logins", s.logins)
	s.mux.HandleFunc("GET /_matrix/provision/v3/contacts", s.contacts)
	s.mux.HandleFunc("GET /_matrix/provision/v3/login/flows", s.flows)
	s.mux.HandleFunc("POST /_matrix/provision/v3/login/start/{flowID}", s.start)
	s.mux.HandleFunc("POST /_matrix/provision/v3/login/step/{processID}/{stepID}/{stepType}", s.step)
	s.mux.HandleFunc("POST /_matrix/provision/v3/login/cancel/{processID}", s.cancel)
	s.mux.HandleFunc("POST /_matrix/provision/v3/login/step/{processID}/{stepID}/cancel", s.cancel)
	s.mux.HandleFunc("POST /_matrix/provision/v3/logout/{loginID}", s.logout)
	s.mux.HandleFunc("POST /_matrix/provision/v3/account-cleanup/{loginID}", s.cleanup)
	s.mux.HandleFunc("POST /_matrix/provision/v3/inbox/send/{loginID}", s.send)
	s.mux.HandleFunc("POST /_matrix/provision/v3/inbox/download/{loginID}", s.download)
}
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.config.SharedSecret == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.config.SharedSecret)) != 1 {
			jsonError(w, http.StatusUnauthorized, errors.New("invalid provisioning secret"))
			return
		}
		key, err := requestBinding(r)
		if err != nil {
			jsonError(w, 400, err)
			return
		}
		if s.config.LoginPermission != nil {
			if allowed, _ := s.config.LoginPermission(key); !allowed {
				jsonError(w, 403, errors.New("login is forbidden for this binding"))
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), bindingKey{}, key)))
	})
}

type bindingKey struct{}

// owner 读取中间件已经校验并规范化的绑定键。
func owner(r *http.Request) string {
	key, _ := r.Context().Value(bindingKey{}).(string)
	return key
}

func requestBinding(r *http.Request) (string, error) {
	return inboxsession.NormalizeBindingKey(r.URL.Query().Get("user_id"))
}
func jsonWrite(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func jsonError(w http.ResponseWriter, code int, err error) {
	jsonWrite(w, code, map[string]string{"error": err.Error()})
}
func (s *Server) whoami(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	rows, err := s.manager.List(o)
	if err != nil {
		jsonError(w, 500, err)
		return
	}
	logins := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		profile := row.RemoteProfile
		if len(profile) == 0 {
			profile = json.RawMessage(`{}`)
		}
		logins = append(logins, map[string]any{"id": row.LoginID, "name": row.RemoteName, "profile": profile,
			"state_event": row.Status.StateEvent, "state_ts": row.Status.Timestamp, "state_reason": row.Status.Error, "state": row.Status})
	}
	jsonWrite(w, 200, map[string]any{"network": map[string]any{"displayname": "WhatsApp", "network_id": "whatsapp", "network_url": "https://whatsapp.com", "beeper_bridge_type": "whatsapp", "default_port": 29318},
		"login_flows": loginFlows, "homeserver": "", "bridge_bot": "", "command_prefix": "", "logins": logins})
}
func (s *Server) logins(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	rows, err := s.manager.List(o)
	if err != nil {
		jsonError(w, 500, err)
		return
	}
	ids := make([]string, 0, len(rows))
	for _, v := range rows {
		ids = append(ids, v.LoginID)
	}
	jsonWrite(w, 200, map[string]any{"login_ids": ids})
}
func (s *Server) contacts(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	id := strings.TrimSpace(r.URL.Query().Get("login_id"))
	if id == "" {
		rows, err := s.manager.List(o)
		if err != nil {
			jsonError(w, statusFor(err), err)
			return
		}
		if len(rows) == 0 {
			jsonError(w, 400, errors.New("not logged in"))
			return
		}
		id = rows[0].LoginID
	}
	contacts, err := s.manager.Contacts(r.Context(), o, id)
	if err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	type contact struct {
		ID          string   `json:"id"`
		Name        string   `json:"name,omitempty"`
		Identifiers []string `json:"identifiers,omitempty"`
	}
	out := make([]contact, 0, len(contacts))
	for _, entry := range contacts {
		name := entry.Info.FullName
		if name == "" {
			name = entry.Info.FirstName
		}
		if s.config.ContactName != nil {
			name, err = s.config.ContactName(entry)
			if err != nil {
				jsonError(w, 500, err)
				return
			}
		}
		row := contact{ID: entry.ID, Name: name}
		if entry.Phone != "" {
			row.Identifiers = []string{"tel:" + entry.Phone}
		}
		out = append(out, row)
	}
	slices.SortFunc(out, func(a, b contact) int { return strings.Compare(a.ID, b.ID) })
	jsonWrite(w, 200, map[string]any{"contacts": out})
}
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	id := r.PathValue("loginID")
	if id == "all" {
		rows, e := s.manager.List(o)
		if e != nil {
			jsonError(w, 500, e)
			return
		}
		for _, row := range rows {
			if e = s.manager.Logout(r.Context(), o, row.LoginID); e != nil {
				jsonError(w, 502, e)
				return
			}
		}
		jsonWrite(w, 200, map[string]any{})
		return
	}
	if err := s.manager.Logout(r.Context(), o, id); err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	jsonWrite(w, 200, map[string]any{})
}

// cleanup 只有实际完成远端解绑并验证本地数据后才返回已验证回执。
func (s *Server) cleanup(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	id := strings.TrimSpace(r.PathValue("loginID"))
	if id == "" || id == "all" {
		jsonError(w, 400, errors.New("invalid login ID"))
		return
	}
	result, err := s.manager.CleanupAccount(r.Context(), o, id)
	if err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	jsonWrite(w, 200, result)
}

// readJSON 拒绝超限和包含多段 JSON 的请求，避免截断后仍处理消息。
func readJSON(w http.ResponseWriter, r *http.Request, limit int64, target any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		jsonError(w, 413, err)
		return false
	}
	if err == nil {
		err = json.Unmarshal(raw, target)
	}
	if err != nil {
		jsonError(w, 400, err)
		return false
	}
	return true
}
func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	var req inbox.SendRequest
	if !readJSON(w, r, 10<<20, &req) {
		return
	}
	out, err := s.manager.Send(r.Context(), o, r.PathValue("loginID"), req)
	if err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	jsonWrite(w, 200, out)
}
func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	o := owner(r)
	var req inbox.DownloadRequest
	if !readJSON(w, r, 1<<20, &req) {
		return
	}
	data, mime, err := s.manager.Download(r.Context(), o, r.PathValue("loginID"), req)
	if err != nil {
		jsonError(w, statusFor(err), err)
		return
	}
	if mime == "" {
		mime = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	_, _ = w.Write(data)
}
func statusFor(err error) int {
	switch {
	case errors.Is(err, inboxservice.ErrNotFound):
		return 404
	case errors.Is(err, inboxservice.ErrCleanupPending):
		return 409
	case errors.Is(err, inboxservice.ErrNotConnected):
		return 502
	case errors.Is(err, context.Canceled):
		return 499
	default:
		return 502
	}
}
