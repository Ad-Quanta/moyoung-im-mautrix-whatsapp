package appservice

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type readFlag struct {
	read bool
}

func (r *readFlag) Read([]byte) (int, error) {
	r.read = true
	return 0, io.EOF
}

func (r *readFlag) Close() error { return nil }

func TestIgnoreHomeserverEventsDoesNotReadOrQuery(t *testing.T) {
	as := &AppService{IgnoreHomeserverEvents: true}
	body := &readFlag{}
	req := httptest.NewRequest(http.MethodPut, "/transactions/txn", body)
	rec := httptest.NewRecorder()
	as.PutTransaction(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("transaction status = %d", rec.Code)
	}
	if body.read {
		t.Fatal("transaction handler read the body")
	}

	body = &readFlag{}
	req = httptest.NewRequest(http.MethodPost, "/ping", body)
	rec = httptest.NewRecorder()
	as.PostPing(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ping status = %d", rec.Code)
	}
	if body.read {
		t.Fatal("ping handler read the body")
	}

	req = httptest.NewRequest(http.MethodGet, "/rooms/alias", nil)
	rec = httptest.NewRecorder()
	as.GetRoom(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("room status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/users/user", nil)
	rec = httptest.NewRecorder()
	as.GetUser(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("user status = %d", rec.Code)
	}
}
