package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/database"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxhttp"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxservice"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxsession"
	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/proto/waAdv"
)

// 验证独立入口读取已有配置，并强制保持 Inbox 的实时模式。
func TestInboxConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`appservice:
  hostname: 127.0.0.1
  port: 29318
provisioning:
  shared_secret: provision-secret-for-test
bridge:
  permissions:
    "*": user
network:
  os_name: Chrome
  initial_auto_reconnect: true
  inbox:
    push_url: http://im-router:8080/internal/whatsapp/events
    push_secret: push-secret-for-test
`), 0600))
	cfg, err := loadInboxConfig(path)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:29318", cfg.Listen)
	require.True(t, cfg.Network.InitialAutoReconnect)
	allowed, max := cfg.loginPermission("@binding_test:localhost")
	require.True(t, allowed)
	require.Zero(t, max)
}

// 验证协议路由、Bearer 鉴权、绑定键校验和流程名称不依赖 Matrix 对象。
func TestInboxHTTPProtocol(t *testing.T) {
	server := inboxhttp.New(nil, inboxhttp.Config{SharedSecret: "provision-secret-for-test"})
	handler := server.Handler()
	request := func(method, path, owner, secret string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path+"?user_id="+owner, nil)
		r.Header.Set("Authorization", "Bearer "+secret)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	require.Equal(t, http.StatusUnauthorized, request(http.MethodGet, "/_matrix/provision/v3/login/flows", "@binding_test:localhost", "bad").Code)
	require.Equal(t, http.StatusBadRequest, request(http.MethodGet, "/_matrix/provision/v3/login/flows", "invalid", "provision-secret-for-test").Code)
	require.Equal(t, http.StatusOK, request(http.MethodGet, "/_matrix/provision/v3/login/flows", "@binding_test:localhost", "provision-secret-for-test").Code)
	require.Contains(t, request(http.MethodGet, "/_matrix/provision/v3/login/flows", "@binding_test:localhost", "provision-secret-for-test").Body.String(), inboxsession.LoginFlowIDQR)
	server.Close()
}

// 模拟旧进程已远端解绑后中断。另一个绑定不能读取回执，清理失败可以回滚重试。
func TestStrictCleanupResumesFromStoredStage(t *testing.T) {
	db := openInboxTestDatabase(t)
	ctx := t.Context()
	log := zerolog.Nop()
	require.NoError(t, database.MigrateInbox(ctx, db))
	const key = "@binding_test:localhost"
	const other = "wa-bind:00000000-0000-0000-0000-000000000000"
	const id = "12345"
	logins := inboxsession.NewStore(db, "")
	require.NoError(t, logins.SavePaired(ctx, &inboxsession.LoginRecord{Owner: key, ID: id, RemoteName: "+12345", RemoteProfile: json.RawMessage(`{"phone":"+12345"}`), Metadata: json.RawMessage(`{"wa_device_id":42}`)}))
	jid := inboxsession.DeviceJID(id, 42)
	devices := database.NewDeviceStore(db, log)
	device := devices.NewDevice()
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{1}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	require.NoError(t, device.Save(ctx))
	_, err := db.Exec(ctx, `INSERT INTO whatsapp_account_cleanup (bridge_id,user_login_id,user_mxid,device_jid,remote_logout_completed,updated_at) VALUES ('',$1,$2,$3,true,1)`, id, key, jid.String())
	require.NoError(t, err)
	manager := inboxservice.NewManager(ctx, db, "", log, inboxservice.Config{}, nil)
	defer manager.Stop()
	require.NoError(t, manager.Start(ctx))
	rows, err := manager.List(key)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, inboxservice.StateLoggedOut, rows[0].Status.StateEvent)
	require.ErrorIs(t, manager.Connect(key, id), inboxservice.ErrCleanupPending)
	provision := inboxhttp.New(manager, inboxhttp.Config{SharedSecret: "provision-secret-for-test"})
	defer provision.Close()
	handler := inboxHandler(provision.Handler())
	request := func(method, path, owner string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/_matrix/provision/v3"+path+"?user_id="+url.QueryEscape(owner), strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer provision-secret-for-test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	route := "/account-cleanup/" + id
	require.Equal(t, 404, request("POST", route, other).Code)
	require.Contains(t, request("GET", "/whoami", key).Body.String(), `"phone":"+12345"`)
	require.JSONEq(t, `{"login_ids":[]}`, request("GET", "/logins", other).Body.String())
	require.Equal(t, 404, request("POST", "/inbox/send/"+id, other).Code)

	// 手机号流程的第一步可离线获取，验证旧 Router 不带 txn_id 的请求及绑定隔离。
	started := request("POST", "/login/start/"+inboxsession.LoginFlowIDPhone, key)
	require.Equal(t, 200, started.Code, started.Body.String())
	var step struct {
		ID     string `json:"login_id"`
		StepID string `json:"step_id"`
		TxnID  string `json:"txn_id"`
	}
	require.NoError(t, json.Unmarshal(started.Body.Bytes(), &step))
	require.Equal(t, inboxsession.LoginStepIDPhoneNumber, step.StepID)
	require.NotEmpty(t, step.TxnID)
	stepPath := "/login/step/" + step.ID + "/" + step.StepID + "/user_input"
	cancelPath := "/login/cancel/" + step.ID
	require.Equal(t, 410, request("POST", stepPath, other).Code)
	require.Equal(t, 200, request("POST", cancelPath, other).Code)
	require.Equal(t, 400, request("POST", "/login/step/"+step.ID+"/old/user_input", key).Code)
	require.Equal(t, 200, request("POST", cancelPath, key).Code)
	require.Equal(t, 410, request("POST", stepPath, key).Code)

	_, err = db.Exec(ctx, `
		CREATE FUNCTION fail_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'simulated failure'; END;
		$$;
		CREATE TRIGGER fail_cleanup BEFORE DELETE ON user_login
		FOR EACH ROW EXECUTE FUNCTION fail_cleanup()`)
	require.NoError(t, err)
	require.Equal(t, 502, request("POST", route, key).Code)
	kept, err := devices.GetDevice(ctx, jid)
	require.NoError(t, err)
	require.NotNil(t, kept)
	_, err = db.Exec(ctx, `DROP TRIGGER fail_cleanup ON user_login`)
	require.NoError(t, err)
	reply := request("POST", route, key)
	require.Equal(t, 200, reply.Code, reply.Body.String())
	var receipt inboxservice.CleanupReceipt
	require.NoError(t, json.Unmarshal(reply.Body.Bytes(), &receipt))
	require.True(t, receipt.Verified)
	require.EqualValues(t, 1, receipt.Removed["whatsmeow_device"])
	require.EqualValues(t, 1, receipt.Removed["user_login"])
	require.JSONEq(t, reply.Body.String(), request("POST", route, key).Body.String())
	require.Equal(t, 404, request("POST", route, other).Code)
	require.JSONEq(t, `{"login_ids":[]}`, request("GET", "/logins", key).Body.String())
	for _, table := range []string{"mx_version", "crypto_version"} {
		exists, err := db.TableExists(ctx, table)
		require.NoError(t, err)
		require.False(t, exists, table)
	}
}

// openInboxTestDatabase 为每个集成测试创建独立 schema，只在显式指定测试库时连接。
func openInboxTestDatabase(t *testing.T) *dbutil.Database {
	t.Helper()
	dsn := os.Getenv("WA_TEST_DSN")
	if dsn == "" {
		t.Skip("未配置 WA_TEST_DSN，跳过 PostgreSQL 集成测试")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "wa_test_" + strings.ToLower(rand.Text())
	_, err = admin.ExecContext(t.Context(), "CREATE SCHEMA "+pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE")
		require.NoError(t, err)
	})
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	raw, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	db, err := dbutil.NewWithDB(raw, "postgres")
	require.NoError(t, err)
	return db
}

// 验证并发首次启动、旧版接管和版本不符时的事务回滚。
func TestWhatsAppUnifiedMigration(t *testing.T) {
	db := openInboxTestDatabase(t)
	ctx := t.Context()
	concurrent := make(chan error, 2)
	for range 2 {
		go func() { concurrent <- database.MigrateInbox(ctx, db) }()
	}
	for range 2 {
		require.NoError(t, <-concurrent)
	}
	var version int
	require.NoError(t, db.QueryRow(ctx, `SELECT version FROM whatsapp_schema_migrations`).Scan(&version))
	require.Equal(t, 1, version)

	// 用真实业务表构造旧版本标记，确保接管过程中已有登录记录原样保留。
	logins := inboxsession.NewStore(db, "")
	require.NoError(t, logins.SavePaired(ctx, &inboxsession.LoginRecord{
		Owner: "@binding_test:localhost", ID: "12345", RemoteName: "+12345",
		RemoteProfile: json.RawMessage(`{"phone":"+12345"}`), Metadata: json.RawMessage(`{"wa_device_id":42}`),
	}))
	_, err := db.Exec(ctx, `
		DROP TABLE whatsapp_schema_migrations;
		CREATE TABLE database_owner (key INTEGER PRIMARY KEY, owner TEXT NOT NULL);
		INSERT INTO database_owner VALUES (0, 'megabridge/mautrix-whatsapp');
		CREATE TABLE version (version INTEGER NOT NULL);
		INSERT INTO version VALUES (33);
		CREATE TABLE whatsapp_version (version INTEGER NOT NULL);
		INSERT INTO whatsapp_version VALUES (11);
		CREATE TABLE whatsmeow_version (version INTEGER NOT NULL);
		INSERT INTO whatsmeow_version VALUES (16)`)
	require.NoError(t, err)
	require.ErrorContains(t, database.MigrateInbox(ctx, db), "33/11/16")
	var hasUnified, hasLegacy bool
	require.NoError(t, db.QueryRow(ctx, `SELECT to_regclass('whatsapp_schema_migrations') IS NOT NULL,
		to_regclass('version') IS NOT NULL`).Scan(&hasUnified, &hasLegacy))
	require.False(t, hasUnified)
	require.True(t, hasLegacy)
	_, err = db.Exec(ctx, `UPDATE whatsapp_version SET version=12`)
	require.NoError(t, err)
	require.NoError(t, database.MigrateInbox(ctx, db))
	require.NoError(t, database.MigrateInbox(ctx, db))
	require.NoError(t, db.QueryRow(ctx, `SELECT version FROM whatsapp_schema_migrations`).Scan(&version))
	require.Equal(t, 1, version)
	for _, table := range []string{"version", "whatsapp_version", "whatsmeow_version", "database_owner"} {
		var exists bool
		require.NoError(t, db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		require.False(t, exists, table)
	}
	var profile string
	require.NoError(t, db.QueryRow(ctx, `SELECT remote_profile FROM user_login WHERE id='12345'`).Scan(&profile))
	require.JSONEq(t, `{"phone":"+12345"}`, profile)
}
