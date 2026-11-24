package matrix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"
)

type homeserverSkipStub struct {
	skip bool
}

func (s homeserverSkipStub) SkipHomeserver() bool               { return s.skip }
func (s homeserverSkipStub) Init(*bridgev2.Bridge)              {}
func (s homeserverSkipStub) Start(context.Context) error        { return nil }
func (s homeserverSkipStub) GetName() bridgev2.BridgeName       { return bridgev2.BridgeName{} }
func (s homeserverSkipStub) GetDBMetaTypes() database.MetaTypes { return database.MetaTypes{} }
func (s homeserverSkipStub) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func (s homeserverSkipStub) GetConfig() (string, any, configupgrade.Upgrader)         { return "", nil, nil }
func (s homeserverSkipStub) LoadUserLogin(context.Context, *bridgev2.UserLogin) error { return nil }
func (s homeserverSkipStub) GetLoginFlows() []bridgev2.LoginFlow                      { return nil }
func (s homeserverSkipStub) CreateLogin(context.Context, *bridgev2.User, string) (bridgev2.LoginProcess, error) {
	return nil, nil
}
func (s homeserverSkipStub) GetBridgeInfoVersion() (int, int) { return 0, 0 }

func TestNewUserIntentSkipsSetupWhenHomeserverSkipped(t *testing.T) {
	br := &Connector{Bridge: &bridgev2.Bridge{Network: homeserverSkipStub{skip: true}}}
	api, token, err := br.NewUserIntent(context.Background(), id.UserID("@user:example.com"), "token")
	if err != nil {
		t.Fatal(err)
	}
	if api != nil {
		t.Fatal("skipped homeserver must not create a double puppet intent")
	}
	if token != "token" {
		t.Fatalf("token = %q", token)
	}
}

func TestAuthMiddlewareDoesNotCheckMatrixAuthWhenHomeserverSkipped(t *testing.T) {
	br := &Connector{
		Config: &bridgeconfig.Config{
			Provisioning: bridgeconfig.ProvisioningConfig{
				SharedSecret:    "0123456789abcdef",
				AllowMatrixAuth: true,
			},
		},
		Bridge: &bridgev2.Bridge{Network: homeserverSkipStub{skip: true}},
	}
	prov := &ProvisioningAPI{br: br}
	called := false
	handler := prov.AuthMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/?user_id=@user:example.com", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if called {
		t.Fatal("invalid matrix token was accepted")
	}
	if rec.Code == http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}
