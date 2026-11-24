package mxmain

import (
	"context"
	"testing"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
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

func TestPostMigrateSkipsHomeserverDoesNotTouchDB(t *testing.T) {
	br := &BridgeMain{Bridge: &bridgev2.Bridge{Network: homeserverSkipStub{skip: true}}}
	if err := br.PostMigrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if br.DB != nil {
		t.Fatal("skipped PostMigrate must not use a database")
	}
}
