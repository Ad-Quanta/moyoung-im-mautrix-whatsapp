package bridgev2

import (
	"context"
	"testing"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2/database"
)

type homeserverSkipStub struct {
	skip bool
}

func (s homeserverSkipStub) SkipHomeserver() bool               { return s.skip }
func (s homeserverSkipStub) Init(*Bridge)                       {}
func (s homeserverSkipStub) Start(context.Context) error        { return nil }
func (s homeserverSkipStub) GetName() BridgeName                { return BridgeName{} }
func (s homeserverSkipStub) GetDBMetaTypes() database.MetaTypes { return database.MetaTypes{} }
func (s homeserverSkipStub) GetCapabilities() *NetworkGeneralCapabilities {
	return &NetworkGeneralCapabilities{}
}
func (s homeserverSkipStub) GetConfig() (string, any, configupgrade.Upgrader) { return "", nil, nil }
func (s homeserverSkipStub) LoadUserLogin(context.Context, *UserLogin) error  { return nil }
func (s homeserverSkipStub) GetLoginFlows() []LoginFlow                       { return nil }
func (s homeserverSkipStub) CreateLogin(context.Context, *User, string) (LoginProcess, error) {
	return nil, nil
}
func (s homeserverSkipStub) GetBridgeInfoVersion() (int, int) { return 1, 2 }

func TestSkipsHomeserver(t *testing.T) {
	if (&Bridge{}).SkipsHomeserver() {
		t.Fatal("nil network must not skip homeserver")
	}
	plain := &Bridge{Network: homeserverSkipStub{}}
	if plain.SkipsHomeserver() {
		t.Fatal("SkipHomeserver false must keep homeserver")
	}
	skipping := &Bridge{Network: homeserverSkipStub{skip: true}}
	if !skipping.SkipsHomeserver() {
		t.Fatal("SkipHomeserver true must skip homeserver")
	}
}

func TestPostStartSkipsHomeserverDoesNotTouchDB(t *testing.T) {
	br := &Bridge{Network: homeserverSkipStub{skip: true}}
	br.PostStart(context.Background())
	if br.DB != nil {
		t.Fatal("skipped PostStart must not create or use a database")
	}
}
