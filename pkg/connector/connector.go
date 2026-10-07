package connector

import (
	"context"
	_ "embed"
	"errors"
	"sync"
	"time"

	"go.mau.fi/util/configupgrade"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

//go:embed example-config.yaml
var ExampleConfig string

type Config struct {
	Proxy       string `yaml:"proxy"`
	GetProxyURL string `yaml:"get_proxy_url"`
}

type Connector struct {
	bridge     *bridgev2.Bridge
	loginMu    sync.Mutex
	loggingOut map[networkid.UserLoginID]bool
	Config     Config
}

func (c *Connector) Init(b *bridgev2.Bridge) {
	c.bridge = b
	b.DB.Log = credentialSafeDBLogger{b.DB.Log}
	b.Config.PortalEventBuffer = 0
}

type credentialSafeDBLogger struct {
	dbutil.DatabaseLogger
}

func (l credentialSafeDBLogger) QueryTiming(ctx context.Context, method, query string, _ []any, nrows int, duration time.Duration, err error) {
	// Login metadata contains credentials; SQL errors can also include row values.
	if err != nil {
		err = errors.New("database query failed")
	}
	l.DatabaseLogger.QueryTiming(ctx, method, query, nil, nrows, duration, err)
}

func (c *Connector) Start(context.Context) error {
	if !c.bridge.Config.SplitPortals || c.bridge.Config.AsyncEvents || c.bridge.Config.PortalEventBuffer != 0 {
		return errors.New("ChatGPT Dots requires split portals and synchronous, unbuffered portal events")
	}
	return nil
}

func (c *Connector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{DisplayName: "ChatGPT Dots", NetworkURL: "https://chatgpt.com", NetworkID: "chatgpt-dots", BeeperBridgeType: "chatgpt-dots", DefaultPort: 29349}
}
func (c *Connector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{UserLogin: func() any { return &LoginMetadata{} }, Message: func() any { return &MessageMetadata{} }}
}
func (c *Connector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func (c *Connector) GetBridgeInfoVersion() (int, int) { return 2, 4 }
func (c *Connector) GetConfig() (string, any, configupgrade.Upgrader) {
	return ExampleConfig, &c.Config, configupgrade.SimpleUpgrader(func(helper configupgrade.Helper) {
		helper.Copy(configupgrade.Str|configupgrade.Null, "proxy")
		helper.Copy(configupgrade.Str|configupgrade.Null, "get_proxy_url")
	})
}
func (c *Connector) LoadUserLogin(_ context.Context, l *bridgev2.UserLogin) error {
	l.Client = &Client{connector: c, login: l, meta: l.Metadata.(*LoginMetadata)}
	return nil
}

var _ bridgev2.NetworkConnector = (*Connector)(nil)
