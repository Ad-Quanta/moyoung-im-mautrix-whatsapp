package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inbox"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxhttp"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxservice"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/zeroconfig"
	"gopkg.in/yaml.v3"
)

// inboxConfig 读取已部署 YAML 中独立服务使用的字段。
type inboxConfig struct {
	Listen     string `yaml:"listen"`
	AppService struct {
		Hostname string `yaml:"hostname"`
		Port     uint16 `yaml:"port"`
	} `yaml:"appservice"`
	Database     dbutil.Config     `yaml:"database"`
	Logging      zeroconfig.Config `yaml:"logging"`
	Provisioning struct {
		SharedSecret   string `yaml:"shared_secret"`
		FailOnWebAuthn bool   `yaml:"fail_on_webauthn"`
	} `yaml:"provisioning"`
	Bridge struct {
		Permissions map[string]loginPermission `yaml:"permissions"`
	} `yaml:"bridge"`
	Network         networkConfig `yaml:"network"`
	contactTemplate *template.Template
}

type networkConfig struct {
	DisplayNameTemplate string `yaml:"displayname_template"`

	OSName                string `yaml:"os_name"`
	BrowserName           string `yaml:"browser_name"`
	Proxy                 string `yaml:"proxy"`
	GetProxyURL           string `yaml:"get_proxy_url"`
	ProxyOnlyLogin        bool   `yaml:"proxy_only_login"`
	EnableStatusBroadcast bool   `yaml:"enable_status_broadcast"`
	InitialAutoReconnect  bool   `yaml:"initial_auto_reconnect"`
	UseRetryStore         bool   `yaml:"use_whatsapp_retry_store"`
	ForceActiveReceipts   bool   `yaml:"force_active_delivery_receipts"`
	Inbox                 struct {
		PushURL    string `yaml:"push_url"`
		PushSecret string `yaml:"push_secret"`
	} `yaml:"inbox"`
}

type loginPermission struct {
	Login     bool `yaml:"login"`
	MaxLogins int  `yaml:"max_logins"`
}

// UnmarshalYAML 保留命名权限、旧整数权限及单独配置登录上限的写法。
func (p *loginPermission) UnmarshalYAML(node *yaml.Node) error {
	switch node.Tag {
	case "!!str":
		switch strings.ToLower(node.Value) {
		case "user", "admin":
			p.Login = true
		case "block", "relay", "commands":
			p.Login = false
		default:
			return fmt.Errorf("无效的登录权限级别 %q", node.Value)
		}
		return nil
	case "!!int":
		var value int
		if err := node.Decode(&value); err != nil {
			return err
		}
		p.Login = value >= 10
		return nil
	default:
		type rawPermission loginPermission
		return node.Decode((*rawPermission)(p))
	}
}

func loadInboxConfig(path string) (*inboxConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &inboxConfig{Network: networkConfig{OSName: "WhatsApp Inbox", BrowserName: "unknown", DisplayNameTemplate: `{{or .BusinessName .PushName .Phone .RedactedPhone "Unknown user"}} (WA)`, EnableStatusBroadcast: true, InitialAutoReconnect: true}}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if len(cfg.Provisioning.SharedSecret) < 16 {
		return nil, fmt.Errorf("provisioning.shared_secret 至少需要 16 字节")
	}
	if strings.TrimSpace(cfg.Network.Inbox.PushURL) == "" || strings.TrimSpace(cfg.Network.Inbox.PushSecret) == "" {
		return nil, fmt.Errorf("network.inbox.push_url 和 push_secret 必须配置")
	}
	cfg.Listen = strings.TrimSpace(cfg.Listen)
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:29318"
		if cfg.AppService.Port != 0 {
			cfg.Listen = net.JoinHostPort(cfg.AppService.Hostname, strconv.Itoa(int(cfg.AppService.Port)))
		}
	}
	cfg.contactTemplate, err = template.New("contact-name").Parse(cfg.Network.DisplayNameTemplate)
	if err != nil {
		return nil, fmt.Errorf("解析联系人名称模板: %w", err)
	}
	if _, err = cfg.contactName(inboxservice.Contact{}); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (cfg *inboxConfig) httpConfig() inboxhttp.Config {
	return inboxhttp.Config{SharedSecret: cfg.Provisioning.SharedSecret, FailOnWebAuthn: cfg.Provisioning.FailOnWebAuthn, LoginPermission: cfg.loginPermission, ContactName: cfg.contactName}
}

// contactName 沿用部署中的联系人备注和名称模板，也兼容旧模板字段。
func (cfg *inboxConfig) contactName(contact inboxservice.Contact) (string, error) {
	info, phone := contact.Info, contact.Phone
	if info.RedactedPhone == "" && phone != "" {
		info.RedactedPhone = phone
		if len(phone) > 4 {
			info.RedactedPhone = phone[:2] + strings.Repeat("∙", len(phone)-4) + phone[len(phone)-2:]
		}
	}
	params := struct {
		types.ContactInfo
		Phone, JID, Notify, VName, Name, Short string
	}{info, phone, phone, info.PushName, info.BusinessName, info.FullName, info.FirstName}
	var out strings.Builder
	if err := cfg.contactTemplate.Execute(&out, params); err != nil {
		return "", fmt.Errorf("渲染联系人名称: %w", err)
	}
	return out.String(), nil
}

func (cfg *inboxConfig) loginPermission(owner string) (bool, int) {
	permissions := cfg.Bridge.Permissions
	if permissions == nil {
		return true, 0
	}
	if p, ok := permissions[owner]; ok {
		return p.Login, p.MaxLogins
	}
	if strings.HasPrefix(owner, "@") {
		if _, domain, ok := strings.Cut(owner, ":"); ok {
			if p, ok := permissions[domain]; ok {
				return p.Login, p.MaxLogins
			}
		}
	}
	p := permissions["*"]
	return p.Login, p.MaxLogins
}

// applyDeviceProfile 在启动连接前设置 WhatsMeow 的设备标识，始终关闭历史同步请求。
func (cfg networkConfig) applyDeviceProfile() {
	store.DeviceProps.Os = new(cfg.OSName)
	store.DeviceProps.RequireFullSync = new(false)
	if id, ok := waCompanionReg.DeviceProps_PlatformType_value[strings.ToUpper(cfg.BrowserName)]; ok {
		store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_PlatformType(id).Enum()
	}
}

func (cfg networkConfig) sessionConfig() inboxservice.Config {
	return inboxservice.Config{
		InitialAutoReconnect: cfg.InitialAutoReconnect, UseRetryMessageStore: cfg.UseRetryStore, ForceActiveReceipts: cfg.ForceActiveReceipts,
		LiveEvents:      inbox.LiveEventOptions{EnableStatusBroadcast: cfg.EnableStatusBroadcast, DisableNewsletter: store.BaseClientPayload.GetUserAgent().GetPlatform() == waWa6.ClientPayload_UserAgent_MACOS},
		ConfigureClient: cfg.configureClient,
	}
}

var proxyClient = &http.Client{Timeout: 30 * time.Second}

// configureClient 复用部署中的静态代理或按连接用途获取动态代理。
func (cfg networkConfig) configureClient(ctx context.Context, client *whatsmeow.Client, pairing bool) error {
	if cfg.ProxyOnlyLogin && !pairing {
		return nil
	}
	proxy := cfg.Proxy
	if cfg.GetProxyURL != "" {
		endpoint, err := url.Parse(cfg.GetProxyURL)
		if err != nil {
			return fmt.Errorf("解析动态代理地址: %w", err)
		}
		reason := "connect"
		if pairing {
			reason = "login"
		}
		query := endpoint.Query()
		query.Set("reason", reason)
		endpoint.RawQuery = query.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "WhatsApp Inbox")
		resp, err := proxyClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("动态代理返回 HTTP %d", resp.StatusCode)
		}
		var result struct {
			ProxyURL string `json:"proxy_url"`
		}
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return err
		}
		proxy = result.ProxyURL
	}
	if proxy == "" {
		return nil
	}
	return client.SetProxyAddress(proxy, whatsmeow.SetProxyOptions{OnlyLogin: cfg.ProxyOnlyLogin, NoMedia: cfg.ProxyOnlyLogin})
}
