package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// inboxPushClient 将实时消息推给 im-router 收件箱；失败只记日志，不回退 Dendrite。
type inboxPushClient struct {
	url    string
	secret string
	client *http.Client
}

func newInboxPushClient(url, secret string) *inboxPushClient {
	url = strings.TrimSpace(url)
	secret = strings.TrimSpace(secret)
	if url == "" {
		return nil
	}
	return &inboxPushClient{
		url:    strings.TrimRight(url, "/"),
		secret: secret,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// inboxPushEvent 桥 → im-router 的推送载荷；密钥字段仅内网使用。
type inboxPushEvent struct {
	LoginID    string          `json:"login_id"`
	ChatID     string          `json:"chat_id"`
	ExternalID string          `json:"external_id"`
	Direction  string          `json:"direction"`
	SenderID   string          `json:"sender_id"`
	MsgKind    string          `json:"msg_kind"`
	Body       string          `json:"body"`
	SentAt     string          `json:"sent_at"` // RFC3339
	GroupName  string          `json:"group_name,omitempty"`
	Media      *inboxPushMedia `json:"media,omitempty"`
}

type inboxPushMedia struct {
	MimeType         string `json:"mime_type,omitempty"`
	Size             int64  `json:"size,omitempty"`
	Width            int    `json:"width,omitempty"`
	Height           int    `json:"height,omitempty"`
	DurationMs       int    `json:"duration_ms,omitempty"`
	Filename         string `json:"filename,omitempty"`
	DirectPath       string `json:"direct_path,omitempty"`
	MediaKeyB64      string `json:"media_key,omitempty"`
	FileSHA256B64    string `json:"file_sha256,omitempty"`
	FileEncSHA256B64 string `json:"file_enc_sha256,omitempty"`
	MediaType        string `json:"media_type,omitempty"`
}

// inboxChatNameEvent 仅更新群正式名称，不写消息行；group_name 为空表示空名终态。
type inboxChatNameEvent struct {
	LoginID   string `json:"login_id"`
	ChatID    string `json:"chat_id"`
	GroupName string `json:"group_name"`
}

// inboxChatNameLookup 是 Router 单个群名查询结果；Found=true 且 GroupName 为空表示空名终态。
type inboxChatNameLookup struct {
	Found     bool   `json:"found"`
	GroupName string `json:"group_name"`
}

func (c *inboxPushClient) PushMessage(ctx context.Context, evt inboxPushEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	return c.postJSON(ctx, c.url, evt)
}

func (c *inboxPushClient) chatsEndpoint() string {
	endpoint := strings.TrimRight(c.url, "/")
	if strings.HasSuffix(endpoint, "/events") {
		return strings.TrimSuffix(endpoint, "/events") + "/chats"
	}
	return endpoint + "/chats"
}

func (c *inboxPushClient) PushChatName(ctx context.Context, evt inboxChatNameEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	return c.postJSON(ctx, c.chatsEndpoint(), evt)
}

// LookupChatName 查询 Router 是否已有该群名记录；空名记录也是有效终态。
func (c *inboxPushClient) LookupChatName(ctx context.Context, loginID, chatID string) (inboxChatNameLookup, error) {
	var out inboxChatNameLookup
	if c == nil {
		return out, fmt.Errorf("inbox push client is not configured")
	}
	query := url.Values{}
	query.Set("login_id", strings.TrimSpace(loginID))
	query.Set("chat_id", strings.TrimSpace(chatID))
	endpoint := c.chatsEndpoint() + "?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return out, err
	}
	c.setAuth(req)
	resp, err := c.do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("解析群名查询响应失败: %w", err)
	}
	out.GroupName = strings.TrimSpace(out.GroupName)
	return out, nil
}

func (c *inboxPushClient) postJSON(ctx context.Context, endpoint string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *inboxPushClient) setAuth(req *http.Request) {
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}
}

func (c *inboxPushClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("inbox push HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return resp, nil
}

func b64(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

func mustDecodeB64(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}
