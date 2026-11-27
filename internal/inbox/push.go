package inbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// PushClient 将实时消息推给 im-router 收件箱，并提供群名查询。
type PushClient struct {
	url    string
	secret string
	client *http.Client
}

// NewPushClient 从 Inbox 配置创建 Router 内部 HTTP 客户端。
func NewPushClient(url, secret string) *PushClient {
	url = strings.TrimSpace(url)
	secret = strings.TrimSpace(secret)
	if url == "" {
		return nil
	}
	return &PushClient{
		url:    strings.TrimRight(url, "/"),
		secret: secret,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *PushClient) PushMessage(ctx context.Context, evt MessageEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	return c.postJSON(ctx, c.url, evt)
}

func (c *PushClient) chatsEndpoint() string {
	endpoint := strings.TrimRight(c.url, "/")
	if strings.HasSuffix(endpoint, "/events") {
		return strings.TrimSuffix(endpoint, "/events") + "/chats"
	}
	return endpoint + "/chats"
}

func (c *PushClient) PushChatName(ctx context.Context, evt ChatNameEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	return c.postJSON(ctx, c.chatsEndpoint(), evt)
}

// LookupChatName 查询 Router 是否已有该群名记录；空名记录也是有效终态。
func (c *PushClient) LookupChatName(ctx context.Context, loginID, chatID string) (ChatNameLookup, error) {
	var out ChatNameLookup
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

func (c *PushClient) postJSON(ctx context.Context, endpoint string, payload any) error {
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

func (c *PushClient) setAuth(req *http.Request) {
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}
}

func (c *PushClient) do(req *http.Request) (*http.Response, error) {
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
