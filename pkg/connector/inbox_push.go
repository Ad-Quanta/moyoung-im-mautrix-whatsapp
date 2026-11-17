package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// inboxChatNameEvent 仅更新群正式名称，不写消息行。
type inboxChatNameEvent struct {
	LoginID   string `json:"login_id"`
	ChatID    string `json:"chat_id"`
	GroupName string `json:"group_name"`
}

func (c *inboxPushClient) PushMessage(ctx context.Context, evt inboxPushEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	return c.postJSON(ctx, c.url, evt)
}

func (c *inboxPushClient) PushChatName(ctx context.Context, evt inboxChatNameEvent) error {
	if c == nil {
		return fmt.Errorf("inbox push client is not configured")
	}
	endpoint := c.url
	if strings.HasSuffix(endpoint, "/events") {
		endpoint = strings.TrimSuffix(endpoint, "/events") + "/chats"
	} else {
		endpoint = endpoint + "/chats"
	}
	return c.postJSON(ctx, endpoint, evt)
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
	if c.secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.secret)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("inbox push HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
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
