// Package inbox 定义 WhatsApp 实时消息及其与 Router 的数据协议。
package inbox

// MessageEvent 桥 → im-router 的推送载荷；密钥字段仅内网使用。
type MessageEvent struct {
	LoginID    string        `json:"login_id"`
	ChatID     string        `json:"chat_id"`
	ExternalID string        `json:"external_id"`
	Direction  string        `json:"direction"`
	SenderID   string        `json:"sender_id"`
	MsgKind    string        `json:"msg_kind"`
	Body       string        `json:"body"`
	SentAt     string        `json:"sent_at"` // RFC3339
	GroupName  string        `json:"group_name,omitempty"`
	Media      *MessageMedia `json:"media,omitempty"`
}

// MessageMedia 是 Router 保存并用于向 WhatsApp 请求下载的媒体字段。
type MessageMedia struct {
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

// ChatNameEvent 仅更新群正式名称，不写消息行；group_name 为空表示空名终态。
type ChatNameEvent struct {
	LoginID   string `json:"login_id"`
	ChatID    string `json:"chat_id"`
	GroupName string `json:"group_name"`
}

// ChatNameLookup 是 Router 单个群名查询结果；Found=true 且 GroupName 为空表示空名终态。
type ChatNameLookup struct {
	Found     bool   `json:"found"`
	GroupName string `json:"group_name"`
}

// SendRequest 是 Router 发给 WhatsApp 的发送请求。
type SendRequest struct {
	ChatID     string `json:"chat_id"`
	Type       string `json:"type"` // text | image | audio
	Body       string `json:"body"`
	MimeType   string `json:"mime_type"`
	Filename   string `json:"filename"`
	DataBase64 string `json:"data_base64"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	DurationMs int    `json:"duration_ms"`
}

// SendResponse 返回 WhatsApp 消息 ID 与已上传媒体的下载元数据。
type SendResponse struct {
	OK            bool   `json:"ok"`
	MessageID     string `json:"message_id"`
	SenderID      string `json:"sender_id"`
	Timestamp     string `json:"timestamp"`
	DirectPath    string `json:"direct_path,omitempty"`
	MediaKey      string `json:"media_key,omitempty"`
	FileSHA256    string `json:"file_sha256,omitempty"`
	FileEncSHA256 string `json:"file_enc_sha256,omitempty"`
	FileLength    uint64 `json:"file_length,omitempty"`
	MimeType      string `json:"mime_type,omitempty"`
	MediaType     string `json:"media_type,omitempty"`
}

// DownloadRequest 携带 Router 持久化的 WhatsApp 媒体元数据。
type DownloadRequest struct {
	MessageID     string `json:"message_id"`
	ChatID        string `json:"chat_id"`
	SenderID      string `json:"sender_id"`
	DirectPath    string `json:"direct_path"`
	MediaKey      string `json:"media_key"`
	FileSHA256    string `json:"file_sha256"`
	FileEncSHA256 string `json:"file_enc_sha256"`
	MediaType     string `json:"media_type"`
	FileLength    uint64 `json:"file_length"`
	MimeType      string `json:"mime_type"`
}
