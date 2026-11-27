package inbox

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/wamedia"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Send 通过 WhatsApp 会话发送文字、图片或语音，并返回 Router 保存的媒体下载凭据。
func Send(ctx context.Context, client *whatsmeow.Client, req SendRequest) (*SendResponse, error) {
	chatJID, err := types.ParseJID(strings.TrimSpace(req.ChatID))
	if err != nil || chatJID.IsEmpty() {
		return nil, fmt.Errorf("invalid chat_id")
	}
	msgType := strings.ToLower(strings.TrimSpace(req.Type))
	var waMsg *waE2E.Message
	var uploaded whatsmeow.UploadResponse
	var hasUpload bool
	var mediaType whatsmeow.MediaType
	var mime string

	switch msgType {
	case "text":
		text := req.Body
		waMsg = &waE2E.Message{Conversation: proto.String(text)}
	case "image", "audio":
		data, decErr := decodeBase64(req.DataBase64)
		if decErr != nil {
			return nil, fmt.Errorf("decode data_base64: %w", decErr)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("media data is empty")
		}
		mime = strings.TrimSpace(req.MimeType)
		if mime == "" {
			mime = http.DetectContentType(data)
		}
		if msgType == "image" {
			mediaType = whatsmeow.MediaImage
			switch mime {
			case "image/jpeg":
				// ok
			case "image/png", "image/webp":
				converted, convErr := wamedia.ConvertImageToJPEG(data)
				if convErr != nil {
					return nil, fmt.Errorf("convert image to jpeg: %w", convErr)
				}
				data = converted
				mime = "image/jpeg"
			default:
				return nil, fmt.Errorf("unsupported image mime: %s", mime)
			}
			uploaded, err = client.Upload(ctx, data, mediaType)
			if err != nil {
				return nil, fmt.Errorf("upload image: %w", err)
			}
			// 上传成功后必须标记，否则响应不会带回 DirectPath/密钥，出站媒体无法再下载。
			hasUpload = true
			width := uint32(req.Width)
			height := uint32(req.Height)
			waMsg = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(req.Body),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
				Width:         &width,
				Height:        &height,
			}}
		} else {
			mediaType = whatsmeow.MediaAudio
			uploaded, err = client.Upload(ctx, data, mediaType)
			if err != nil {
				return nil, fmt.Errorf("upload audio: %w", err)
			}
			// 上传成功后必须标记，否则响应不会带回 DirectPath/密钥，出站媒体无法再下载。
			hasUpload = true
			seconds := uint32(0)
			if req.DurationMs > 0 {
				seconds = uint32((req.DurationMs + 999) / 1000)
			}
			waMsg = &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				Mimetype:      proto.String(mime),
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
				Seconds:       &seconds,
				PTT:           proto.Bool(true),
			}}
		}
	default:
		return nil, fmt.Errorf("unsupported type %q (text|image|audio)", req.Type)
	}

	msgID := client.GenerateMessageID()
	resp, err := client.SendMessage(ctx, chatJID, waMsg, whatsmeow.SendRequestExtra{ID: msgID})
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}
	out := &SendResponse{
		OK:        true,
		MessageID: string(resp.ID),
		SenderID:  resp.Sender.String(),
		Timestamp: resp.Timestamp.UTC().Format(time.RFC3339Nano),
	}
	if hasUpload {
		out.DirectPath = uploaded.DirectPath
		out.MediaKey = EncodeMediaKey(uploaded.MediaKey)
		out.FileSHA256 = EncodeMediaKey(uploaded.FileSHA256)
		out.FileEncSHA256 = EncodeMediaKey(uploaded.FileEncSHA256)
		out.FileLength = uploaded.FileLength
		out.MimeType = mime
		out.MediaType = string(mediaType)
	}
	return out, nil
}

// Download 根据 Router 保存的媒体凭据下载文件，过期时调用当前会话的手机重传处理。
func Download(ctx context.Context, client *whatsmeow.Client, req DownloadRequest, retryFromPhone func(context.Context, string, *wamedia.DownloadInfo) error) ([]byte, string, error) {
	mediaKey, err := decodeBase64(req.MediaKey)
	if err != nil {
		return nil, "", fmt.Errorf("media_key: %w", err)
	}
	fileSHA, err := decodeBase64(req.FileSHA256)
	if err != nil {
		return nil, "", fmt.Errorf("file_sha256: %w", err)
	}
	fileEncSHA, err := decodeBase64(req.FileEncSHA256)
	if err != nil {
		return nil, "", fmt.Errorf("file_enc_sha256: %w", err)
	}
	mediaType := whatsmeow.MediaType(strings.TrimSpace(req.MediaType))
	if mediaType == "" {
		mediaType = whatsmeow.MediaImage
	}
	directPath := strings.TrimSpace(req.DirectPath)
	if directPath == "" || !strings.HasPrefix(directPath, "/") {
		return nil, "", fmt.Errorf("invalid direct_path %q: inbox media metadata missing; usually caused by outbound send not persisting upload keys", directPath)
	}
	keys := &wamedia.DownloadInfo{
		Key:        mediaKey,
		Length:     req.FileLength,
		Type:       mediaType,
		SHA256:     fileSHA,
		EncSHA256:  fileEncSHA,
		DirectPath: directPath,
		MimeType:   req.MimeType,
	}
	data, err := client.DownloadMediaWithPath(ctx, keys.DirectPath, keys.EncSHA256, keys.SHA256, keys.Key, keys.Type, "file", false)
	if err != nil && (errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) ||
		errors.Is(err, whatsmeow.ErrNoURLPresent)) {
		msgID := composeInboxMediaMessageID(req)
		if msgID != "" && retryFromPhone != nil {
			if waitErr := retryFromPhone(ctx, msgID, keys); waitErr != nil {
				return nil, "", fmt.Errorf("request media from phone: %w", waitErr)
			}
			data, err = client.DownloadMediaWithPath(ctx, keys.DirectPath, keys.EncSHA256, keys.SHA256, keys.Key, keys.Type, "file", false)
		}
	}
	if err != nil {
		return nil, "", err
	}
	mime := strings.TrimSpace(req.MimeType)
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	return data, mime, nil
}

// composeInboxMediaMessageID 从 Router 保存的字段拼出手机媒体重传需要的消息键。
func composeInboxMediaMessageID(req DownloadRequest) string {
	raw := strings.TrimSpace(req.MessageID)
	chatID := strings.TrimSpace(req.ChatID)
	senderID := strings.TrimSpace(req.SenderID)
	if chatID != "" && senderID != "" {
		idPart := raw
		if parts := strings.Split(raw, ":"); len(parts) == 3 {
			idPart = parts[2]
		}
		if idPart == "" {
			return ""
		}
		return chatID + ":" + senderID + ":" + idPart
	}
	return raw
}

// EncodeMediaKey 把 WhatsApp 二进制媒体字段编码为 Router 协议中的 Base64。
func EncodeMediaKey(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

func decodeBase64(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}
