package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/msgconv"
)

const maxInboxProvisionBody = 10 << 20 // 10MiB：手表图片上限 8MB + JSON 开销

type inboxSendRequest struct {
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

type inboxSendResponse struct {
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

type inboxDownloadRequest struct {
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

func (wa *WhatsAppConnector) initInboxProvision(ctx context.Context) error {
	if !wa.Config.Inbox.Enabled {
		return nil
	}
	provisioning, ok := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return fmt.Errorf("matrix connector does not expose provisioning API")
	}
	wa.inboxRouteOnce.Do(func() {
		r := provisioning.GetProvisioning().GetRouter()
		r.HandleFunc("POST /v3/inbox/send/{loginID}", wa.postInboxSend)
		r.HandleFunc("POST /v3/inbox/download/{loginID}", wa.postInboxDownload)
	})
	wa.Bridge.Log.Info().
		Str("push_url", wa.Config.Inbox.PushURL).
		Msg("WhatsApp inbox mode provisioning routes registered")
	return nil
}

func writeInboxJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func writeInboxError(w http.ResponseWriter, statusCode int, err error) {
	writeInboxJSON(w, statusCode, map[string]string{"error": err.Error()})
}

func (wa *WhatsAppConnector) resolveInboxLogin(r *http.Request) (*WhatsAppClient, error) {
	loginID := networkid.UserLoginID(strings.TrimSpace(r.PathValue("loginID")))
	if loginID == "" || loginID == "all" {
		return nil, fmt.Errorf("invalid login ID")
	}
	provisioning := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning).GetProvisioning()
	user := provisioning.GetUser(r)
	if user == nil {
		return nil, errInboxUnauthorized
	}
	login, err := wa.Bridge.GetExistingUserLoginByID(r.Context(), loginID)
	if err != nil {
		return nil, fmt.Errorf("load login: %w", err)
	}
	if login == nil || login.UserMXID != user.MXID {
		return nil, errInboxNotFound
	}
	client, ok := login.Client.(*WhatsAppClient)
	if !ok || client == nil || client.Client == nil {
		return nil, fmt.Errorf("whatsapp client is not ready")
	}
	return client, nil
}

var (
	errInboxUnauthorized = errors.New("provisioning user is unavailable")
	errInboxNotFound     = errors.New("login not found")
)

func (wa *WhatsAppConnector) postInboxSend(w http.ResponseWriter, r *http.Request) {
	client, err := wa.resolveInboxLogin(r)
	if errors.Is(err, errInboxUnauthorized) {
		writeInboxError(w, http.StatusUnauthorized, err)
		return
	} else if errors.Is(err, errInboxNotFound) {
		writeInboxError(w, http.StatusNotFound, err)
		return
	} else if err != nil {
		writeInboxError(w, http.StatusBadGateway, err)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxInboxProvisionBody+1))
	if err != nil {
		writeInboxError(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
		return
	}
	if len(raw) > maxInboxProvisionBody {
		writeInboxError(w, http.StatusRequestEntityTooLarge, fmt.Errorf("request body too large"))
		return
	}
	var req inboxSendRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		writeInboxError(w, http.StatusBadRequest, fmt.Errorf("invalid json: %w", err))
		return
	}
	resp, err := client.inboxSend(r.Context(), req)
	if err != nil {
		writeInboxError(w, http.StatusBadGateway, err)
		return
	}
	writeInboxJSON(w, http.StatusOK, resp)
}

func (wa *WhatsAppConnector) postInboxDownload(w http.ResponseWriter, r *http.Request) {
	client, err := wa.resolveInboxLogin(r)
	if errors.Is(err, errInboxUnauthorized) {
		writeInboxError(w, http.StatusUnauthorized, err)
		return
	} else if errors.Is(err, errInboxNotFound) {
		writeInboxError(w, http.StatusNotFound, err)
		return
	} else if err != nil {
		writeInboxError(w, http.StatusBadGateway, err)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeInboxError(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
		return
	}
	var req inboxDownloadRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		writeInboxError(w, http.StatusBadRequest, fmt.Errorf("invalid json: %w", err))
		return
	}
	data, mime, err := client.inboxDownload(r.Context(), req)
	if err != nil {
		writeInboxError(w, http.StatusBadGateway, err)
		return
	}
	if mime == "" {
		mime = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (wa *WhatsAppClient) inboxSend(ctx context.Context, req inboxSendRequest) (*inboxSendResponse, error) {
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
		data, decErr := mustDecodeB64(req.DataBase64)
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
				converted, convErr := wa.Main.MsgConv.ConvertImageToJPEGPublic(data)
				if convErr != nil {
					return nil, fmt.Errorf("convert image to jpeg: %w", convErr)
				}
				data = converted
				mime = "image/jpeg"
			default:
				return nil, fmt.Errorf("unsupported image mime: %s", mime)
			}
			uploaded, err = wa.Client.Upload(ctx, data, mediaType)
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
			uploaded, err = wa.Client.Upload(ctx, data, mediaType)
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

	msgID := wa.Client.GenerateMessageID()
	resp, err := wa.Client.SendMessage(ctx, chatJID, waMsg, whatsmeow.SendRequestExtra{ID: msgID})
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}
	out := &inboxSendResponse{
		OK:        true,
		MessageID: string(resp.ID),
		SenderID:  resp.Sender.String(),
		Timestamp: resp.Timestamp.UTC().Format(time.RFC3339Nano),
	}
	if hasUpload {
		out.DirectPath = uploaded.DirectPath
		out.MediaKey = b64(uploaded.MediaKey)
		out.FileSHA256 = b64(uploaded.FileSHA256)
		out.FileEncSHA256 = b64(uploaded.FileEncSHA256)
		out.FileLength = uploaded.FileLength
		out.MimeType = mime
		out.MediaType = string(mediaType)
	}
	return out, nil
}

func (wa *WhatsAppClient) inboxDownload(ctx context.Context, req inboxDownloadRequest) ([]byte, string, error) {
	mediaKey, err := mustDecodeB64(req.MediaKey)
	if err != nil {
		return nil, "", fmt.Errorf("media_key: %w", err)
	}
	fileSHA, err := mustDecodeB64(req.FileSHA256)
	if err != nil {
		return nil, "", fmt.Errorf("file_sha256: %w", err)
	}
	fileEncSHA, err := mustDecodeB64(req.FileEncSHA256)
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
	keys := &msgconv.FailedMediaKeys{
		Key:        mediaKey,
		Length:     req.FileLength,
		Type:       mediaType,
		SHA256:     fileSHA,
		EncSHA256:  fileEncSHA,
		DirectPath: directPath,
		MimeType:   req.MimeType,
	}
	data, err := wa.Client.DownloadMediaWithPath(ctx, keys.DirectPath, keys.EncSHA256, keys.SHA256, keys.Key, keys.Type, "file", false)
	if err != nil && (errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) ||
		errors.Is(err, whatsmeow.ErrNoURLPresent)) {
		msgID := composeInboxMediaMessageID(req)
		if msgID != "" {
			if waitErr := wa.requestAndWaitDirectMedia(ctx, msgID, keys); waitErr != nil {
				return nil, "", fmt.Errorf("request media from phone: %w", waitErr)
			}
			data, err = wa.Client.DownloadMediaWithPath(ctx, keys.DirectPath, keys.EncSHA256, keys.SHA256, keys.Key, keys.Type, "file", false)
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

// composeInboxMediaMessageID 将 chat/sender/id 拼成 waid 所需的 MessageID。
// sendMediaRequestDirect 依赖 ParseMessageID；仅传裸 WhatsApp ID 会失败。
func composeInboxMediaMessageID(req inboxDownloadRequest) networkid.MessageID {
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
		return networkid.MessageID(fmt.Sprintf("%s:%s:%s", chatID, senderID, idPart))
	}
	return networkid.MessageID(raw)
}
