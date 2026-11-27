package inbox

import (
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// ExtractedMessage 收件箱模式从 WhatsApp 实时事件抽出的字段，不经 Matrix 转换。
type ExtractedMessage struct {
	MsgKind       string
	Body          string
	GroupName     string
	Skip          bool // 编辑/撤回/表情等收件箱不保存的类型
	DirectPath    string
	MediaKey      []byte
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     string
	MimeType      string
	Size          int64
	Width         int
	Height        int
	DurationMs    int
	Filename      string
	HasMedia      bool
}

// ExtractMessage 将 WhatsApp 实时消息提取为收件箱字段。
func ExtractMessage(msg *waE2E.Message, parsedType string) ExtractedMessage {
	out := ExtractedMessage{MsgKind: "unknown", Body: ""}
	if msg == nil {
		out.Skip = true
		return out
	}
	switch {
	case strings.HasPrefix(parsedType, "reaction"), parsedType == "reaction remove",
		parsedType == "encrypted reaction", parsedType == "edit", parsedType == "revoke",
		parsedType == "encrypted comment", parsedType == "comment",
		strings.HasPrefix(parsedType, "poll "), parsedType == "ignore",
		strings.HasPrefix(parsedType, "unknown_protocol_"):
		out.Skip = true
		return out
	}

	switch {
	case msg.GetConversation() != "" || msg.GetExtendedTextMessage() != nil:
		out.MsgKind = "text"
		if t := msg.GetExtendedTextMessage().GetText(); t != "" {
			out.Body = t
		} else {
			out.Body = msg.GetConversation()
		}
	case msg.GetImageMessage() != nil:
		im := msg.GetImageMessage()
		out.MsgKind = "image"
		out.Body = im.GetCaption()
		fillInboxMedia(&out, im.GetDirectPath(), im.GetMediaKey(), im.GetFileSHA256(), im.GetFileEncSHA256(),
			string(whatsmeow.MediaImage), im.GetMimetype(), int64(im.GetFileLength()),
			int(im.GetWidth()), int(im.GetHeight()), 0, "")
	case msg.GetAudioMessage() != nil:
		am := msg.GetAudioMessage()
		out.MsgKind = "audio"
		fillInboxMedia(&out, am.GetDirectPath(), am.GetMediaKey(), am.GetFileSHA256(), am.GetFileEncSHA256(),
			string(whatsmeow.MediaAudio), am.GetMimetype(), int64(am.GetFileLength()),
			0, 0, int(am.GetSeconds())*1000, "")
	case msg.GetVideoMessage() != nil:
		vm := msg.GetVideoMessage()
		out.MsgKind = "video"
		out.Body = vm.GetCaption()
		fillInboxMedia(&out, vm.GetDirectPath(), vm.GetMediaKey(), vm.GetFileSHA256(), vm.GetFileEncSHA256(),
			string(whatsmeow.MediaVideo), vm.GetMimetype(), int64(vm.GetFileLength()),
			int(vm.GetWidth()), int(vm.GetHeight()), int(vm.GetSeconds())*1000, "")
	case msg.GetPtvMessage() != nil:
		vm := msg.GetPtvMessage()
		out.MsgKind = "video"
		out.Body = vm.GetCaption()
		fillInboxMedia(&out, vm.GetDirectPath(), vm.GetMediaKey(), vm.GetFileSHA256(), vm.GetFileEncSHA256(),
			string(whatsmeow.MediaVideo), vm.GetMimetype(), int64(vm.GetFileLength()),
			int(vm.GetWidth()), int(vm.GetHeight()), int(vm.GetSeconds())*1000, "")
	case msg.GetStickerMessage() != nil:
		sm := msg.GetStickerMessage()
		out.MsgKind = "unknown"
		out.Body = "sticker"
		fillInboxMedia(&out, sm.GetDirectPath(), sm.GetMediaKey(), sm.GetFileSHA256(), sm.GetFileEncSHA256(),
			string(whatsmeow.MediaImage), sm.GetMimetype(), int64(sm.GetFileLength()),
			int(sm.GetWidth()), int(sm.GetHeight()), 0, "sticker.webp")
	case msg.GetDocumentMessage() != nil:
		dm := msg.GetDocumentMessage()
		out.MsgKind = "unknown"
		out.Body = firstNonEmpty(dm.GetCaption(), dm.GetFileName(), "document")
		fillInboxMedia(&out, dm.GetDirectPath(), dm.GetMediaKey(), dm.GetFileSHA256(), dm.GetFileEncSHA256(),
			string(whatsmeow.MediaDocument), dm.GetMimetype(), int64(dm.GetFileLength()),
			0, 0, 0, dm.GetFileName())
	case msg.GetContactMessage() != nil, msg.GetContactsArrayMessage() != nil:
		out.MsgKind = "unknown"
		out.Body = "contact"
	case msg.GetLocationMessage() != nil, msg.GetLiveLocationMessage() != nil:
		out.MsgKind = "unknown"
		out.Body = "location"
	case msg.GetGroupInviteMessage() != nil:
		out.MsgKind = "system"
		out.Body = "group invite"
		if n := msg.GetGroupInviteMessage().GetGroupName(); n != "" {
			out.GroupName = n
		}
	case msg.GetProtocolMessage() != nil &&
		msg.GetProtocolMessage().GetType() == waE2E.ProtocolMessage_EPHEMERAL_SETTING:
		out.MsgKind = "system"
		out.Body = "disappearing timer change"
	default:
		out.MsgKind = "unknown"
		if parsedType != "" && parsedType != "ignore" {
			out.Body = parsedType
		}
	}
	return out
}

func fillInboxMedia(out *ExtractedMessage, directPath string, key, sha, encSha []byte, mediaType, mime string, size int64, w, h, durMs int, filename string) {
	out.HasMedia = len(key) > 0 || directPath != ""
	out.DirectPath = directPath
	out.MediaKey = key
	out.FileSHA256 = sha
	out.FileEncSHA256 = encSha
	out.MediaType = mediaType
	out.MimeType = mime
	out.Size = size
	out.Width = w
	out.Height = h
	out.DurationMs = durMs
	out.Filename = filename
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// Direction 返回 Router 识别的收发方向。
func Direction(info types.MessageInfo) string {
	if info.IsFromMe {
		return "out"
	}
	return "in"
}
