package connector

import (
	"context"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func (wa *WhatsAppClient) inboxEnabled() bool {
	return wa.Main != nil && wa.Main.Config.Inbox.Enabled
}

// handleWAMessageInbox 收件箱模式：实时消息推 im-router，不进 Matrix portal。
func (wa *WhatsAppClient) handleWAMessageInbox(ctx context.Context, evt *events.Message, parsedMessageType string) bool {
	if (evt.Info.Chat == types.StatusBroadcastJID && !wa.Main.Config.EnableStatusBroadcast) ||
		(evt.Info.Chat.Server == types.NewsletterServer && wa.disableNewsletter) ||
		(evt.Info.Chat.Server == types.BroadcastServer && evt.Info.IsFromMe) {
		return true
	}
	extracted := extractInboxMessage(evt.Message, parsedMessageType)
	if extracted.Skip {
		return true
	}
	push := wa.Main.inboxPush
	if push == nil {
		wa.UserLogin.Log.Error().Msg("inbox mode enabled but push client is nil; dropping message (no Dendrite fallback)")
		return true
	}
	payload := inboxPushEvent{
		LoginID:    string(wa.UserLogin.ID),
		ChatID:     evt.Info.Chat.String(),
		ExternalID: evt.Info.ID,
		Direction:  inboxDirection(evt.Info),
		SenderID:   evt.Info.Sender.String(),
		MsgKind:    extracted.MsgKind,
		Body:       extracted.Body,
		SentAt:     evt.Info.Timestamp.UTC().Format(time.RFC3339Nano),
		GroupName:  extracted.GroupName,
	}
	if extracted.HasMedia {
		payload.Media = &inboxPushMedia{
			MimeType:         extracted.MimeType,
			Size:             extracted.Size,
			Width:            extracted.Width,
			Height:           extracted.Height,
			DurationMs:       extracted.DurationMs,
			Filename:         extracted.Filename,
			DirectPath:       extracted.DirectPath,
			MediaKeyB64:      b64(extracted.MediaKey),
			FileSHA256B64:    b64(extracted.FileSHA256),
			FileEncSHA256B64: b64(extracted.FileEncSHA256),
			MediaType:        extracted.MediaType,
		}
	}
	if err := push.PushMessage(ctx, payload); err != nil {
		wa.UserLogin.Log.Err(err).
			Str("message_id", evt.Info.ID).
			Stringer("chat_jid", evt.Info.Chat).
			Msg("Failed to push WhatsApp message to inbox; not falling back to Dendrite")
	}
	return true
}

// handleWACallStartInbox 收件箱模式保存通话事件为 call。
func (wa *WhatsAppClient) handleWACallStartInbox(ctx context.Context, group, sender types.JID, id, callType string, ts time.Time) bool {
	if time.Since(ts) > callEventMaxAge {
		return true
	}
	push := wa.Main.inboxPush
	if push == nil {
		wa.UserLogin.Log.Error().Msg("inbox mode enabled but push client is nil; dropping call event")
		return true
	}
	chat := group
	if chat.IsEmpty() {
		chat = sender
	}
	body := "call"
	if strings.TrimSpace(callType) != "" {
		body = "call:" + callType
	}
	payload := inboxPushEvent{
		LoginID:    string(wa.UserLogin.ID),
		ChatID:     chat.String(),
		ExternalID: id,
		Direction:  "in",
		SenderID:   sender.String(),
		MsgKind:    "call",
		Body:       body,
		SentAt:     ts.UTC().Format(time.RFC3339Nano),
	}
	if err := push.PushMessage(ctx, payload); err != nil {
		wa.UserLogin.Log.Err(err).Str("call_id", id).Msg("Failed to push call event to inbox")
	}
	return true
}

// handleWAGroupNameInbox 仅推送已知正式群名，不主动拉群列表。
func (wa *WhatsAppClient) handleWAGroupNameInbox(ctx context.Context, chatJID types.JID, groupName string) bool {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" || chatJID.IsEmpty() {
		return true
	}
	push := wa.Main.inboxPush
	if push == nil {
		return true
	}
	if err := push.PushChatName(ctx, inboxChatNameEvent{
		LoginID:   string(wa.UserLogin.ID),
		ChatID:    chatJID.String(),
		GroupName: groupName,
	}); err != nil {
		wa.UserLogin.Log.Err(err).Stringer("chat_jid", chatJID).Msg("Failed to push group name to inbox")
	}
	return true
}
