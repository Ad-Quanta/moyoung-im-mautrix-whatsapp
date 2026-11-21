package connector

import (
	"context"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// inboxGroupNameRetryAfter 主动拉群名失败（含空名）后的冷却，避免每条消息打 WhatsApp。
const inboxGroupNameRetryAfter = 5 * time.Minute

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
	// 群聊：消息本身通常不带群名。本地已有缓存则附带写入；否则异步 GetGroupInfo 补推。
	if evt.Info.Chat.Server == types.GroupServer {
		if name := strings.TrimSpace(payload.GroupName); name != "" {
			wa.rememberInboxGroupName(evt.Info.Chat, name)
		} else if cached := wa.getInboxGroupName(evt.Info.Chat); cached != "" {
			payload.GroupName = cached
		} else {
			wa.ensureInboxGroupNameAsync(evt.Info.Chat)
		}
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

// handleWAGroupNameInbox 推送正式群名，并写入进程内缓存；不做全量群列表拉取。
func (wa *WhatsAppClient) handleWAGroupNameInbox(ctx context.Context, chatJID types.JID, groupName string) bool {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" || chatJID.IsEmpty() {
		return true
	}
	wa.rememberInboxGroupName(chatJID, groupName)
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

// rememberInboxGroupName 记录已确认的群正式名，供后续消息免重复拉取。
func (wa *WhatsAppClient) rememberInboxGroupName(chatJID types.JID, groupName string) {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" || chatJID.IsEmpty() {
		return
	}
	key := chatJID.String()
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNames == nil {
		wa.inboxGroupNames = make(map[string]string)
	}
	wa.inboxGroupNames[key] = groupName
	if wa.inboxGroupNameFailedAt != nil {
		delete(wa.inboxGroupNameFailedAt, key)
	}
}

// getInboxGroupName 读取进程内已缓存的群名；无缓存返回空串。
func (wa *WhatsAppClient) getInboxGroupName(chatJID types.JID) string {
	if chatJID.IsEmpty() {
		return ""
	}
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNames == nil {
		return ""
	}
	return wa.inboxGroupNames[chatJID.String()]
}

// ensureInboxGroupNameAsync 仅在本地尚无群名时异步 GetGroupInfo；同 JID 单飞，失败进入冷却。
func (wa *WhatsAppClient) ensureInboxGroupNameAsync(chatJID types.JID) {
	if chatJID.IsEmpty() || chatJID.Server != types.GroupServer {
		return
	}
	key := chatJID.String()
	wa.inboxGroupNameMu.Lock()
	if wa.inboxGroupNames == nil {
		wa.inboxGroupNames = make(map[string]string)
	}
	if wa.inboxGroupNameFetching == nil {
		wa.inboxGroupNameFetching = make(map[string]struct{})
	}
	if wa.inboxGroupNameFailedAt == nil {
		wa.inboxGroupNameFailedAt = make(map[string]time.Time)
	}
	if _, ok := wa.inboxGroupNames[key]; ok {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	if _, ok := wa.inboxGroupNameFetching[key]; ok {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	if failedAt, ok := wa.inboxGroupNameFailedAt[key]; ok && time.Since(failedAt) < inboxGroupNameRetryAfter {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	wa.inboxGroupNameFetching[key] = struct{}{}
	wa.inboxGroupNameMu.Unlock()

	go wa.fetchAndPushInboxGroupName(chatJID)
}

// fetchAndPushInboxGroupName 主动拉取单个群正式名并推送到收件箱。
func (wa *WhatsAppClient) fetchAndPushInboxGroupName(chatJID types.JID) {
	key := chatJID.String()
	defer func() {
		wa.inboxGroupNameMu.Lock()
		delete(wa.inboxGroupNameFetching, key)
		wa.inboxGroupNameMu.Unlock()
	}()

	if wa.Client == nil {
		wa.markInboxGroupNameFailed(chatJID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := wa.Client.GetGroupInfo(ctx, chatJID)
	if err != nil || info == nil {
		if wa.UserLogin != nil {
			wa.UserLogin.Log.Warn().Err(err).Stringer("chat_jid", chatJID).
				Msg("Failed to GetGroupInfo for inbox group name backfill")
		}
		wa.markInboxGroupNameFailed(chatJID)
		return
	}
	name := strings.TrimSpace(info.Name)
	if name == "" {
		// 社群默认公告组等可能返回空名；冷却后再试，避免刷接口。
		if wa.UserLogin != nil {
			wa.UserLogin.Log.Debug().Stringer("chat_jid", chatJID).
				Msg("GetGroupInfo returned empty name; skip inbox name push")
		}
		wa.markInboxGroupNameFailed(chatJID)
		return
	}
	wa.handleWAGroupNameInbox(ctx, chatJID, name)
}

// markInboxGroupNameFailed 记录拉取失败时间，进入冷却窗口。
func (wa *WhatsAppClient) markInboxGroupNameFailed(chatJID types.JID) {
	if chatJID.IsEmpty() {
		return
	}
	key := chatJID.String()
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNameFailedAt == nil {
		wa.inboxGroupNameFailedAt = make(map[string]time.Time)
	}
	wa.inboxGroupNameFailedAt[key] = time.Now()
}
