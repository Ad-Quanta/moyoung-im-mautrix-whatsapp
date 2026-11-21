package connector

import (
	"context"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// inboxGroupNameLookupRetryAfter 仅用于 Router 查询瞬时失败后的本地冷却；不用于重试 GetGroupInfo。
const inboxGroupNameLookupRetryAfter = 10 * time.Minute

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
	// 群聊：消息本身通常不带群名。内存命中则直接复用；未命中才异步走 DB→GetGroupInfo 补齐。
	if evt.Info.Chat.Server == types.GroupServer {
		if name := strings.TrimSpace(payload.GroupName); name != "" {
			wa.rememberInboxGroupName(evt.Info.Chat, name)
		} else if cached, found := wa.getInboxGroupName(evt.Info.Chat); found {
			// found 且空串表示已确认终态，不再触发 DB/WhatsApp 查询。
			if cached != "" {
				payload.GroupName = cached
			}
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

// rememberInboxGroupName 记录已解析群名；空串表示空名终态，后续不再自动 GetGroupInfo。
func (wa *WhatsAppClient) rememberInboxGroupName(chatJID types.JID, groupName string) {
	if chatJID.IsEmpty() {
		return
	}
	groupName = strings.TrimSpace(groupName)
	key := chatJID.String()
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNames == nil {
		wa.inboxGroupNames = make(map[string]string)
	}
	wa.inboxGroupNames[key] = groupName
	if wa.inboxGroupNameTempFailAt != nil {
		delete(wa.inboxGroupNameTempFailAt, key)
	}
}

// getInboxGroupName 读取进程内群名解析结果；found=false 表示尚未解析。
func (wa *WhatsAppClient) getInboxGroupName(chatJID types.JID) (name string, found bool) {
	if chatJID.IsEmpty() {
		return "", false
	}
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNames == nil {
		return "", false
	}
	name, found = wa.inboxGroupNames[chatJID.String()]
	return name, found
}

// ensureInboxGroupNameAsync 仅在内存未解析时异步补齐：先查 Router DB，无记录再 GetGroupInfo 一次。
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
	if wa.inboxGroupNameTempFailAt == nil {
		wa.inboxGroupNameTempFailAt = make(map[string]time.Time)
	}
	if _, ok := wa.inboxGroupNames[key]; ok {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	if _, ok := wa.inboxGroupNameFetching[key]; ok {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	if failedAt, ok := wa.inboxGroupNameTempFailAt[key]; ok && time.Since(failedAt) < inboxGroupNameLookupRetryAfter {
		wa.inboxGroupNameMu.Unlock()
		return
	}
	wa.inboxGroupNameFetching[key] = struct{}{}
	wa.inboxGroupNameMu.Unlock()

	go wa.fetchAndPushInboxGroupName(chatJID)
}

// fetchAndPushInboxGroupName 按最终方案补群名：内存 miss → Router DB → 仅无行时 GetGroupInfo；失败写空名终态。
func (wa *WhatsAppClient) fetchAndPushInboxGroupName(chatJID types.JID) {
	key := chatJID.String()
	defer func() {
		wa.inboxGroupNameMu.Lock()
		delete(wa.inboxGroupNameFetching, key)
		wa.inboxGroupNameMu.Unlock()
	}()

	if wa.Main == nil || wa.UserLogin == nil {
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}
	push := wa.Main.inboxPush
	if push == nil {
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lookup, err := push.LookupChatName(ctx, string(wa.UserLogin.ID), chatJID.String())
	if err != nil {
		wa.UserLogin.Log.Warn().Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to lookup inbox group name from router; skip WhatsApp fetch this round")
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}
	if lookup.Found {
		// DB 已有记录（含空名终态）时只回填内存，禁止再打 WhatsApp。
		wa.rememberInboxGroupName(chatJID, lookup.GroupName)
		return
	}

	if wa.Client == nil {
		// 客户端未就绪不算终态，短冷却后由后续消息再触发。
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}

	info, err := wa.Client.GetGroupInfo(ctx, chatJID)
	if err != nil || info == nil {
		wa.UserLogin.Log.Warn().Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to GetGroupInfo for inbox group name backfill; write empty terminal")
		wa.persistInboxGroupNameTerminal(ctx, chatJID)
		return
	}
	name := strings.TrimSpace(info.Name)
	if name == "" {
		// 社群默认公告组等可能返回空名；按终态落库，永不自动重试。
		wa.UserLogin.Log.Debug().Stringer("chat_jid", chatJID).
			Msg("GetGroupInfo returned empty name; write empty terminal")
		wa.persistInboxGroupNameTerminal(ctx, chatJID)
		return
	}
	wa.handleWAGroupNameInbox(ctx, chatJID, name)
}

// persistInboxGroupNameTerminal 将空名终态写入 Router，并缓存到进程内。
func (wa *WhatsAppClient) persistInboxGroupNameTerminal(ctx context.Context, chatJID types.JID) {
	wa.rememberInboxGroupName(chatJID, "")
	push := wa.Main.inboxPush
	if push == nil || wa.UserLogin == nil {
		return
	}
	if err := push.PushChatName(ctx, inboxChatNameEvent{
		LoginID:   string(wa.UserLogin.ID),
		ChatID:    chatJID.String(),
		GroupName: "",
	}); err != nil {
		// 保持本地空名终态，至少在本次进程生命周期内不重复 GetGroupInfo。
		wa.UserLogin.Log.Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to push empty group name terminal to inbox")
	}
}

// markInboxGroupNameLookupTempFail 记录 Router/客户端瞬时失败时间，进入短冷却。
func (wa *WhatsAppClient) markInboxGroupNameLookupTempFail(chatJID types.JID) {
	if chatJID.IsEmpty() {
		return
	}
	key := chatJID.String()
	wa.inboxGroupNameMu.Lock()
	defer wa.inboxGroupNameMu.Unlock()
	if wa.inboxGroupNameTempFailAt == nil {
		wa.inboxGroupNameTempFailAt = make(map[string]time.Time)
	}
	wa.inboxGroupNameTempFailAt[key] = time.Now()
}
