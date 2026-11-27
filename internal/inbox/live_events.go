package inbox

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// inboxGroupNameLookupRetryAfter 用于 Router 查询瞬时失败后的本地冷却。
const inboxGroupNameLookupRetryAfter = 10 * time.Minute

// CallEventMaxAge 限制实时通话通知的有效时长。
const CallEventMaxAge = 15 * time.Minute

// LiveEvents 为单个 WhatsApp 登录推送实时事件，并维护该登录的群名缓存。
type LiveEvents struct {
	ctx     context.Context
	loginID string
	log     zerolog.Logger
	client  *whatsmeow.Client
	push    *PushClient
	options LiveEventOptions

	groupMu         sync.Mutex
	groupNames      map[string]string
	groupFetching   map[string]struct{}
	groupTempFailAt map[string]time.Time
}

// LiveEventOptions 保留 Inbox 对状态广播和频道消息的过滤规则。
type LiveEventOptions struct {
	EnableStatusBroadcast bool
	DisableNewsletter     bool
}

// NewLiveEvents 绑定单个登录。异步群名查询使用调用方提供的运行时上下文。
func NewLiveEvents(ctx context.Context, loginID string, log zerolog.Logger, client *whatsmeow.Client, push *PushClient, options LiveEventOptions) *LiveEvents {
	return &LiveEvents{ctx: ctx, loginID: loginID, log: log, client: client, push: push, options: options}
}

// Handle 接收 WhatsMeow 原生事件，分别返回是否由 Inbox 消费和是否确认处理成功。
func (wa *LiveEvents) Handle(ctx context.Context, raw any) (handled, success bool) {
	switch evt := raw.(type) {
	case *events.Message:
		return true, wa.handleMessage(ctx, evt)
	case *events.CallOffer:
		return true, wa.handleCall(ctx, evt.GroupJID, evt.CallCreator, evt.CallCreatorAlt, evt.CallID, "", evt.Timestamp)
	case *events.CallOfferNotice:
		return true, wa.handleCall(ctx, evt.GroupJID, evt.CallCreator, evt.CallCreatorAlt, evt.CallID, evt.Type, evt.Timestamp)
	case *events.GroupInfo:
		if evt.Name != nil {
			return true, wa.GroupName(ctx, evt.JID, evt.Name.Name)
		}
		return true, true
	case *events.JoinedGroup:
		return true, wa.GroupName(ctx, evt.JID, evt.Name)
	default:
		return false, true
	}
}

// handleMessage 沿用实时消息的地址解析及过滤规则，然后进入 Router 推送。
func (wa *LiveEvents) handleMessage(ctx context.Context, evt *events.Message) bool {
	if (evt.Info.Chat == types.StatusBroadcastJID && !wa.options.EnableStatusBroadcast) ||
		(evt.Info.Chat.Server == types.NewsletterServer && wa.options.DisableNewsletter) ||
		(evt.Info.Chat.Server == types.BroadcastServer && evt.Info.IsFromMe) {
		return true
	}
	if !FillAltJIDs(ctx, wa.client.Store.LIDs, &evt.Info.MessageSource) {
		return false
	}
	parsedType, ok := DecryptMessage(ctx, wa.client, wa.log, evt)
	if !ok || parsedType == "ignore" || strings.HasPrefix(parsedType, "unknown_protocol_") {
		return true
	}
	if evt.Info.Chat == types.MuseJID && evt.Message.GetProtocolMessage().GetType() == waE2E.ProtocolMessage_AI_METADATA_OPERATION {
		return true
	}
	parsedType, _, ok = NormalizeAssociation(wa.log, evt, parsedType)
	if !ok {
		return true
	}
	return wa.Message(ctx, evt, parsedType)
}

// FillAltJIDs 从设备存储补齐解密所需的备用地址。
func FillAltJIDs(ctx context.Context, lids store.LIDStore, info *types.MessageSource) bool {
	var err error
	if info.Sender.Server == types.DefaultUserServer && info.SenderAlt.IsEmpty() {
		info.SenderAlt, err = lids.GetLIDForPN(ctx, info.Sender)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Stringer("sender", info.Sender).Msg("Failed to get LID for sender")
			return false
		}
	}
	if info.Chat.Server == types.DefaultUserServer && info.IsFromMe && info.RecipientAlt.IsEmpty() {
		info.RecipientAlt, err = lids.GetLIDForPN(ctx, info.Chat)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Stringer("chat", info.Chat).Msg("Failed to get LID for chat")
			return false
		}
	}
	return true
}

// handleCall 将联系人号码映射为 WhatsApp LID 后推送实时通话。
func (wa *LiveEvents) handleCall(ctx context.Context, group, sender, senderAlt types.JID, id, callType string, ts time.Time) bool {
	if sender.Server == types.DefaultUserServer && senderAlt.IsEmpty() {
		senderAlt, _ = wa.client.Store.LIDs.GetLIDForPN(ctx, sender)
	}
	if sender.Server == types.DefaultUserServer && senderAlt.Server == types.HiddenUserServer {
		sender = senderAlt
	}
	return wa.Call(ctx, group, sender, id, callType, ts)
}

// Message 收件箱模式：实时消息推 im-router，不进 Matrix portal。
func (wa *LiveEvents) Message(ctx context.Context, evt *events.Message, parsedMessageType string) bool {
	extracted := ExtractMessage(evt.Message, parsedMessageType)
	if extracted.Skip {
		return true
	}
	push := wa.push
	if push == nil {
		wa.log.Error().Msg("inbox mode enabled but push client is nil; dropping message (no Dendrite fallback)")
		return true
	}
	payload := MessageEvent{
		LoginID:    wa.loginID,
		ChatID:     evt.Info.Chat.String(),
		ExternalID: evt.Info.ID,
		Direction:  Direction(evt.Info),
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
		payload.Media = &MessageMedia{
			MimeType:         extracted.MimeType,
			Size:             extracted.Size,
			Width:            extracted.Width,
			Height:           extracted.Height,
			DurationMs:       extracted.DurationMs,
			Filename:         extracted.Filename,
			DirectPath:       extracted.DirectPath,
			MediaKeyB64:      EncodeMediaKey(extracted.MediaKey),
			FileSHA256B64:    EncodeMediaKey(extracted.FileSHA256),
			FileEncSHA256B64: EncodeMediaKey(extracted.FileEncSHA256),
			MediaType:        extracted.MediaType,
		}
	}
	if err := push.PushMessage(ctx, payload); err != nil {
		wa.log.Err(err).
			Str("message_id", evt.Info.ID).
			Stringer("chat_jid", evt.Info.Chat).
			Msg("Failed to push WhatsApp message to inbox")
	}
	return true
}

// Call 收件箱模式保存通话事件为 call。
func (wa *LiveEvents) Call(ctx context.Context, group, sender types.JID, id, callType string, ts time.Time) bool {
	if time.Since(ts) > CallEventMaxAge {
		return true
	}
	push := wa.push
	if push == nil {
		wa.log.Error().Msg("inbox mode enabled but push client is nil; dropping call event")
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
	payload := MessageEvent{
		LoginID:    wa.loginID,
		ChatID:     chat.String(),
		ExternalID: id,
		Direction:  "in",
		SenderID:   sender.String(),
		MsgKind:    "call",
		Body:       body,
		SentAt:     ts.UTC().Format(time.RFC3339Nano),
	}
	if err := push.PushMessage(ctx, payload); err != nil {
		wa.log.Err(err).Str("call_id", id).Msg("Failed to push call event to inbox")
	}
	return true
}

// GroupName 推送正式群名，并写入进程内缓存；不做全量群列表拉取。
func (wa *LiveEvents) GroupName(ctx context.Context, chatJID types.JID, groupName string) bool {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" || chatJID.IsEmpty() {
		return true
	}
	wa.rememberInboxGroupName(chatJID, groupName)
	push := wa.push
	if push == nil {
		return true
	}
	if err := push.PushChatName(ctx, ChatNameEvent{
		LoginID:   wa.loginID,
		ChatID:    chatJID.String(),
		GroupName: groupName,
	}); err != nil {
		wa.log.Err(err).Stringer("chat_jid", chatJID).Msg("Failed to push group name to inbox")
	}
	return true
}

// rememberInboxGroupName 记录已解析群名；空串表示空名终态，后续不再自动 GetGroupInfo。
func (wa *LiveEvents) rememberInboxGroupName(chatJID types.JID, groupName string) {
	if chatJID.IsEmpty() {
		return
	}
	groupName = strings.TrimSpace(groupName)
	key := chatJID.String()
	wa.groupMu.Lock()
	defer wa.groupMu.Unlock()
	if wa.groupNames == nil {
		wa.groupNames = make(map[string]string)
	}
	wa.groupNames[key] = groupName
	if wa.groupTempFailAt != nil {
		delete(wa.groupTempFailAt, key)
	}
}

// getInboxGroupName 读取进程内群名解析结果；found=false 表示尚未解析。
func (wa *LiveEvents) getInboxGroupName(chatJID types.JID) (name string, found bool) {
	if chatJID.IsEmpty() {
		return "", false
	}
	wa.groupMu.Lock()
	defer wa.groupMu.Unlock()
	if wa.groupNames == nil {
		return "", false
	}
	name, found = wa.groupNames[chatJID.String()]
	return name, found
}

// ensureInboxGroupNameAsync 仅在内存未解析时异步补齐：先查 Router DB，无记录再 GetGroupInfo 一次。
func (wa *LiveEvents) ensureInboxGroupNameAsync(chatJID types.JID) {
	if chatJID.IsEmpty() || chatJID.Server != types.GroupServer {
		return
	}
	key := chatJID.String()
	wa.groupMu.Lock()
	if wa.groupNames == nil {
		wa.groupNames = make(map[string]string)
	}
	if wa.groupFetching == nil {
		wa.groupFetching = make(map[string]struct{})
	}
	if wa.groupTempFailAt == nil {
		wa.groupTempFailAt = make(map[string]time.Time)
	}
	if _, ok := wa.groupNames[key]; ok {
		wa.groupMu.Unlock()
		return
	}
	if _, ok := wa.groupFetching[key]; ok {
		wa.groupMu.Unlock()
		return
	}
	if failedAt, ok := wa.groupTempFailAt[key]; ok && time.Since(failedAt) < inboxGroupNameLookupRetryAfter {
		wa.groupMu.Unlock()
		return
	}
	wa.groupFetching[key] = struct{}{}
	wa.groupMu.Unlock()

	go wa.fetchAndPushInboxGroupName(chatJID)
}

// fetchAndPushInboxGroupName 按最终方案补群名：内存 miss → Router DB → 仅无行时 GetGroupInfo；失败写空名终态。
func (wa *LiveEvents) fetchAndPushInboxGroupName(chatJID types.JID) {
	key := chatJID.String()
	defer func() {
		wa.groupMu.Lock()
		delete(wa.groupFetching, key)
		wa.groupMu.Unlock()
	}()

	push := wa.push
	if push == nil {
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}

	ctx, cancel := context.WithTimeout(wa.ctx, 30*time.Second)
	defer cancel()

	lookup, err := push.LookupChatName(ctx, wa.loginID, chatJID.String())
	if err != nil {
		wa.log.Warn().Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to lookup inbox group name from router; skip WhatsApp fetch this round")
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}
	if lookup.Found {
		// DB 已有记录（含空名终态）时只回填内存，禁止再打 WhatsApp。
		wa.rememberInboxGroupName(chatJID, lookup.GroupName)
		return
	}

	if wa.client == nil {
		// 客户端未就绪不算终态，短冷却后由后续消息再触发。
		wa.markInboxGroupNameLookupTempFail(chatJID)
		return
	}

	info, err := wa.client.GetGroupInfo(ctx, chatJID)
	if err != nil || info == nil {
		wa.log.Warn().Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to GetGroupInfo for inbox group name backfill; write empty terminal")
		wa.persistInboxGroupNameTerminal(ctx, chatJID)
		return
	}
	name := strings.TrimSpace(info.Name)
	if name == "" {
		// 社群默认公告组等可能返回空名；按终态落库，永不自动重试。
		wa.log.Debug().Stringer("chat_jid", chatJID).
			Msg("GetGroupInfo returned empty name; write empty terminal")
		wa.persistInboxGroupNameTerminal(ctx, chatJID)
		return
	}
	wa.GroupName(ctx, chatJID, name)
}

// persistInboxGroupNameTerminal 将空名终态写入 Router，并缓存到进程内。
func (wa *LiveEvents) persistInboxGroupNameTerminal(ctx context.Context, chatJID types.JID) {
	wa.rememberInboxGroupName(chatJID, "")
	push := wa.push
	if push == nil {
		return
	}
	if err := push.PushChatName(ctx, ChatNameEvent{
		LoginID:   wa.loginID,
		ChatID:    chatJID.String(),
		GroupName: "",
	}); err != nil {
		// 保持本地空名终态，至少在本次进程生命周期内不重复 GetGroupInfo。
		wa.log.Err(err).Stringer("chat_jid", chatJID).
			Msg("Failed to push empty group name terminal to inbox")
	}
}

// markInboxGroupNameLookupTempFail 记录 Router/客户端瞬时失败时间，进入短冷却。
func (wa *LiveEvents) markInboxGroupNameLookupTempFail(chatJID types.JID) {
	if chatJID.IsEmpty() {
		return
	}
	key := chatJID.String()
	wa.groupMu.Lock()
	defer wa.groupMu.Unlock()
	if wa.groupTempFailAt == nil {
		wa.groupTempFailAt = make(map[string]time.Time)
	}
	wa.groupTempFailAt[key] = time.Now()
}
