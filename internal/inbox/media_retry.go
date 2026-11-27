package inbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/wamedia"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const mediaRetryTimeout = 30 * time.Second

// MediaRetry 管理单个登录的在途手机重传请求，供下载 API 和 WhatsMeow 事件共用。
type MediaRetry struct {
	ctx      context.Context
	client   *whatsmeow.Client
	isOwnJID func(types.JID) bool
	mu       sync.Mutex
	pending  map[mediaRetryID]*mediaRetryRequest
}

type mediaRetryID struct {
	chat, sender types.JID
	messageID    types.MessageID
	fromMe       bool
}

type mediaRetryRequest struct {
	key  []byte
	done chan struct{}
	path string
	err  error
}

// NewMediaRetry 使用调用方的上下文关闭在途重传，账号历史 LID 由 isOwnJID 提供。
func NewMediaRetry(ctx context.Context, client *whatsmeow.Client, isOwnJID func(types.JID) bool) *MediaRetry {
	return &MediaRetry{ctx: ctx, client: client, isOwnJID: isOwnJID, pending: make(map[mediaRetryID]*mediaRetryRequest)}
}

// Wait 请求手机刷新媒体路径，同一条消息的并发下载共享一次请求。
func (m *MediaRetry) Wait(ctx context.Context, rawID string, keys *wamedia.DownloadInfo) error {
	parts := strings.SplitN(rawID, ":", 3)
	if len(parts) != 3 || parts[0] == "fake" || parts[2] == "" || strings.HasPrefix(parts[2], "FAKE::") {
		return errors.New("invalid media retry message ID")
	}
	chat, err := types.ParseJID(parts[0])
	if err != nil || chat.IsEmpty() {
		return errors.New("invalid media retry chat JID")
	}
	sender, err := types.ParseJID(parts[1])
	if err != nil || sender.IsEmpty() {
		return errors.New("invalid media retry sender JID")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	info := types.MessageInfo{ID: parts[2], MessageSource: types.MessageSource{
		Chat: chat, Sender: sender, IsFromMe: m.isOwnJID(sender), IsGroup: mediaRetryIsGroup(chat),
	}}
	id := makeMediaRetryID(chat, sender, info.ID, info.IsFromMe)
	m.mu.Lock()
	req := m.pending[id]
	if req == nil {
		req = &mediaRetryRequest{key: bytes.Clone(keys.Key), done: make(chan struct{})}
		m.pending[id] = req
		go m.request(id, req, &info)
	} else if !bytes.Equal(req.key, keys.Key) {
		m.mu.Unlock()
		return errors.New("media retry key does not match the pending request")
	}
	m.mu.Unlock()

	select {
	case <-req.done:
		if req.err != nil {
			return req.err
		}
		keys.DirectPath = req.path
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MediaRetry) request(id mediaRetryID, req *mediaRetryRequest, info *types.MessageInfo) {
	// 共享请求随会话或重传超时结束，单个 HTTP 请求取消时其他下载仍可继续等待。
	ctx, cancel := context.WithTimeout(m.ctx, mediaRetryTimeout)
	defer cancel()
	if err := m.client.SendMediaRetryReceipt(ctx, info, req.key); err != nil {
		m.finish(id, req, "", fmt.Errorf("failed to send media retry request: %w", err))
		return
	}
	select {
	case <-req.done:
	case <-ctx.Done():
		m.finish(id, req, "", fmt.Errorf("phone did not respond to media retry: %w", ctx.Err()))
	}
}

func (m *MediaRetry) finish(id mediaRetryID, req *mediaRetryRequest, path string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending[id] == req {
		delete(m.pending, id)
		req.path, req.err = path, err
		close(req.done)
	}
}

// Handle 匹配并解密手机回执。私聊回执没有 participant，使用会话地址和收发方向匹配。
func (m *MediaRetry) Handle(ctx context.Context, evt *events.MediaRetry) {
	id := makeMediaRetryID(evt.ChatID, evt.SenderID, evt.MessageID, evt.FromMe)
	req := m.lookup(id)
	if req == nil && !evt.ChatID.IsEmpty() {
		// 手机回执的地址可能使用该私聊的另一种标识。
		var alt types.JID
		switch evt.ChatID.Server {
		case types.DefaultUserServer:
			alt, _ = m.client.Store.LIDs.GetLIDForPN(ctx, evt.ChatID)
		case types.HiddenUserServer:
			alt, _ = m.client.Store.LIDs.GetPNForLID(ctx, evt.ChatID)
		}
		if !alt.IsEmpty() {
			id.chat = alt.ToNonAD()
			req = m.lookup(id)
		}
	}
	if req == nil {
		return
	}
	data, err := whatsmeow.DecryptMediaRetryNotification(evt, req.key)
	if err != nil {
		m.finish(id, req, "", fmt.Errorf("media retry notification: %w", err))
		return
	}
	if path := data.GetDirectPath(); path != "" {
		m.finish(id, req, path, nil)
		return
	}
	switch data.GetResult() {
	case waMmsRetry.MediaRetryNotification_NOT_FOUND:
		err = whatsmeow.ErrMediaNotAvailableOnPhone
	case waMmsRetry.MediaRetryNotification_DECRYPTION_ERROR:
		err = errors.New("phone reported a decryption error retrieving the media")
	default:
		err = fmt.Errorf("phone returned media retry result %s without a download path", data.GetResult())
	}
	m.finish(id, req, "", err)
}

func (m *MediaRetry) lookup(id mediaRetryID) *mediaRetryRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[id]
}

func makeMediaRetryID(chat, sender types.JID, id types.MessageID, fromMe bool) mediaRetryID {
	if !mediaRetryIsGroup(chat) {
		sender = types.EmptyJID
	}
	return mediaRetryID{chat: chat.ToNonAD(), sender: sender.ToNonAD(), messageID: id, fromMe: fromMe}
}

func mediaRetryIsGroup(chat types.JID) bool {
	return chat.Server != types.DefaultUserServer && chat.Server != types.HiddenUserServer && chat.Server != types.BotServer
}
