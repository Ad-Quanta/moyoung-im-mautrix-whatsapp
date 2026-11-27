// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package inbox

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// DecryptMessage 解析 WhatsApp 加密的反应、评论和消息封装，并返回原生消息类型。
func DecryptMessage(ctx context.Context, client *whatsmeow.Client, log zerolog.Logger, evt *events.Message) (string, bool) {
	parsedMessageType := MessageType(evt.Message)
	if encReact := evt.Message.GetEncReactionMessage(); encReact != nil {
		decrypted, err := client.DecryptReaction(ctx, evt)
		if err != nil {
			log.Err(err).Str("message_id", evt.Info.ID).Msg("Failed to decrypt reaction")
			return "", false
		}
		decrypted.Key = encReact.GetTargetMessageKey()
		evt.Message.ReactionMessage = decrypted
	}
	if encComment := evt.Message.GetEncCommentMessage(); encComment != nil {
		decrypted, err := client.DecryptComment(ctx, evt)
		if err != nil {
			log.Err(err).Str("message_id", evt.Info.ID).Msg("Failed to decrypt comment")
		} else {
			decrypted.EncCommentMessage = evt.Message.GetEncCommentMessage()
			evt.Message = decrypted
		}
	}
	if encMessage := evt.Message.GetSecretEncryptedMessage(); encMessage != nil {
		decrypted, err := client.DecryptSecretEncryptedMessage(ctx, evt)
		if err != nil {
			log.Err(err).
				Str("message_id", evt.Info.ID).
				Stringer("evt_sender", evt.Info.Sender).
				Any("target_message_key", encMessage.TargetMessageKey).
				Msg("Failed to decrypt secret-encrypted message")
			return "", false
		}
		evt.RawMessage = decrypted
		evt.UnwrapRaw()
		parsedMessageType = MessageType(evt.Message)
	}
	return parsedMessageType, true
}

// NormalizeAssociation 将高清补传归类为编辑，忽略独立的实况照片更新。
func NormalizeAssociation(log zerolog.Logger, evt *events.Message, parsedMessageType string) (messageType string, dontRenderEdited bool, ok bool) {
	messageAssoc := evt.Message.GetMessageContextInfo().GetMessageAssociation()
	if assocType := messageAssoc.GetAssociationType(); assocType == waE2E.MessageAssociation_HD_IMAGE_DUAL_UPLOAD || assocType == waE2E.MessageAssociation_HD_VIDEO_DUAL_UPLOAD {
		parentKey := messageAssoc.GetParentMessageKey()
		protocolMsg, shouldHideEdit := makeHDMediaReplacementEdit(evt.Message, parentKey)
		dontRenderEdited = shouldHideEdit
		log.Debug().
			Str("message_id", evt.Info.ID).
			Str("parent_id", parentKey.GetID()).
			Stringer("assoc_type", assocType).
			Msg("Received HD replacement message, converting to edit")

		evt.Message = &waE2E.Message{
			ProtocolMessage: protocolMsg,
		}
		parsedMessageType = MessageType(evt.Message)
	} else if assocType == waE2E.MessageAssociation_MOTION_PHOTO {
		log.Debug().
			Str("message_id", evt.Info.ID).
			Str("parent_id", messageAssoc.GetParentMessageKey().GetID()).
			Msg("Ignoring motion photo update")
		return parsedMessageType, false, false
	}

	return parsedMessageType, dontRenderEdited, true
}

func makeHDMediaReplacementEdit(message *waE2E.Message, parentKey *waCommon.MessageKey) (*waE2E.ProtocolMessage, bool) {
	protocolMsg := message.GetProtocolMessage()
	associatedMessage := message.GetAssociatedChildMessage().GetMessage()
	if protocolMsg.GetType() != waE2E.ProtocolMessage_MESSAGE_EDIT || protocolMsg.GetKey() == nil {
		protocolMsg = associatedMessage.GetProtocolMessage()
	}
	if protocolMsg.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT && protocolMsg.GetKey() != nil {
		if child := protocolMsg.GetEditedMessage().GetAssociatedChildMessage().GetMessage(); child != nil {
			protocolMsg.EditedMessage = child
		}
		protocolMsg.Key = parentKey
		return protocolMsg, false
	}
	return &waE2E.ProtocolMessage{
		Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
		Key:           parentKey,
		EditedMessage: associatedMessage,
	}, true
}

// MessageType 根据 WhatsApp 协议字段识别消息类型，供实时处理和旧连接器共用。
func MessageType(waMsg *waE2E.Message) string {
	switch {
	case waMsg == nil:
		return "ignore"
	case waMsg.Conversation != nil, waMsg.ExtendedTextMessage != nil:
		return "text"
	case waMsg.ImageMessage != nil:
		return fmt.Sprintf("image %s", waMsg.GetImageMessage().GetMimetype())
	case waMsg.StickerMessage != nil:
		return fmt.Sprintf("sticker %s", waMsg.GetStickerMessage().GetMimetype())
	case waMsg.VideoMessage != nil:
		return fmt.Sprintf("video %s", waMsg.GetVideoMessage().GetMimetype())
	case waMsg.PtvMessage != nil:
		return fmt.Sprintf("round video %s", waMsg.GetPtvMessage().GetMimetype())
	case waMsg.AudioMessage != nil:
		return fmt.Sprintf("audio %s", waMsg.GetAudioMessage().GetMimetype())
	case waMsg.DocumentMessage != nil:
		return fmt.Sprintf("document %s", waMsg.GetDocumentMessage().GetMimetype())
	case waMsg.ContactMessage != nil:
		return "contact"
	case waMsg.ContactsArrayMessage != nil:
		return "contact array"
	case waMsg.LocationMessage != nil:
		return "location"
	case waMsg.LiveLocationMessage != nil:
		return "live location start"
	case waMsg.GroupInviteMessage != nil:
		return "group invite"
	case waMsg.GroupMentionedMessage != nil:
		return "group mention"
	case waMsg.ScheduledCallCreationMessage != nil:
		return "scheduled call create"
	case waMsg.ScheduledCallEditMessage != nil:
		return "scheduled call edit"
	case waMsg.ReactionMessage != nil:
		if waMsg.ReactionMessage.GetText() == "" {
			return "reaction remove"
		}
		return "reaction"
	case waMsg.EncReactionMessage != nil:
		return "encrypted reaction"
	case waMsg.EncCommentMessage != nil:
		return "encrypted comment"
	case waMsg.CommentMessage != nil:
		return "comment"
	case waMsg.PollCreationMessage != nil || waMsg.PollCreationMessageV2 != nil || waMsg.PollCreationMessageV3 != nil ||
		waMsg.PollCreationMessageV5 != nil || waMsg.PollCreationMessageV6 != nil:
		return "poll create"
	case waMsg.PollCreationMessageV4 != nil:
		return "poll create (v4)"
	case waMsg.PollUpdateMessage != nil:
		return "poll update"
	case waMsg.ProtocolMessage != nil:
		switch waMsg.GetProtocolMessage().GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			if waMsg.GetProtocolMessage().GetKey() == nil {
				return "ignore"
			}
			return "revoke"
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			return "edit"
		case waE2E.ProtocolMessage_EPHEMERAL_SETTING:
			return "disappearing timer change"
		case waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE,
			waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION,
			waE2E.ProtocolMessage_INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC:
			return "ignore"
		default:
			return fmt.Sprintf("unknown_protocol_%d", waMsg.GetProtocolMessage().GetType())
		}
	case waMsg.ButtonsMessage != nil:
		return "buttons"
	case waMsg.ButtonsResponseMessage != nil:
		return "buttons response"
	case waMsg.TemplateMessage != nil:
		return "template"
	case waMsg.HighlyStructuredMessage != nil:
		return "highly structured template"
	case waMsg.TemplateButtonReplyMessage != nil:
		return "template button reply"
	case waMsg.InteractiveMessage != nil:
		return "interactive"
	case waMsg.InteractiveResponseMessage != nil:
		return "interactive response"
	case waMsg.ListMessage != nil:
		return "list"
	case waMsg.ProductMessage != nil:
		return "product"
	case waMsg.ListResponseMessage != nil:
		return "list response"
	case waMsg.OrderMessage != nil:
		return "order"
	case waMsg.InvoiceMessage != nil:
		return "invoice"
	case waMsg.BotInvokeMessage != nil:
		return "bot invoke"
	case waMsg.EventMessage != nil:
		return "event"
	case waMsg.EventCoverImage != nil:
		return "event cover image"
	case waMsg.EncEventResponseMessage != nil:
		return "ignore" // these are ignored for now as they're not meant to be shown as new messages
		//return "encrypted event response"
	case waMsg.NewsletterAdminInviteMessage != nil:
		return "newsletter admin invite"
	case waMsg.SecretEncryptedMessage != nil:
		return "secret encrypted"
	case waMsg.PollResultSnapshotMessage != nil:
		return "poll result snapshot"
	case waMsg.MessageHistoryNotice != nil:
		return "message history notice"
	case waMsg.RequestPhoneNumberMessage != nil:
		return "request phone number"
	case waMsg.KeepInChatMessage != nil:
		return "keep in chat"
	case waMsg.StatusMentionMessage != nil:
		return "status mention"
	case waMsg.StickerPackMessage != nil:
		return "sticker pack"
	case waMsg.AlbumMessage != nil:
		if waMsg.GetAlbumMessage().GetContextInfo().GetStanzaID() == "" {
			return "ignore"
		}
		return "album"
	case waMsg.SendPaymentMessage != nil, waMsg.RequestPaymentMessage != nil,
		waMsg.DeclinePaymentRequestMessage != nil, waMsg.CancelPaymentRequestMessage != nil,
		waMsg.PaymentInviteMessage != nil:
		return "payment"
	case waMsg.Call != nil:
		return "call"
	case waMsg.Chat != nil:
		return "chat"
	case waMsg.PlaceholderMessage != nil:
		return "placeholder"
	case waMsg.SenderKeyDistributionMessage != nil, waMsg.StickerSyncRmrMessage != nil, waMsg.MessageHistoryBundle != nil:
		return "ignore"
	default:
		return "unknown"
	}
}
