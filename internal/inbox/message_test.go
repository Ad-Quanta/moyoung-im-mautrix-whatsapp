package inbox

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestExtractInboxMessage_SkipsUnsupportedKinds(t *testing.T) {
	cases := []string{
		"reaction",
		"reaction remove",
		"encrypted reaction",
		"edit",
		"revoke",
		"encrypted comment",
		"comment",
		"poll vote",
		"ignore",
		"unknown_protocol_99",
	}
	for _, parsedType := range cases {
		t.Run(parsedType, func(t *testing.T) {
			out := ExtractMessage(&waE2E.Message{Conversation: proto.String("x")}, parsedType)
			if !out.Skip {
				t.Fatalf("expected skip for parsedType=%q", parsedType)
			}
		})
	}
}

func TestExtractInboxMessage_TextAndImage(t *testing.T) {
	textOut := ExtractMessage(&waE2E.Message{Conversation: proto.String("hello")}, "text")
	if textOut.Skip || textOut.MsgKind != "text" || textOut.Body != "hello" {
		t.Fatalf("unexpected text extract: %+v", textOut)
	}

	extOut := ExtractMessage(&waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("ext")},
	}, "text")
	if extOut.Skip || extOut.MsgKind != "text" || extOut.Body != "ext" {
		t.Fatalf("unexpected extended text extract: %+v", extOut)
	}

	imgOut := ExtractMessage(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Caption:       proto.String("cap"),
		DirectPath:    proto.String("/path"),
		MediaKey:      []byte{1, 2, 3},
		FileSHA256:    []byte{4},
		FileEncSHA256: []byte{5},
		Mimetype:      proto.String("image/jpeg"),
		FileLength:    proto.Uint64(9),
		Width:         proto.Uint32(10),
		Height:        proto.Uint32(20),
	}}, "image")
	if imgOut.Skip || imgOut.MsgKind != "image" || imgOut.Body != "cap" || !imgOut.HasMedia {
		t.Fatalf("unexpected image extract: %+v", imgOut)
	}
	if imgOut.DirectPath != "/path" || imgOut.MimeType != "image/jpeg" || imgOut.Size != 9 || imgOut.Width != 10 || imgOut.Height != 20 {
		t.Fatalf("unexpected image media fields: %+v", imgOut)
	}
}

func TestExtractInboxMessage_NilMessageSkipped(t *testing.T) {
	out := ExtractMessage(nil, "text")
	if !out.Skip {
		t.Fatalf("nil message should skip")
	}
}
