package connector

import (
	"os"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/status"
)

func TestOwnRemoteDisplayName(t *testing.T) {
	if got := ownRemoteDisplayName("Biz", "Push"); got != "Biz" {
		t.Fatalf("business preferred: %q", got)
	}
	if got := ownRemoteDisplayName("  ", "Push"); got != "Push" {
		t.Fatalf("fallback push: %q", got)
	}
	if got := ownRemoteDisplayName("", "  Push  "); got != "Push" {
		t.Fatalf("trim push: %q", got)
	}
	if got := ownRemoteDisplayName(" ", "\t"); got != "" {
		t.Fatalf("empty: %q", got)
	}
}

func TestApplyInboxRemoteProfileName(t *testing.T) {
	profile := &status.RemoteProfile{Phone: "+8615814500799"}
	if !applyInboxRemoteProfileName(profile, "Alice") {
		t.Fatal("expected update")
	}
	if profile.Name != "Alice" || profile.Phone != "+8615814500799" {
		t.Fatalf("unexpected profile: %+v", profile)
	}
	if applyInboxRemoteProfileName(profile, "Alice") {
		t.Fatal("same name should not update")
	}
	if applyInboxRemoteProfileName(profile, "  ") {
		t.Fatal("blank name should not update")
	}
	if applyInboxRemoteProfileName(nil, "Bob") {
		t.Fatal("nil profile should not update")
	}
}

func TestInboxRemoteProfileSyncCallSites(t *testing.T) {
	clientSrc, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	handleSrc, err := os.ReadFile("handlewhatsapp.go")
	if err != nil {
		t.Fatal(err)
	}
	clientBody := string(clientSrc)
	handleBody := string(handleSrc)

	if strings.Contains(clientBody, "Inbox mode: refuse syncRemoteProfile") {
		t.Fatal("inbox must not refuse syncRemoteProfile anymore")
	}
	if !strings.Contains(clientBody, "Inbox mode: remote profile name updated") {
		t.Fatal("inbox name-only update path missing")
	}
	if !strings.Contains(clientBody, "applyInboxRemoteProfileName(&wa.UserLogin.RemoteProfile, name)") {
		t.Fatal("inbox path must use applyInboxRemoteProfileName")
	}

	// Connected / CriticalBlock 必须无条件触发 syncRemoteProfile。
	if strings.Contains(handleBody, "inbox 模式不维护 Matrix RemoteProfile/ghost") {
		t.Fatal("Connected still skips remote profile sync in inbox")
	}
	if strings.Contains(handleBody, "inbox 模式不维护 Matrix RemoteProfile，阻止 CriticalBlock") {
		t.Fatal("CriticalBlock still skips remote profile sync in inbox")
	}
	if !strings.Contains(handleBody, "go wa.syncRemoteProfile(ctx, nil)") {
		t.Fatal("Connected must call syncRemoteProfile")
	}
	if !strings.Contains(handleBody, "go wa.syncRemoteProfile(log.WithContext(context.Background()), nil)") {
		t.Fatal("CriticalBlock must call syncRemoteProfile")
	}

	// PushNameSetting：inbox 走 syncRemoteProfile，非 inbox 仍走 syncGhost。
	idx := strings.Index(handleBody, "case *events.PushNameSetting:")
	if idx < 0 {
		t.Fatal("PushNameSetting case missing")
	}
	snippet := handleBody[idx:]
	end := strings.Index(snippet, "case *events.Contact:")
	if end > 0 {
		snippet = snippet[:end]
	}
	if !strings.Contains(snippet, "go wa.syncRemoteProfile(ctx, nil)") || !strings.Contains(snippet, "go wa.syncGhost(wa.JID.ToNonAD(), \"push name setting\", nil)") {
		t.Fatalf("PushNameSetting must branch inbox syncRemoteProfile vs syncGhost:\n%s", snippet)
	}
}
