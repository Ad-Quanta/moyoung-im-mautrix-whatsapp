package connector

import (
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
