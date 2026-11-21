package connector

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestInboxGroupNameCacheAndFetchGate(t *testing.T) {
	jid := types.NewJID("120363432741761595", types.GroupServer)

	wa := &WhatsAppClient{}
	wa.rememberInboxGroupName(jid, "测试")
	if got := wa.getInboxGroupName(jid); got != "测试" {
		t.Fatalf("getInboxGroupName=%q, want 测试", got)
	}

	// 已有缓存时不应进入 fetching。
	wa.ensureInboxGroupNameAsync(jid)
	wa.inboxGroupNameMu.Lock()
	_, fetching := wa.inboxGroupNameFetching[jid.String()]
	wa.inboxGroupNameMu.Unlock()
	if fetching {
		t.Fatal("cached name must not start fetch")
	}

	wa2 := &WhatsAppClient{}
	wa2.markInboxGroupNameFailed(jid)
	wa2.ensureInboxGroupNameAsync(jid)
	wa2.inboxGroupNameMu.Lock()
	_, fetching = wa2.inboxGroupNameFetching[jid.String()]
	wa2.inboxGroupNameMu.Unlock()
	if fetching {
		t.Fatal("cooldown must prevent fetch")
	}

	// 冷却过期后应标记 fetching（Client 为空时后台会立刻失败并清掉 fetching）。
	wa2.inboxGroupNameMu.Lock()
	wa2.inboxGroupNameFailedAt[jid.String()] = time.Now().Add(-inboxGroupNameRetryAfter - time.Second)
	wa2.inboxGroupNameMu.Unlock()
	wa2.ensureInboxGroupNameAsync(jid)
	wa2.inboxGroupNameMu.Lock()
	_, fetching = wa2.inboxGroupNameFetching[jid.String()]
	wa2.inboxGroupNameMu.Unlock()
	if !fetching {
		// 后台 goroutine 可能已跑完；至少失败冷却应被刷新。
		wa2.inboxGroupNameMu.Lock()
		failedAt, ok := wa2.inboxGroupNameFailedAt[jid.String()]
		wa2.inboxGroupNameMu.Unlock()
		if !ok || time.Since(failedAt) > time.Second {
			t.Fatal("expected fetching mark or fresh failure timestamp")
		}
	}
}
