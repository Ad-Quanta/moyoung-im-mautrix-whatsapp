package inbox

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestInboxGroupNameCacheAndFetchGate(t *testing.T) {
	jid := types.NewJID("120363432741761595", types.GroupServer)

	wa := &LiveEvents{}
	wa.rememberInboxGroupName(jid, "测试")
	if got, found := wa.getInboxGroupName(jid); !found || got != "测试" {
		t.Fatalf("getInboxGroupName=%q found=%v, want 测试/true", got, found)
	}

	// 已有正式名时不应进入 fetching。
	wa.ensureInboxGroupNameAsync(jid)
	wa.groupMu.Lock()
	_, fetching := wa.groupFetching[jid.String()]
	wa.groupMu.Unlock()
	if fetching {
		t.Fatal("cached name must not start fetch")
	}

	// 空名终态同样禁止自动补拉。
	waEmpty := &LiveEvents{}
	waEmpty.rememberInboxGroupName(jid, "")
	waEmpty.ensureInboxGroupNameAsync(jid)
	waEmpty.groupMu.Lock()
	_, fetching = waEmpty.groupFetching[jid.String()]
	waEmpty.groupMu.Unlock()
	if fetching {
		t.Fatal("empty terminal must not start fetch")
	}

	wa2 := &LiveEvents{}
	wa2.markInboxGroupNameLookupTempFail(jid)
	wa2.ensureInboxGroupNameAsync(jid)
	wa2.groupMu.Lock()
	_, fetching = wa2.groupFetching[jid.String()]
	wa2.groupMu.Unlock()
	if fetching {
		t.Fatal("lookup cooldown must prevent fetch")
	}

	// 冷却过期后应标记 fetching（无推送客户端 时后台会立刻 temp-fail 并清掉 fetching）。
	wa2.groupMu.Lock()
	wa2.groupTempFailAt[jid.String()] = time.Now().Add(-inboxGroupNameLookupRetryAfter - time.Second)
	wa2.groupMu.Unlock()
	wa2.ensureInboxGroupNameAsync(jid)
	wa2.groupMu.Lock()
	_, fetching = wa2.groupFetching[jid.String()]
	wa2.groupMu.Unlock()
	if !fetching {
		// 后台 goroutine 可能已跑完；至少应留下临时失败时间戳。
		wa2.groupMu.Lock()
		failedAt, ok := wa2.groupTempFailAt[jid.String()]
		wa2.groupMu.Unlock()
		if !ok || time.Since(failedAt) > time.Second {
			t.Fatal("expected fetching mark or fresh temp-fail timestamp")
		}
	}
}
