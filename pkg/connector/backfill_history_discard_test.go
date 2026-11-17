package connector

import (
	"os"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
)

func TestShouldDiscardHistorySyncNotification(t *testing.T) {
	discard := []waE2E.HistorySyncType{
		waE2E.HistorySyncType_INITIAL_BOOTSTRAP,
		waE2E.HistorySyncType_RECENT,
		waE2E.HistorySyncType_FULL,
		waE2E.HistorySyncType_ON_DEMAND,
	}
	keep := []waE2E.HistorySyncType{
		waE2E.HistorySyncType_PUSH_NAME,
		waE2E.HistorySyncType_INITIAL_STATUS_V3,
		waE2E.HistorySyncType_NON_BLOCKING_DATA,
		waE2E.HistorySyncType_NO_HISTORY,
		waE2E.HistorySyncType_MESSAGE_ACCESS_STATUS,
	}
	for _, syncType := range discard {
		if !shouldDiscardHistorySyncNotification(syncType) {
			t.Fatalf("应丢弃历史包类型 %v，但判断为保留", syncType)
		}
	}
	for _, syncType := range keep {
		if shouldDiscardHistorySyncNotification(syncType) {
			t.Fatalf("应保留历史包类型 %v，但判断为丢弃", syncType)
		}
	}
}


// TestHistoryDiscardGatesBeforeDownloadHistorySync 验收：inbox 启用时，
// save / download 两处均在 DownloadHistorySync 之前按 shouldDiscard 返回。
func TestHistoryDiscardGatesBeforeDownloadHistorySync(t *testing.T) {
	raw, err := os.ReadFile("backfill.go")
	if err != nil {
		t.Fatalf("读取 backfill.go 失败: %v", err)
	}
	src := string(raw)

	assertDiscardBeforeDownload := func(t *testing.T, funcName string) {
		t.Helper()
		marker := "func (wa *WhatsAppClient) " + funcName
		start := strings.Index(src, marker)
		if start < 0 {
			t.Fatalf("未找到 %s", funcName)
		}
		rest := src[start:]
		next := strings.Index(rest[len(marker):], "\nfunc (")
		body := rest
		if next >= 0 {
			body = rest[:len(marker)+next]
		}
		gate := strings.Index(body, "inboxEnabled()")
		discard := strings.Index(body, "shouldDiscardHistorySyncNotification")
		download := strings.Index(body, "DownloadHistorySync(")
		if gate < 0 || discard < 0 {
			t.Fatalf("%s 必须同时 gated by inboxEnabled 与 shouldDiscardHistorySyncNotification", funcName)
		}
		if download >= 0 && download < discard {
			t.Fatalf("%s 中 DownloadHistorySync( 调用不得出现在 discard 判断之前", funcName)
		}
		// save 路径没有 DownloadHistorySync 调用是预期；download 路径必须有且位于 return 之后。
		if funcName == "downloadAndSaveWAHistorySyncData" {
			if download < 0 {
				t.Fatalf("%s 应保留 DownloadHistorySync( 调用（非 discard 路径）", funcName)
			}
			retAfterDiscard := strings.Index(body[discard:], "return")
			if retAfterDiscard < 0 || discard+retAfterDiscard > download {
				t.Fatalf("%s discard 分支必须在 DownloadHistorySync 之前 return", funcName)
			}
		}
	}

	assertDiscardBeforeDownload(t, "saveWAHistorySyncNotification")
	assertDiscardBeforeDownload(t, "downloadAndSaveWAHistorySyncData")
}

