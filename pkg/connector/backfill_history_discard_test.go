package connector

import (
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
