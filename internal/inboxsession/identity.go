package inboxsession

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	inboxProvisionUserPattern = regexp.MustCompile(`^wa-bind:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	// 兼容迁移前已落库的合成用户键，保留大小写以查回原会话。
	legacyProvisionUserPattern = regexp.MustCompile(`^@[A-Za-z0-9._=\-+/]{1,200}:[A-Za-z0-9.\-]{1,200}$`)
)

// NormalizeBindingKey 校验 provision 的 user_id。
// 新登录使用 wa-bind:<uuid>；旧登录仍是 @binding_...:domain。两者都只存在于桥数据库。
func NormalizeBindingKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("provision user_id 不能为空")
	}
	if len(raw) > 255 || strings.ContainsAny(raw, " \t\r\n") {
		return "", fmt.Errorf("provision user_id 无效")
	}
	lower := strings.ToLower(raw)
	if inboxProvisionUserPattern.MatchString(lower) {
		return lower, nil
	}
	if legacyProvisionUserPattern.MatchString(raw) {
		return raw, nil
	}
	return "", fmt.Errorf("provision user_id 不是收件箱绑定键")
}
