package connector

import (
	"fmt"
	"regexp"
	"strings"

	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/id"
)

// 收件箱登录键前缀。后面必须是绑定 UUID，不能带 Matrix 的 @ 和域名。
const inboxProvisionUserPrefix = "wa-bind:"

var (
	inboxProvisionUserPattern = regexp.MustCompile(`^wa-bind:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	// 阶段 1、2 已写入的合成 MXID。继续接受，避免已登录会话要重新扫码；不会拿它去 Dendrite 建号。
	legacyProvisionUserPattern = regexp.MustCompile(`^@[A-Za-z0-9._=\-+/]{1,200}:[A-Za-z0-9.\-]{1,200}$`)
)

// NormalizeProvisionUserID 校验 provision 的 user_id。
// 新登录使用 wa-bind:<uuid>；旧登录仍是 @binding_...:domain。两者都只存在于桥数据库。
func NormalizeProvisionUserID(raw string) (id.UserID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("provision user_id 不能为空")
	}
	if len(raw) > 255 || strings.ContainsAny(raw, " \t\r\n") {
		return "", fmt.Errorf("provision user_id 无效")
	}
	lower := strings.ToLower(raw)
	if inboxProvisionUserPattern.MatchString(lower) {
		return id.UserID(lower), nil
	}
	if legacyProvisionUserPattern.MatchString(raw) {
		return id.UserID(raw), nil
	}
	return "", fmt.Errorf("provision user_id 不是收件箱绑定键")
}

// installInboxProvisionIdentity 让 provision 中间件先认出绑定键，再进入 whoami、登录和收件箱发送。
func (wa *WhatsAppConnector) installInboxProvisionIdentity() error {
	if wa == nil || wa.Bridge == nil || wa.Bridge.Matrix == nil {
		return fmt.Errorf("matrix connector is not ready")
	}
	mx, ok := wa.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.Provisioning == nil {
		return fmt.Errorf("matrix connector does not expose provisioning API")
	}
	mx.Provisioning.NormalizeUserID = func(raw id.UserID) (id.UserID, error) {
		return NormalizeProvisionUserID(string(raw))
	}
	return nil
}
