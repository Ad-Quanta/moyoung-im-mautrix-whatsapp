package inboxsession

import (
	"context"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// LoginID 返回与已有 user_login 记录兼容的 WhatsApp 手机号标识。
func LoginID(jid types.JID) string {
	if jid.Server != types.DefaultUserServer {
		return ""
	}
	return jid.User
}

// DeviceJID 使用旧登录键和会话元数据中的设备号构造 WhatsApp 设备地址。
func DeviceJID(loginID string, deviceID uint16) types.JID {
	if loginID == "" {
		return types.EmptyJID
	}
	return types.JID{Server: types.DefaultUserServer, User: loginID, Device: deviceID}
}

// RestoreDevice 从现有 WhatsMeow 存储读取已绑定设备。设备号为零表示已经解绑。
func RestoreDevice(ctx context.Context, loginID string, metadata *UserLoginMetadata, lookup func(context.Context, types.JID) (*store.Device, error)) (types.JID, *store.Device, error) {
	if metadata.WADeviceID == 0 {
		return types.EmptyJID, nil, nil
	}
	jid := DeviceJID(loginID, metadata.WADeviceID)
	device, err := lookup(ctx, jid)
	return jid, device, err
}
