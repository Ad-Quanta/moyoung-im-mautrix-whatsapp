package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

const waVersionKey = "whatsapp_web_version"

// prepareWhatsAppVersion 在会话并发启动前更新 WhatsMeow 的进程级协议版本。
// 查询失败时沿用旧库缓存或 WhatsMeow 内置版本，服务仍可启动。
func prepareWhatsAppVersion(ctx context.Context, db *dbutil.Database, log zerolog.Logger) {
	var cached string
	switch err := db.QueryRow(ctx, `SELECT value FROM kv_store WHERE bridge_id='' AND key=$1`, waVersionKey).Scan(&cached); {
	case err == nil:
		version, err := store.ParseVersion(cached)
		if err == nil {
			store.SetWAVersion(version)
		} else {
			log.Warn().Err(err).Msg("无法解析缓存的 WhatsApp 协议版本")
		}
	case !errors.Is(err, sql.ErrNoRows):
		log.Warn().Err(err).Msg("无法读取缓存的 WhatsApp 协议版本")
	}
	version, err := whatsmeow.GetLatestVersion(ctx, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		log.Warn().Err(err).Msg("无法刷新 WhatsApp 协议版本，使用当前版本")
		return
	}
	store.SetWAVersion(*version)
	_, err = db.Exec(ctx, `INSERT INTO kv_store (bridge_id,key,value) VALUES ('',$1,$2)
        ON CONFLICT (bridge_id,key) DO UPDATE SET value=excluded.value`, waVersionKey, version.String())
	if err != nil {
		log.Warn().Err(err).Msg("无法缓存 WhatsApp 协议版本")
	}
}
