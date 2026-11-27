package database

import (
	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func init() {
	// PostgreSQL 设备查询使用数组条件，独立入口也需要注册 lib/pq 的数组编码。
	sqlstore.PostgresArrayWrapper = pq.Array
}

// NewDeviceStore 供 WhatsMeow 读写已由统一 migration 创建的协议表。
func NewDeviceStore(db *dbutil.Database, log zerolog.Logger) *sqlstore.Container {
	log = log.With().Str("db_section", "whatsmeow").Logger()
	return sqlstore.NewWithWrappedDB(db, waLog.Zerolog(log))
}
