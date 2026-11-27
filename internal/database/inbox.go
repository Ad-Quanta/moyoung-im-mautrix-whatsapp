package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"go.mau.fi/util/dbutil"
)

//go:embed migrations/000001_init.up.sql
var initialSchema string

// 数组下标从零开始，迁移文件从 000001 开始。后续变更按顺序追加。
var inboxMigrations = [...]string{initialSchema}

// 在同一数据库内按 schema 串行创建和迁移，互不阻塞其他实例的独立 schema。
const lockInboxSchema = `SELECT pg_advisory_xact_lock(hashtextextended('whatsapp-inbox:' || $1::text, 0))`

// legacyOwner 用于确认导入的旧版本库归属。
const legacyOwner = "megabridge/mautrix-whatsapp"

// MigrateInbox 统一管理 WhatsApp 业务和协议表。事务锁保障并行启动和失败重试。
func MigrateInbox(ctx context.Context, db *dbutil.Database) error {
	if db.Dialect != dbutil.Postgres {
		return fmt.Errorf("WhatsApp migration 仅支持 PostgreSQL")
	}
	return db.DoTxn(ctx, &dbutil.TxnOptions{Isolation: sql.LevelReadCommitted}, func(ctx context.Context) error {
		var schema string
		if err := db.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
			return fmt.Errorf("读取 WhatsApp schema: %w", err)
		}
		if _, err := db.Exec(ctx, lockInboxSchema, schema); err != nil {
			return fmt.Errorf("锁定 WhatsApp migration: %w", err)
		}
		version, err := readInboxVersion(ctx, db, schema)
		if err != nil {
			return err
		}
		if version < 0 || version > len(inboxMigrations) {
			return fmt.Errorf("不支持的 WhatsApp 数据库版本 %d，当前支持至 %d", version, len(inboxMigrations))
		}
		for version < len(inboxMigrations) {
			if _, err := db.Exec(ctx, inboxMigrations[version]); err != nil {
				return fmt.Errorf("执行 WhatsApp migration %06d: %w", version+1, err)
			}
			version++
			if _, err := db.Exec(ctx, `UPDATE whatsapp_schema_migrations SET version=$1 WHERE singleton`, version); err != nil {
				return fmt.Errorf("记录 WhatsApp migration %06d: %w", version, err)
			}
		}
		return nil
	})
}

// readInboxVersion 识别统一版本表、可接管的旧库和新 schema，并初始化唯一版本记录。
func readInboxVersion(ctx context.Context, db *dbutil.Database, schema string) (int, error) {
	var unified, legacy, occupied bool
	err := db.QueryRow(ctx, `SELECT
		to_regclass('whatsapp_schema_migrations') IS NOT NULL,
		to_regclass('database_owner') IS NOT NULL OR to_regclass('version') IS NOT NULL
			OR to_regclass('whatsapp_version') IS NOT NULL OR to_regclass('whatsmeow_version') IS NOT NULL,
		EXISTS (SELECT FROM pg_depend d JOIN pg_namespace n ON d.refobjid = n.oid
			WHERE d.refclassid = 'pg_namespace'::regclass AND n.nspname = $1)`, schema).
		Scan(&unified, &legacy, &occupied)
	if err != nil {
		return 0, fmt.Errorf("识别 WhatsApp schema 状态: %w", err)
	}
	var version int
	switch {
	case unified && legacy:
		return 0, fmt.Errorf("WhatsApp schema 同时含统一和旧版本表")
	case unified:
		if err := db.QueryRow(ctx, `SELECT version FROM whatsapp_schema_migrations WHERE singleton`).Scan(&version); err != nil {
			return 0, fmt.Errorf("读取 WhatsApp migration 版本: %w", err)
		}
		if version < 1 {
			return 0, fmt.Errorf("无效的 WhatsApp migration 版本 %d", version)
		}
		return version, nil
	case legacy:
		if err := adoptLegacyInbox(ctx, db); err != nil {
			return 0, err
		}
		version = 1
	case occupied:
		return 0, fmt.Errorf("WhatsApp schema 已包含对象且没有受支持的版本记录")
	}
	if _, err := db.Exec(ctx, `CREATE TABLE whatsapp_schema_migrations (
		singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
		version INTEGER NOT NULL CHECK (version >= 0)
	)`); err != nil {
		return 0, fmt.Errorf("创建 WhatsApp migration 版本表: %w", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO whatsapp_schema_migrations (version) VALUES ($1)`, version); err != nil {
		return 0, fmt.Errorf("初始化 WhatsApp migration 版本: %w", err)
	}
	return version, nil
}

// adoptLegacyInbox 将完整的旧基线映射为 000001，仅替换迁移元数据，保留原有业务行与序列。
func adoptLegacyInbox(ctx context.Context, db *dbutil.Database) error {
	var owner string
	var bridge, connector, protocol int
	err := db.QueryRow(ctx, `SELECT
		(SELECT owner FROM database_owner WHERE key=0),
		(SELECT version FROM version),
		(SELECT version FROM whatsapp_version),
		(SELECT version FROM whatsmeow_version)`).Scan(&owner, &bridge, &connector, &protocol)
	if err != nil {
		return fmt.Errorf("读取旧 WhatsApp 版本记录: %w", err)
	}
	if owner != legacyOwner {
		return fmt.Errorf("旧 WhatsApp 库归属不匹配")
	}
	if bridge != 33 || connector != 12 || protocol != 16 {
		return fmt.Errorf("旧 WhatsApp 库版本为 %d/%d/%d，需先用旧程序升级到 33/12/16", bridge, connector, protocol)
	}
	// 通过实际查询确认独立服务依赖的登录、设备、联系人及注销表可用。
	if _, err := db.Exec(ctx, `SELECT 1 FROM user_login, whatsmeow_device, whatsmeow_contacts,
		whatsmeow_lid_map, whatsapp_account_cleanup LIMIT 0`); err != nil {
		return fmt.Errorf("旧 WhatsApp 库缺少基线所需的表: %w", err)
	}
	if _, err := db.Exec(ctx, `DROP TABLE version, whatsapp_version, whatsmeow_version, database_owner`); err != nil {
		return fmt.Errorf("替换旧 WhatsApp 版本表: %w", err)
	}
	return nil
}
