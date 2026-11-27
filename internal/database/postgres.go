package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"gopkg.in/yaml.v3"
)

// Open 创建并连接配置指定的 schema，连接信息统一从 DB_DSN 读取。
func Open(ctx context.Context, path string, cfg dbutil.Config, log zerolog.Logger) (*dbutil.Database, error) {
	schema, err := configuredDatabaseSchema(path)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(strings.TrimSpace(os.Getenv("DB_DSN")))
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || strings.Trim(u.Path, "/") == "" {
		return nil, fmt.Errorf("DB_DSN 必须是包含数据库名的 PostgreSQL URL")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("DB_DSN 查询参数无效")
	}
	if q.Has("search_path") {
		return nil, fmt.Errorf("DB_DSN 不能包含 search_path，schema 必须写在 database.schema")
	}
	if err = ensureDatabaseSchema(ctx, u.String(), schema); err != nil {
		return nil, err
	}
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	cfg.Type, cfg.URI = "postgres", u.String()
	cfg.ReadOnlyPool.Type, cfg.ReadOnlyPool.URI = cfg.Type, cfg.URI
	return dbutil.NewFromConfig("", cfg, dbutil.ZeroLogger(log.With().Str("db_section", "main").Logger()))
}

// databaseSchemaFile 只读取配置中的 schema。dbutil 不认识该字段，不能从已解析结构取。
type databaseSchemaFile struct {
	Database struct {
		Schema string `yaml:"schema"`
	} `yaml:"database"`
}

func configuredDatabaseSchema(configPath string) (string, error) {
	if strings.TrimSpace(configPath) == "" {
		return "", fmt.Errorf("配置文件路径为空，无法读取 database.schema")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("读取配置中的 database.schema: %w", err)
	}
	var cfg databaseSchemaFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("解析 database.schema: %w", err)
	}
	schema := strings.TrimSpace(cfg.Database.Schema)
	if override := strings.TrimSpace(os.Getenv("WHATSAPP_BRIDGE_SCHEMA")); override != "" {
		schema = override
	}
	if err := validateDatabaseSchema(schema); err != nil {
		return "", fmt.Errorf("database.schema: %w", err)
	}
	return schema, nil
}

var databaseSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func validateDatabaseSchema(schema string) error {
	if !databaseSchemaPattern.MatchString(schema) {
		return fmt.Errorf("名称无效")
	}
	lower := strings.ToLower(schema)
	if lower == "information_schema" || lower == "pg_catalog" || strings.HasPrefix(lower, "pg_") {
		return fmt.Errorf("不能使用系统 schema")
	}
	return nil
}

// ensureDatabaseSchema 在桥自己的 migration 建表前创建配置指定的 schema。
func ensureDatabaseSchema(ctx context.Context, dsn, schema string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("连接 PostgreSQL 失败: %w", err)
	}
	defer tx.Rollback()
	// 首次启动可能有多个 WhatsApp 进程同时创建同名 schema。
	if _, err := tx.ExecContext(ctx, lockInboxSchema, schema); err != nil {
		return fmt.Errorf("锁定 WhatsApp schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "`+schema+`"`); err != nil {
		return fmt.Errorf("创建 schema %s: %w", schema, err)
	}
	return tx.Commit()
}
