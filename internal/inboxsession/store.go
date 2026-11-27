// Copyright (c) 2024 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inboxsession

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"go.mau.fi/util/dbutil"
)

// Store 保存 WhatsApp 登录记录，使用服务配置选定的 schema 和既有 user_login 表。
// login ID 与绑定键均为字符串，调用者可独立解码旧版 profile 和 metadata。
type Store struct {
	db       *dbutil.Database
	bridgeID string
	rows     *dbutil.QueryHelper[*LoginRecord]
}

// LoginRecord 保留已有数据列与 JSON 内容，兼容现有设备和绑定记录。
type LoginRecord struct {
	BridgeID      string
	Owner         string
	ID            string
	RemoteName    string
	RemoteProfile json.RawMessage
	SpaceRoom     sql.NullString
	Metadata      json.RawMessage
}

func NewStore(db *dbutil.Database, bridgeID string) *Store {
	return &Store{db: db, bridgeID: bridgeID, rows: dbutil.MakeQueryHelperReflect[*LoginRecord](db)}
}

const (
	getUserLoginBaseQuery = `
		SELECT bridge_id, user_mxid, id, remote_name, remote_profile, space_room, metadata FROM user_login
	`
	getLoginByIDQuery          = getUserLoginBaseQuery + `WHERE bridge_id=$1 AND id=$2`
	getAllUsersWithLoginsQuery = `SELECT DISTINCT user_mxid FROM user_login WHERE bridge_id=$1`
	getAllLoginsForUserQuery   = getUserLoginBaseQuery + `WHERE bridge_id=$1 AND user_mxid=$2`
	insertUserLoginQuery       = `
		INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, remote_profile, space_room, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	updateUserLoginQuery = `
		UPDATE user_login SET remote_name=$4, remote_profile=$5, space_room=$6, metadata=$7
		WHERE bridge_id=$1 AND user_mxid=$2 AND id=$3
	`
	deleteUserLoginQuery = `
		DELETE FROM user_login WHERE bridge_id=$1 AND id=$2
	`
)

func (s *Store) GetByID(ctx context.Context, id string) (*LoginRecord, error) {
	return s.rows.QueryOne(ctx, getLoginByIDQuery, s.bridgeID, id)
}

func (s *Store) ListOwners(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, getAllUsersWithLoginsQuery, s.bridgeID)
	return dbutil.NewRowIterWithError(rows, dbutil.ScanSingleColumn[string], err).AsList()
}

func (s *Store) ListByOwner(ctx context.Context, owner string) ([]*LoginRecord, error) {
	return s.rows.QueryMany(ctx, getAllLoginsForUserQuery, s.bridgeID, owner)
}

func (s *Store) Insert(ctx context.Context, login *LoginRecord) error {
	return s.rows.Exec(ctx, insertUserLoginQuery, s.values(login)...)
}

func (s *Store) Update(ctx context.Context, login *LoginRecord) error {
	return s.rows.Exec(ctx, updateUserLoginQuery, s.values(login)...)
}

func (s *Store) Delete(ctx context.Context, id string) error {
	return s.rows.Exec(ctx, deleteUserLoginQuery, s.bridgeID, id)
}

// ErrLoginOwned 表示手机号已由另一绑定持有。
var ErrLoginOwned = errors.New("WhatsApp login belongs to another binding")

// ListAll 枚举当前数据库分区内可恢复的登录。
func (s *Store) ListAll(ctx context.Context) ([]*LoginRecord, error) {
	return s.rows.QueryMany(ctx, getUserLoginBaseQuery+`WHERE bridge_id=$1 ORDER BY id`, s.bridgeID)
}

// SavePaired 原子写入绑定行及配对会话，只允许原 owner 更新同一手机号的设备。
func (s *Store) SavePaired(ctx context.Context, login *LoginRecord) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		_, err := s.db.Exec(ctx, `INSERT INTO "user" (bridge_id, mxid) VALUES ($1, $2)
			ON CONFLICT (bridge_id, mxid) DO NOTHING`, s.bridgeID, login.Owner)
		if err != nil {
			return err
		}
		result, err := s.db.Exec(ctx, insertUserLoginQuery+`
			ON CONFLICT (bridge_id, id) DO UPDATE SET remote_name=excluded.remote_name,
				remote_profile=excluded.remote_profile, metadata=excluded.metadata
			WHERE user_login.user_mxid=excluded.user_mxid`, s.values(login)...)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err == nil && count == 0 {
			return ErrLoginOwned
		}
		return err
	})
}

// DeleteByOwner 仅删除属于指定绑定的登录。
func (s *Store) DeleteByOwner(ctx context.Context, owner, id string) error {
	return s.rows.Exec(ctx, deleteUserLoginQuery+` AND user_mxid=$3`, s.bridgeID, id, owner)
}

func (r *LoginRecord) Scan(row dbutil.Scannable) (*LoginRecord, error) {
	err := row.Scan(&r.BridgeID, &r.Owner, &r.ID, &r.RemoteName,
		dbutil.JSON{Data: &r.RemoteProfile}, &r.SpaceRoom, dbutil.JSON{Data: &r.Metadata})
	return dbutil.ValueOrErr(r, err)
}

func (s *Store) values(r *LoginRecord) []any {
	// PostgreSQL 的 JSONB 参数按文本传递，空资料保留 SQL NULL。
	var profile any
	if len(r.RemoteProfile) > 0 {
		profile = string(r.RemoteProfile)
	}
	return []any{s.bridgeID, r.Owner, r.ID, r.RemoteName, profile, r.SpaceRoom, string(r.Metadata)}
}
