// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package wamedia

import "go.mau.fi/whatsmeow"

// DownloadInfo 保存 WhatsApp 原生媒体下载及重试字段，沿用旧库元数据的 JSON 键。
type DownloadInfo struct {
	Key        []byte              `json:"key"`
	Length     uint64              `json:"length"`
	Type       whatsmeow.MediaType `json:"type"`
	SHA256     []byte              `json:"sha256"`
	EncSHA256  []byte              `json:"enc_sha256"`
	DirectPath string              `json:"direct_path,omitempty"`
	MimeType   string              `json:"mime_type,omitempty"`
}

func (f *DownloadInfo) GetDirectPath() string {
	return f.DirectPath
}

func (f *DownloadInfo) GetMediaType() whatsmeow.MediaType {
	return f.Type
}

func (f *DownloadInfo) GetFileLength() uint64 {
	return f.Length
}

func (f *DownloadInfo) GetMediaKey() []byte {
	return f.Key
}

func (f *DownloadInfo) GetFileSHA256() []byte {
	return f.SHA256
}

func (f *DownloadInfo) GetFileEncSHA256() []byte {
	return f.EncSHA256
}

var (
	_ whatsmeow.DownloadableMessage = (*DownloadInfo)(nil)
	_ whatsmeow.MediaTypeable       = (*DownloadInfo)(nil)
)
