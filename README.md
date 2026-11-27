# WhatsApp Inbox

独立连接 WhatsApp，将实时消息推送给 Router。服务通过独立 HTTP 监听提供登录、联系人、发送、媒体下载和注销。当前命令保留 `mautrix-whatsapp` 文件名，以兼容现有 Compose 服务名。

## 启动与构建

```bash
go build ./cmd/mautrix-whatsapp
./mautrix-whatsapp -config /data/config.yaml
```

在配置中填写 `listen`、`database.schema`、`provisioning.shared_secret`，以及 `network.inbox` 的 `push_url` 和 `push_secret`。数据库连接来自 `DB_DSN`。`GET /health` 用于健康检查。Router 继续使用 `/_matrix/provision/v3` 路径和既有绑定键。启动时不加载 Matrix registration。

## 数据库

WhatsApp 在 `database.schema` 指定的 PostgreSQL schema 内维护一套连续编号的 migration。`internal/database/migrations/000001_init.up.sql` 创建独立的业务、注销和 WhatsMeow 协议表，`whatsapp_schema_migrations` 只记录一个版本号。后续结构变更按顺序追加编号。升级 WhatsMeow 依赖时，也在此目录补充对应的协议表变更。

需要保留旧数据时，在停写后把旧 schema 连同 `database_owner` 和三张版本表搬到目标库。使用旧程序把源版本先升级到 Bridge 33、Connector 12、WhatsMeow 16。新程序首次启动会在一个事务内接管该结构，保留业务数据及序列，并把版本记录转换为统一版本 1。全新部署可以直接从空 schema 启动。

Router 在主仓维护 WhatsApp 收件箱的独立 schema 和 migration。设置测试 PostgreSQL URL `WA_TEST_DSN` 后，可以用 `go test ./cmd/mautrix-whatsapp` 验证统一迁移和注销流程。

## 源码

`internal/inboxhttp` 提供 Router 兼容的登录、状态、联系人、发送、媒体下载和注销接口。`internal/inboxservice` 管理 WhatsApp 实时会话，`internal/inboxsession` 负责扫码、配对码、Passkey 和已保存设备恢复，`internal/inbox` 定义实时消息与媒体协议，`internal/wamedia` 提供图片转换。服务入口不加载 Matrix、Appservice、Federation、旧连接器或旧 Matrix 加密存储。
