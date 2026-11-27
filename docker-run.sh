#!/bin/sh
# 直接启动 WhatsApp Inbox，运行配置由部署环境提供。
cd /data || exit 1
exec /usr/bin/mautrix-whatsapp -config /data/config.yaml "$@"
