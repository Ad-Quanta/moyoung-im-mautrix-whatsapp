FROM golang:1-alpine3.24 AS builder

RUN apk add --no-cache ca-certificates git

COPY . /build
WORKDIR /build
# 独立 Inbox 入口使用标准 Go 构建。
RUN CGO_ENABLED=0 ./build.sh

FROM alpine:3.24

ENV UID=1337 \
    GID=1337

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/mautrix-whatsapp /usr/bin/mautrix-whatsapp
VOLUME /data
WORKDIR /data

CMD ["/usr/bin/mautrix-whatsapp", "-config", "/data/config.yaml"]
