FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY . .

# Build etl and probe binaries.
RUN set -eu; \
  mkdir -p /app/bin; \
  for d in ./cmd/*; do \
    [ -d "$d" ] || continue; \
    [ -f "$d/main.go" ] || continue; \
    name="$(basename "$d")"; \
    echo "==> building $name"; \
    go build -o "/app/bin/$name" "./cmd/$name"; \
  done

FROM alpine:3

RUN apk add bash jq

# Create non-root user/group
RUN addgroup -S etl && adduser -S -G etl -h /home/etl etl

WORKDIR /app
COPY --from=builder /app/bin/ /usr/local/bin/
COPY --from=builder /app/tests/ /app/tests/
COPY --from=builder /app/configs/ /app/configs/

# directories for generated configs and sqlite DB (owned by non-root)
RUN mkdir -p /configs /data/db /home/etl && \
    chown -R etl:etl /configs /data/db /home/etl /app

USER etl
