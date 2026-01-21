FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY ./ .

# Build etl and probe binaries.
RUN set -eu; \
  mkdir -p /app/bin; \
  for d in ./cmd/*; do \
    [ -d "$d" ] || continue; \
    [ -f "$d/main.go" ] || continue; \
    name="$(basename "$d")"; \
    echo "==> building $name"; \
    CGO_ENABLED=0 go build -o "/usr/local/bin/$name" "./cmd/$name"; \
  done

#### development tooling ####
RUN apk add bash jq vim
RUN go test ./internal/...

FROM alpine:3

# Create non-root user/group
RUN addgroup -S etl && adduser -S -G etl -h /home/etl etl

WORKDIR /app
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY --from=builder /app/tests/ /app/tests
COPY --from=builder /app/configs/ /app/configs

# directories for generated configs and sqlite DB (owned by non-root)
RUN mkdir -p /configs /data/db /home/etl && \
    chown -R etl:etl /configs /data/db /home/etl /app

#### run with non-root user ####
USER etl
