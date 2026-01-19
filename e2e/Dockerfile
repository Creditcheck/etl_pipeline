FROM golang:1.25-alpine AS builder

WORKDIR /app
#COPY ../etl .
COPY etl .

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

# Runtime image
FROM alpine:3

WORKDIR /app
COPY --from=builder /app/bin/ /usr/local/bin/

# directory for generated configs and sqlite DB
RUN mkdir -p /configs /data/db

ENTRYPOINT ["/bin/sh"]

