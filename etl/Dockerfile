# ---------- build stage ----------
FROM golang:1.25-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build Linux binary
RUN cd cmd/etl && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /out/etl-linux

# Build Windows binary
#RUN cd cmd/etl && \
#    CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
#    go build -o /out/etl-windows.exe

# ---------- runtime stage (Linux only) ----------
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/etl-linux /usr/local/bin/etl

ENTRYPOINT ["/usr/local/bin/etl"]

