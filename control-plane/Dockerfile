FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.work go.work.sum ./
COPY control-plane/go.mod control-plane/go.sum ./control-plane/
COPY worker-agent/go.mod worker-agent/go.sum ./worker-agent/
COPY scheduler/go.mod ./scheduler/
COPY scheduler/ ./scheduler/
COPY control-plane/ ./control-plane/
COPY worker-agent/ ./worker-agent/

RUN CGO_ENABLED=0 GOOS=linux go -C control-plane build \
    -trimpath -ldflags="-s -w" -o /out/atlasfarm-control-plane ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 65532 atlasfarm \
    && adduser -S -D -H -u 65532 -G atlasfarm atlasfarm
COPY --from=builder /out/atlasfarm-control-plane /usr/local/bin/atlasfarm-control-plane
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/atlasfarm-control-plane", "--enable-pprof=false"]
