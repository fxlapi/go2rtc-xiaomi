# syntax=docker/dockerfile:labs

# 基于 AlexxIT/go2rtc 官方 docker/Dockerfile 改造：
# 构建上下文为本插件目录，源码位于 src/ 子目录（含全部补丁）。

# 0. Prepare images
ARG PYTHON_VERSION="3.13"
ARG GO_VERSION="1.27"

# 1. Build go2rtc binary (patched source in src/)
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /build

RUN apk add git

# Cache dependencies
COPY src/go.mod src/go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

COPY src/ .
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath

# 2. Final image
FROM python:${PYTHON_VERSION}-alpine AS base

RUN apk add --no-cache tini ffmpeg ffplay bash curl jq alsa-plugins-pulse font-droid

# Hardware Acceleration for Intel CPU (+50MB)
ARG TARGETARCH
RUN if [ "${TARGETARCH}" = "amd64" ]; then apk add --no-cache libva-intel-driver intel-media-driver; fi

COPY --from=build /build/go2rtc /usr/local/bin/

EXPOSE 1984 8554 8555 8555/udp
ENTRYPOINT ["/sbin/tini", "--"]
VOLUME /config
WORKDIR /config

CMD ["go2rtc", "-config", "/config/go2rtc.yaml"]
