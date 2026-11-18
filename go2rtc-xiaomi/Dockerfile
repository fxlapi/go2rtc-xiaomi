# syntax=docker/dockerfile:labs

# 基于 AlexxIT/go2rtc 官方 docker/Dockerfile 改造：
# 构建上下文为本插件目录，源码位于 src/ 子目录（含全部补丁）。
# 国内加速：基础镜像走 DaoCloud 镜像站，Alpine 软件包走清华源，Go 模块走 goproxy.cn。

# 0. Prepare images
ARG PYTHON_VERSION="3.13"
ARG GO_VERSION="1.27"
ARG GO_IMAGE="docker.m.daocloud.io/library/golang"
ARG PY_IMAGE="docker.m.daocloud.io/library/python"

# 1. Build go2rtc binary (patched source in src/)
FROM --platform=$BUILDPLATFORM ${GO_IMAGE}:${GO_VERSION}-alpine AS build
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /build

RUN sed -i 's#dl-cdn.alpinelinux.org#mirrors.tuna.tsinghua.edu.cn#g' /etc/apk/repositories \
    && apk add git

# Cache dependencies
COPY src/go.mod src/go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

COPY src/ .
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath

# 2. Final image
FROM ${PY_IMAGE}:${PYTHON_VERSION}-alpine AS base

RUN sed -i 's#dl-cdn.alpinelinux.org#mirrors.tuna.tsinghua.edu.cn#g' /etc/apk/repositories \
    && apk add --no-cache tini ffmpeg ffplay bash curl jq alsa-plugins-pulse font-droid

# Hardware Acceleration for Intel CPU (+50MB)
ARG TARGETARCH
RUN if [ "${TARGETARCH}" = "amd64" ]; then apk add --no-cache libva-intel-driver intel-media-driver; fi

COPY --from=build /build/go2rtc /usr/local/bin/

# 整合界面直接内置于 go2rtc 二进制（src/www/），无需额外面板文件

EXPOSE 1984 8554 8555 8555/udp
ENTRYPOINT ["/sbin/tini", "--"]
VOLUME /config
WORKDIR /config

CMD ["go2rtc", "-config", "/config/go2rtc.yaml"]
