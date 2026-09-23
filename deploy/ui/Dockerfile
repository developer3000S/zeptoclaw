# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# ZeptoClaw Mesh UI — production image.
#
# Stage 1 compiles the dashboard backend (cmd/zeptomesh-ui) with the exact Go
# toolchain from go.mod. The whole frontend is embedded into the binary
# (//go:embed all:static), so stage 2 needs no static assets copied across.
#
# Stage 2 mirrors the agent image: CA certs, curl for the HEALTHCHECK, tzdata,
# and a non-root user. Runtime configuration is entirely environmental.
# ---------------------------------------------------------------------------

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

# Dependencies first: this layer is reused unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG VERSION=0.1.0
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

# -trimpath removes /src from panic traces; the version package reads these
# -X overrides (see internal/version). Same flags as the agent build.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/developer3000S/zeptoclaw/internal/version.Version=${VERSION} \
        -X github.com/developer3000S/zeptoclaw/internal/version.GitCommit=${GIT_COMMIT} \
        -X github.com/developer3000S/zeptoclaw/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/zeptomesh-ui ./cmd/zeptomesh-ui

# ---------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

# curl drives the HEALTHCHECK; ca-certificates is needed when a managed node's
# admin API is reached over TLS; tzdata because the dashboard renders times.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl tzdata \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /var/lib/zeptomesh-ui --shell /usr/sbin/nologin zeptomesh

COPY --from=build /out/zeptomesh-ui /usr/local/bin/zeptomesh-ui

# Holds nodes.json — the UI's editable source of truth for mesh membership.
VOLUME ["/var/lib/zeptomesh-ui"]

# ZETOMESH_UI_LISTEN must be 0.0.0.0 here, unlike a bare-metal install: Docker
# publishes a port by connecting to the container's routable address, so a
# listener bound to 127.0.0.1 inside the netns would be unreachable from the
# host. install-ui.sh sets it explicitly, but the default keeps the image
# usable on its own.
ENV ZETOMESH_UI_DATA=/var/lib/zeptomesh-ui \
    ZETOMESH_UI_LISTEN=0.0.0.0:28090 \
    ZETOMESH_UI_POLL_INTERVAL=5s \
    ZETOMESH_UI_LOG_LEVEL=info \
    TZ=UTC

USER zeptomesh
WORKDIR /var/lib/zeptomesh-ui

EXPOSE 28090/tcp

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:28090/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/zeptomesh-ui"]
