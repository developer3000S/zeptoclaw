# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# ZeptoClaw Agent Mesh — production image.
#
# Stage 1 compiles the daemon with the exact Go toolchain from go.mod, so the
# image never drifts from what the project supports. Stage 2 is a minimal
# runtime: CA certs, tzdata and a non-root user, nothing more.
#
# The binary is statically linked (CGO_ENABLED=0) so the runtime base does not
# need a glibc matching the build environment.
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
# -X overrides (see internal/version).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/developer3000S/zeptoclaw/internal/version.GitCommit=${GIT_COMMIT} \
        -X github.com/developer3000S/zeptoclaw/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/zeptomesh-node ./cmd/zeptomesh-node

# ---------------------------------------------------------------------------

FROM debian:bookworm-slim AS runtime

# curl is used by the HEALTHCHECK; ca-certificates by picoclaw http mode and
# any outbound TLS; tzdata because schedules and logs are timezone-sensitive.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl tzdata \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /var/lib/zeptomesh --shell /usr/sbin/nologin zeptomesh

COPY --from=build /out/zeptomesh-node /usr/local/bin/zeptomesh-node
# Mount your own file over this path to replace the whole configuration.
COPY configs/node.yaml /etc/zeptomesh/node.yaml

# One volume holds identity keys, the BadgerDB store, artifacts and the task
# workspaces. Losing it means losing the node's peer identity.
VOLUME ["/var/lib/zeptomesh"]

# The config loader expands ${VAR:-default} itself (internal/config.expandEnv),
# so this template drives every instance: change the env, not the file.
#
# ZETOMESH_API_HOST must be 0.0.0.0 here, unlike a bare-metal install: Docker
# publishes a port by connecting to the container's routable address, so an API
# bound to 127.0.0.1 inside the netns would be unreachable from the host.
ENV ZETOMESH_DATA=/var/lib/zeptomesh \
    ZETOMESH_CONFIG=/etc/zeptomesh/node.yaml \
    ZETOMESH_API_HOST=0.0.0.0 \
    ZETOMESH_MESH_PORT=4001 \
    ZETOMESH_API_PORT=8081 \
    ZETOMESH_PROM_PORT=9464 \
    ZETOMESH_PICO_MODE=stub \
    TZ=UTC

USER zeptomesh
WORKDIR /var/lib/zeptomesh

# QUIC (udp) and TCP mesh ports, the admin API and the Prometheus listener.
EXPOSE 4001/tcp 4001/udp 8081/tcp 9464/tcp

HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${ZETOMESH_API_PORT:-8081}/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/zeptomesh-node"]
CMD ["run"]
