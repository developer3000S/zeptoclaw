# ZeptoClaw Agent Mesh - Docker
# Образ соответствует локальному тулчейну (rustc 1.99): Cargo.lock не
# коммитится, поэтому свежие транзитивные зависимости (icu_provider,
# uuid и др.) требуют актуальной версии rustc — не ниже локальной.
FROM rust:1.99-slim AS builder

WORKDIR /build

# Установка зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование исходников
COPY zeptoclaw_rust/ ./zeptoclaw_rust/

# Сборка
WORKDIR /build/zeptoclaw_rust
RUN cargo build --release

# Минимальный runtime образ
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя
RUN useradd -m -u 1000 zepto

WORKDIR /app

# Копирование бинарника
COPY --from=builder /build/zeptoclaw_rust/target/release/zeptomesh-node /app/zeptomesh-node

# Директории
RUN mkdir -p /var/lib/zeptomesh /var/log/zeptomesh && \
    chown -R zepto:zepto /var/lib/zeptomesh /var/log/zeptomesh

USER zepto

EXPOSE 4001 8081 9464

ENTRYPOINT ["/app/zeptomesh-node"]
CMD ["run", "-c", "/etc/zeptomesh/node.yaml"]
