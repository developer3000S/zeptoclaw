# zeptoclaw_rust — Rust-порт ZeptoClaw Agent Mesh

Порт Go-демона (`/internal`, `/cmd`) на Rust. Crate `zeptomesh` + бинарь
`zeptomesh-node`. Wire-совместим с Go-версией: общий protobuf-контракт,
идентичные канонические энкодеры, тот же формат файла ключей и admin-API.

## Быстрый старт

```bash
cargo build --release          # target/release/zeptomesh-node
cargo test                     # юнит-тесты + tests/smoke_e2e.rs

./target/release/zeptomesh-node genkey --out ./n0/keys/peer.key --print-id
ZEPTOMESH_DATA=./n0 ZEPTOMESH_INDEX=0 \
  ./target/release/zeptomesh-node run -c ../configs/node.yaml &
./target/release/zeptomesh-node status -addr http://127.0.0.1:8081
```

## Документация

- **[RUST_PORT.md](RUST_PORT.md)** — состав модулей, статус переноса по
  Go-пакетам, ключевые решения и известные ограничения.
- Контракт протокола: [../api/proto/zeptomesh/v1/mesh.proto](../api/proto/zeptomesh/v1/mesh.proto)
- Канонические энкодеры: [../docs/PROTOCOLS.md](../docs/PROTOCOLS.md)
- Архитектура: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)

Каталог `target/` и данные узла (`zeptomesh-data/`) в VCS не попадают
(см. корневой `.gitignore`).
