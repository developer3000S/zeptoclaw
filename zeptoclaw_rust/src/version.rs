//! Версия сборки (аналог `internal/version`).

/// Версия узла mesh.
pub const VERSION: &str = "0.1.0-rust";

/// Возвращает строку версии, учитывающую переопределение через env.
pub fn version() -> String {
    std::env::var("ZEPTOMESH_VERSION").unwrap_or_else(|_| VERSION.to_string())
}
