//! Сквозной smoke-тест Rust-порта: запускаем настоящий бинарь zeptomesh-node,
//! проверяем genkey → run → HTTP admin-API (status/tasks) → shutdown.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

fn bin() -> String { env!("CARGO_BIN_EXE_zeptomesh-node").to_string() }

fn get(url: &str) -> serde_json::Value {
    let out = reqwest::blocking::Client::new()
        .get(url).send().unwrap().text().unwrap();
    serde_json::from_str(&out).unwrap_or_else(|e| panic!("bad json {url}: {e}: {out}"))
}

#[test]
fn node_run_status_shutdown() {
    let dir = std::env::temp_dir().join(format!("zm-smoke-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();

    // 1. genkey
    let key = dir.join("node.key");
    let port = 39500 + (std::process::id() % 500);
    // Формат YAML соответствует Go-конфигу (internal/config): node.listen —
    // multiaddr, identity.key_file, api.auth_token_env (токен берётся из env).
    std::env::set_var("ZM_SMOKE_API_TOKEN", "tok123");
    let yaml = format!(
        "node:\n  name: smoke\n  listen:\n    - /ip4/127.0.0.1/tcp/{}\nidentity:\n  key_file: {}\napi:\n  listen: 127.0.0.1:{}\n  auth_token_env: ZM_SMOKE_API_TOKEN\npicoclaw:\n  mode: stub\n",
        port, key.display(), port + 1
    );
    let cfg = dir.join("node.yaml");
    std::fs::write(&cfg, &yaml).unwrap();
    let o = Command::new(bin()).args(["genkey", "--out"]).arg(&key).output().unwrap();
    assert!(o.status.success(), "genkey: {}", String::from_utf8_lossy(&o.stderr));
    assert!(key.exists());

    // 2. run (фоновый процесс), ждём строки готовности
    let mut child = Command::new(bin())
        .args(["run", "-c"]).arg(&cfg)
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).unwrap() == 0 { panic!("node exited early: {line}"); }
        if line.contains("listening") || line.contains("started") || line.contains("peer_id") { break; }
    }

    // 3. API: status с токеном и без
    let base = format!("http://127.0.0.1:{}", port + 1);
    let st = get(&format!("{base}/api/v1/status?token=tok123"));
    assert_eq!(st["node"]["name"].as_str().unwrap_or(""), "smoke");
    let peers = get(&format!("{base}/api/v1/peers?token=tok123"));
    assert!(peers.is_array() || peers.get("peers").is_some());
    // без токена — 401
    let r = reqwest::blocking::Client::new().get(format!("{base}/api/v1/status")).send().unwrap();
    assert_eq!(r.status().as_u16(), 401);

    // 4. submit задачи через stub-адаптер + poll результата
    let body = serde_json::json!({"instruction": "echo hello", "required_skills": ["general"]});
    let r = reqwest::blocking::Client::new()
        .post(format!("{base}/api/v1/tasks?token=tok123")).json(&body).send().unwrap();
    assert_eq!(r.status().as_u16(), 200, "submit failed");
    let sub: serde_json::Value = r.json().unwrap();
    let id = sub["task_id"].as_str().unwrap().to_string();
    let mut status = String::new();
    for _ in 0..100 {
        let t = get(&format!("{base}/api/v1/tasks/{id}?token=tok123"));
        status = t["status"].as_str().unwrap_or("").to_string();
        if status == "COMPLETED" || status == "FAILED" { break; }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(status, "COMPLETED", "task did not complete");

    // 5. shutdown
    let r = reqwest::blocking::Client::new()
        .post(format!("{base}/api/v1/shutdown?token=tok123")).send().unwrap();
    assert!(r.status().is_success());
    let _ = child.wait();
    std::fs::remove_dir_all(&dir).ok();
}
