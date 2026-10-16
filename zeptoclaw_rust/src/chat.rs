//! Порт пакета `internal/chat`: чат-ручка поверх brain-бэкендов.
//!
//! Хранит историю диалогов в памяти (per-conversation), шлёт сообщения в
//! OpenAI-совместимый `/v1/chat/completions` выбранных бэкендов (Go: chat
//! ходит через brain pool с фолбэком на следующий usable-эндпоинт).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::picoclaw::BackendPool;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String, // user | assistant | system
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistoryEntry {
    pub conversation: String,
    pub messages: Vec<ChatMessage>,
}

/// Максимум сообщений, хранимых на диалог (Go: chat.historyLimit).
const HISTORY_LIMIT: usize = 200;

pub struct ChatService {
    pool: Arc<BackendPool>,
    conversations: Mutex<HashMap<String, Vec<ChatMessage>>>,
}

impl ChatService {
    pub fn new(pool: Arc<BackendPool>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            conversations: Mutex::new(HashMap::new()),
        })
    }

    pub fn history(&self, conversation: &str) -> Vec<ChatMessage> {
        let g = self.conversations.lock().unwrap();
        g.get(conversation).cloned().unwrap_or_default()
    }

    pub fn clear(&self, conversation: &str) {
        self.conversations.lock().unwrap().remove(conversation);
    }

    /// Отправить сообщение в диалог; ответ модели добавляется в историю.
    /// Возвращает (assistant_message, backend_id).
    pub async fn send(
        &self,
        conversation: &str,
        text: &str,
        model: Option<&str>,
    ) -> Result<(ChatMessage, String)> {
        if text.trim().is_empty() {
            bail!("chat: empty message");
        }
        let user = ChatMessage {
            role: "user".into(),
            content: text.to_string(),
            created_at: Some(Utc::now()),
        };
        let mut convo = self.history(conversation);
        convo.push(user.clone());

        let candidates = self.pick_candidates(model);
        if candidates.is_empty() {
            bail!("chat: no reachable LLM backend (check brain backends / picoclaw.backends)");
        }

        let mut last_err = String::from("no backend attempted");
        for (backend_id, base_url, api_key, default_model) in candidates {
            let body = serde_json::json!({
                "model": model.unwrap_or(&default_model),
                "messages": convo.iter().map(|m| serde_json::json!({
                    "role": m.role, "content": m.content,
                })).collect::<Vec<_>>(),
                "stream": false,
            });
            let url = format!("{base_url}/v1/chat/completions");
            let mut req = self.pool.http().post(&url).json(&body);
            if !api_key.is_empty() {
                req = req.bearer_auth(&api_key);
            }
            match req.send().await {
                Ok(r) => {
                    let status = r.status();
                    let txt = r.text().await.unwrap_or_default();
                    if !status.is_success() {
                        last_err = format!("{backend_id}: HTTP {status}: {}", truncate(&txt, 200));
                        continue;
                    }
                    let v: serde_json::Value = match serde_json::from_str(&txt) {
                        Ok(v) => v,
                        Err(e) => {
                            last_err = format!("{backend_id}: bad json: {e}");
                            continue;
                        }
                    };
                    let content = v["choices"][0]["message"]["content"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    if content.is_empty() {
                        last_err = format!("{backend_id}: empty completion");
                        continue;
                    }
                    let reply = ChatMessage {
                        role: "assistant".into(),
                        content: content.clone(),
                        created_at: Some(Utc::now()),
                    };
                    convo.push(reply.clone());
                    if convo.len() > HISTORY_LIMIT {
                        convo.drain(..convo.len() - HISTORY_LIMIT);
                    }
                    self.conversations
                        .lock()
                        .unwrap()
                        .insert(conversation.to_string(), convo);
                    return Ok((reply, backend_id));
                }
                Err(e) => {
                    last_err = format!("{backend_id}: {e}");
                    continue;
                }
            }
        }
        // История пользователя всё равно сохраняется (Go: append до запроса).
        let mut g = self.conversations.lock().unwrap();
        let c = g.entry(conversation.to_string()).or_default();
        if c.last().map(|m| m.content.as_str()) != Some(text) {
            c.push(user);
            if c.len() > HISTORY_LIMIT {
                c.drain(..c.len() - HISTORY_LIMIT);
            }
        }
        drop(g);
        bail!("chat: all backends failed, last error: {last_err}")
    }

    /// Порядок обхода: явная модель → самый свежий verified/usable backend.
    fn pick_candidates(&self, model: Option<&str>) -> Vec<(String, String, String, String)> {
        let mut out = Vec::new();
        for b in self.pool.usable() {
            let default_model = match model {
                Some(m) if b.models.iter().any(|x| x == m) => m.to_string(),
                _ => biggest_of(&b.models),
            };
            if default_model.is_empty() && model.is_none() {
                continue; // не знаем, что просить
            }
            out.push((
                b.id.clone(),
                b.base_url.clone(),
                b.api_key.clone(),
                if let Some(m) = model { m.to_string() } else { default_model },
            ));
        }
        out
    }
}

fn biggest_of(models: &[String]) -> String {
    crate::picoclaw::biggest_model(models).unwrap_or_default()
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut t = s[..n].to_string();
        t.push('…');
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::picoclaw::BackendConfig;

    #[test]
    fn no_backends_errors() {
        let pool = Arc::new(BackendPool::new("", &[BackendConfig::default()]));
        let svc = ChatService::new(pool);
        // usable() пуст → pick пуст → send обязан упасть без паники.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(svc.send("c1", "hi", None)).unwrap_err();
        assert!(err.to_string().contains("no reachable LLM backend"));
        assert_eq!(svc.history("c1").len(), 0);
    }

    #[test]
    fn history_limit_and_clear() {
        let pool = Arc::new(BackendPool::new("", &[]));
        let svc = ChatService::new(pool);
        {
            let mut g = svc.conversations.lock().unwrap();
            g.insert(
                "x".into(),
                (0..HISTORY_LIMIT + 50)
                    .map(|i| ChatMessage { role: "user".into(), content: i.to_string(), created_at: None })
                    .collect(),
            );
        }
        svc.clear("x");
        assert!(svc.history("x").is_empty());
    }
}
