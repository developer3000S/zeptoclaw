// agents.js — вкладка «Агенты»: карточки управляемых узлов + управление списком.

import { api } from "./api.js";
import { store, subscribe, esc, fmtPct, fmtTime } from "./store.js";
import { toast } from "./main.js";

export function initAgents() {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Агенты</h1>
    <p class="hint">Управляемые узлы из списка панели. Токены узлов хранятся только в бэкенде.</p>

    <div class="panel" style="margin-bottom:12px">
      <form id="add-node-form" class="form-row">
        <div><label for="n-name">Имя</label><input id="n-name" placeholder="zepto-3" required></div>
        <div><label for="n-url">URL админ-API</label><input id="n-url" placeholder="http://zepto-3:8081" required></div>
        <div><label for="n-token">Токен узла (необязательно)</label><input id="n-token" type="password" placeholder="не хранится в браузере"></div>
        <div style="display:flex;align-items:end"><button class="btn btn-primary" type="submit">Добавить</button></div>
      </form>
    </div>

    <div class="grid-cards" id="agent-cards"></div>
  `;

  document.getElementById("add-node-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const name = document.getElementById("n-name").value.trim();
    const url = document.getElementById("n-url").value.trim();
    const token = document.getElementById("n-token").value.trim();
    try {
      await api.addNode(name, url, token);
      toast(`узел «${name}» добавлен`, "ok");
      e.target.reset();
    } catch (err) {
      toast("не удалось добавить узел: " + err.message, "err");
    }
  });

  subscribe(renderAgents);
  renderAgents(store);
}

function renderAgents(s) {
  const el = document.getElementById("agent-cards");
  if (!el) return;
  const nodes = s.mesh?.nodes ?? [];
  if (!nodes.length) {
    el.innerHTML = `<div class="empty">Нет управляемых узлов. Добавьте первый узел формой выше.</div>`;
    return;
  }
  el.innerHTML = nodes.map((n) => nodeCard(n)).join("");
  for (const n of nodes) bindCardActions(n);
}

function nodeCard(n) {
  const st = n.status || {};
  const reachable = n.reachable;
  const cls = reachable ? "ok" : "bad";
  const adapterOK = st.adapter?.healthy;
  const err = n.error
    ? `<div class="hint" style="color:var(--red)">ошибка: ${esc(n.error)}</div>`
    : "";
  return `
  <div class="card" data-name="${esc(n.name)}">
    <h3>${esc(n.name)} <span class="pill ${cls}">${reachable ? "reachable" : "unreachable"}</span></h3>
    ${err}
    <dl class="kv">
      <dt>url</dt><dd class="mono">${esc(n.url)}</dd>
      <dt>peer_id</dt><dd class="mono">${esc(n.peer_id || "—")}</dd>
      <dt>версия</dt><dd>${esc(st.version || "—")}</dd>
      <dt>uptime</dt><dd>${st.uptime_sec ? humanDuration(st.uptime_sec) : "—"}</dd>
      <dt>load</dt><dd>${fmtPct(st.load)}</dd>
      <dt>задачи</dt><dd>${st.running_tasks ?? 0} running / ${st.tracked_tasks ?? 0} tracked</dd>
      <dt>соседи</dt><dd>${st.neighbors_connected ?? 0} подключено / ${st.neighbors_total ?? 0} всего</dd>
      <dt>адаптер</dt><dd>${esc(st.adapter?.name || "—")} ${adapterOK === undefined ? "" : adapterOK ? '<span class="pill ok">healthy</span>' : '<span class="pill bad">down</span>'} ${esc(st.adapter?.model || "")}</dd>
      <dt>trust_mode</dt><dd>${esc(st.security?.trust_mode || "—")}</dd>
      <dt>latency</dt><dd>${reachable ? n.latency_ms + " ms" : "—"}</dd>
    </dl>
    <div style="display:flex;gap:6px;margin-top:10px;flex-wrap:wrap">
      <button class="btn btn-sm" data-act="tasks">Задачи</button>
      <button class="btn btn-sm" data-act="reload">reload-config</button>
      <button class="btn btn-sm btn-danger" data-act="leave">leave</button>
      <button class="btn btn-sm btn-danger" data-act="delete">удалить</button>
    </div>
  </div>`;
}

function bindCardActions(n) {
  const card = document.querySelector(`.card[data-name="${CSS.escape(n.name)}"]`);
  if (!card) return;
  card.querySelector('[data-act="tasks"]').addEventListener("click", () => {
    location.hash = "#/tasks/" + encodeURIComponent(n.name);
  });
  card.querySelector('[data-act="reload"]').addEventListener("click", async (e) => {
    const btn = e.target;
    btn.disabled = true;
    try {
      const res = await api.node(n.name).reloadConfig();
      const applied = res?.applied ? "применено" : "не применено";
      const restart = res?.requires_restart ? ", требует рестарта (leave/systemctl restart)" : "";
      toast(`reload-config: ${applied}${restart}`, restart ? "err" : "ok");
    } catch (err) {
      toast("reload-config: " + err.message, "err");
    } finally {
      btn.disabled = false;
    }
  });
  card.querySelector('[data-act="leave"]').addEventListener("click", async () => {
    if (!confirm(`Узел «${n.name}» покинет сеть — его перезапустит супервизор. Продолжить?`)) return;
    try {
      await api.node(n.name).leave();
      toast(`узел «${n.name}» ушёл (будет перезапущен)`, "ok");
    } catch (err) {
      toast("leave: " + err.message, "err");
    }
  });
  card.querySelector('[data-act="delete"]').addEventListener("click", async () => {
    if (!confirm(`Удалить узел «${n.name}» из списка панели? Сами настройки агента не затрагиваются.`)) return;
    try {
      await api.deleteNode(n.name);
      toast(`узел «${n.name}» удалён из списка`, "ok");
    } catch (err) {
      toast("удаление узла: " + err.message, "err");
    }
  });
}

function humanDuration(sec) {
  const s = Math.floor(sec);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  if (h) return `${h}ч ${m}м`;
  if (m) return `${m}м ${r}с`;
  return `${r}с`;
}
