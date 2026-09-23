// config.js — вкладка «Конфиг»: просмотр конфига узла и hot-reload.

import { api } from "./api.js";
import { store, subscribe, esc, firstReachable } from "./store.js";
import { toast } from "./main.js";

let selectedNode = "";
let cfgYaml = "";

export function initConfig() {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Конфиг</h1>
    <div class="filters">
      <label class="field"><span>Узел</span><select id="c-node"></select></label>
      <button class="btn btn-sm" id="c-reload">reload-config</button>
    </div>
    <div class="panel">
      <h3>Конфигурация узла</h3>
      <div id="config-view"><p class="hint">Загрузка…</p></div>
    </div>
  `;

  const nodeSel = document.getElementById("c-node");
  nodeSel.addEventListener("change", (e) => { selectedNode = e.target.value; loadConfig(); });
  document.getElementById("c-reload").addEventListener("click", reloadConfig);

  subscribe(renderNodeSelector);
  renderNodeSelector(store);
  if (selectedNode) { nodeSel.value = selectedNode; loadConfig(); }
}

function renderNodeSelector(s) {
  const sel = document.getElementById("c-node");
  if (!sel) return;
  const nodes = (s.mesh?.nodes ?? []).filter((n) => n.reachable);
  const prev = selectedNode || firstReachable(s.mesh);
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.name)}">${esc(n.name)}</option>`).join("") ||
    `<option value="">нет reachable-узлов</option>`;
  if (nodes.some((n) => n.name === prev)) sel.value = prev;
  selectedNode = sel.value;
}

async function loadConfig() {
  const el = document.getElementById("config-view");
  if (!el) return;
  if (!selectedNode) { el.innerHTML = `<p class="hint">Нет доступного узла.</p>`; return; }
  try {
    const res = await api.node(selectedNode).config();
    // Узел отдаёт конфиг как YAML-строку или как JSON — показываем как есть.
    cfgYaml = typeof res === "string" ? res : (res?.config ?? res?.yaml ?? JSON.stringify(res, null, 2));
    el.innerHTML = `<pre class="json">${esc(cfgYaml)}</pre>`;
  } catch (err) {
    el.innerHTML = `<p class="hint" style="color:var(--red)">${esc(err.message)}</p>`;
  }
}

async function reloadConfig() {
  if (!selectedNode) { toast("выберите узел", "err"); return; }
  try {
    const res = await api.node(selectedNode).reloadConfig();
    const applied = res?.applied;
    const restart = res?.requires_restart;
    const msg = `reload-config: ${applied ? "применено" : "не применено"}` +
      (restart ? " — требует рестарта: выполните leave или systemctl restart" : "");
    toast(msg, restart || !applied ? "err" : "ok");
    // После reload конфиг в памяти узла обновляется — перечитываем для сверки.
    loadConfig();
  } catch (err) {
    toast("reload-config: " + err.message, "err");
  }
}
