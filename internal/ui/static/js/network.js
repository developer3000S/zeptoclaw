// network.js — вкладка «Сеть»: весь граф сети + панель деталей + фильтры.

import { api, ApiUnauthorized } from "./api.js";
import { store, subscribe, esc, fmtTime, fmtPct } from "./store.js";
import { MeshGraph, passFilter } from "./graph.js";

let graph = null;
let selected = null;       // выбранная вершина
let filters = {
  managedOnly: false,
  connectedOnly: false,
  trust: "trusted-known",  // по умолчанию скрываем untrusted/blocked
  hideUntrusted: true,
  hideDisconnected: true,
};

export function initNetwork() {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Сеть</h1>
    <p class="hint">Граф объединяет управляемые узлы и всех пиров, о которых они сообщают. Клик по вершине — детали.</p>

    <div class="filters">
      <label class="field"><span>Доверие</span>
        <select id="f-trust">
          <option value="trusted-known">trusted + known (по умолчанию)</option>
          <option value="all">все</option>
          <option value="trusted">trusted</option>
          <option value="known">known</option>
          <option value="untrusted">untrusted</option>
          <option value="blocked">blocked</option>
        </select>
      </label>
      <label class="check"><input type="checkbox" id="f-connected"> только подключённые</label>
      <label class="check"><input type="checkbox" id="f-managed"> только managed</label>
      <div class="field"><button id="f-reset" class="btn btn-sm">Сбросить фильтры</button></div>
    </div>

    <div class="split">
      <div class="graph-wrap"><svg id="mesh-graph"></svg></div>
      <div class="panel" id="vertex-details">
        <h3>Деталы узла</h3>
        <p class="hint">Выберите вершину на графе.</p>
      </div>
    </div>

    <div class="legend">
      <span><i style="background:#3fb950"></i>managed</span>
      <span><i style="background:#7d8ea1"></i>discovered</span>
      <span><i style="background:#f85149"></i>untrusted / blocked</span>
      <span><i style="background:transparent;border-style:dashed"></i>ушёл (left)</span>
      <span>сплошное ребро — подключено, пунктирное — известно, но отключено</span>
    </div>
  `;

  const svg = document.getElementById("mesh-graph");
  graph = new MeshGraph(svg, (v) => {
    selected = v.id;
    renderDetails();
    graph.animate();
  });

  document.getElementById("f-trust").value = filters.trust;
  document.getElementById("f-trust").addEventListener("change", (e) => {
    filters.trust = e.target.value;
    filters.hideUntrusted = filters.trust === "trusted-known";
    refreshGraph();
  });
  document.getElementById("f-connected").addEventListener("change", (e) => {
    filters.connectedOnly = e.target.checked;
    refreshGraph();
  });
  document.getElementById("f-managed").addEventListener("change", (e) => {
    filters.managedOnly = e.target.checked;
    refreshGraph();
  });
  document.getElementById("f-reset").addEventListener("click", () => {
    filters = { managedOnly: false, connectedOnly: false, trust: "trusted-known", hideUntrusted: true, hideDisconnected: true };
    document.getElementById("f-trust").value = filters.trust;
    document.getElementById("f-connected").checked = false;
    document.getElementById("f-managed").checked = false;
    refreshGraph();
  });

  subscribe(renderNetwork);
  renderNetwork(store);
}

function refreshGraph() {
  if (store.mesh) renderNetwork(store);
}

function renderNetwork(s) {
  if (!graph || !s.mesh) return;
  const changed = graph.setData(s.mesh.graph, filters);
  if (changed) graph.animate();
  if (selected) renderDetails();
}

function renderDetails() {
  const el = document.getElementById("vertex-details");
  if (!el) return;
  const v = (store.mesh?.graph?.vertices ?? []).find((x) => x.id === selected);
  if (!v) {
    el.innerHTML = `<h3>Деталы узла</h3><p class="hint">Выберите вершину на графе.</p>`;
    return;
  }
  const peers = store.mesh?.nodes ?? [];
  const node = peers.find((n) => n.peer_id === v.id);
  const addrs = (node?.status?.addrs ?? []).map(esc).join("<br>") || "—";
  el.innerHTML = `
    <h3>${esc(v.label || v.id)}</h3>
    <dl class="kv">
      <dt>peer_id</dt><dd class="mono">${esc(v.id)}</dd>
      <dt>тип</dt><dd>${v.kind === "managed" ? "managed" : "discovered"}</dd>
      <dt>trust</dt><dd>${esc(v.trust || "unknown")}</dd>
      <dt>подключён</dt><dd>${v.connected ? "да" : "нет"}</dd>
      <dt>ушёл</dt><dd>${v.left ? "да" : "нет"}</dd>
      <dt>load</dt><dd>${fmtPct(v.load)}</dd>
      <dt>skills</dt><dd>${(v.skills ?? []).map(esc).join(", ") || "—"}</dd>
      <dt>откуда виден</dt><dd class="mono">${esc(v.source || "—")}</dd>
      <dt>addrs</dt><dd class="mono">${addrs}</dd>
    </dl>
    ${node ? `<p class="hint">управляемый узел «${esc(node.name)}» — полный статус на вкладке «Агенты».</p>` : ""}
  `;
}
