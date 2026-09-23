// store.js — состояние панели: снимок сети + подписки контроллеров вкладок.
//
// Один поллинг /api/v1/mesh (см. main.js) обновляет store, а контроллеры
// перерисовываются через subscribe. Так график и карточки не делают
// собственных запросов — панель нагрузивает BFF одним запросом на такт.

export const store = {
  mesh: null,        // последний MeshSnapshot
  error: null,       // ошибка последнего поллинга (ApiUnauthorized — отдельно)
  unauthorized: false,
  paused: false,
  _subs: new Set(),
};

export function setMesh(mesh) {
  store.mesh = mesh;
  store.error = null;
  store.unauthorized = false;
  notify();
}

export function setPollError(err) {
  store.error = err;
  store.unauthorized = err?.name === "ApiUnauthorized";
  notify();
}

export function setPaused(paused) {
  store.paused = paused;
  notify();
}

export function subscribe(fn) {
  store._subs.add(fn);
  return () => store._subs.delete(fn);
}

function notify() {
  for (const fn of store._subs) {
    try { fn(store); } catch (e) { console.error("subscriber failed:", e); }
  }
}

// ---------- helpers для рендера из снимка ----------

export function managedNodes(mesh) {
  // Управляемые узлы — это nodes[] снимка; graph-вершины лишь дополняют их.
  return (mesh?.nodes ?? []).slice();
}

export function reachableNodes(mesh) {
  return managedNodes(mesh).filter((n) => n.reachable);
}

export function firstReachable(mesh) {
  return reachableNodes(mesh)[0]?.name ?? "";
}

// vertexOf сопоставляет узел снимка с вершиной графа по peer_id.
export function vertexOf(mesh, peerID) {
  return (mesh?.graph?.vertices ?? []).find((v) => v.id === peerID) ?? null;
}

// trustOf возвращает худший уровень доверия вершины.
export function trustOf(vertex) {
  return vertex?.trust ?? "unknown";
}

export function isManagedVertex(v) {
  return v?.kind === "managed";
}

export function fmtTime(unix) {
  if (!unix) return "—";
  const d = new Date(unix * 1000);
  return d.toLocaleTimeString("ru-RU");
}

export function fmtPct(x) {
  if (x === undefined || x === null) return "—";
  return (x * 100).toFixed(0) + "%";
}

export function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}
