// main.js — точка входа: роутер, auto-refresh, индикатор обновления, форма токена/401.

import { api, ApiUnauthorized, getToken, setToken } from "./api.js";
import { store, setMesh, setPollError, setPaused, subscribe } from "./store.js";
import { initNetwork } from "./network.js";
import { initAgents } from "./agents.js";
import { initTasks } from "./tasks.js";
import { initSkills } from "./skills.js";
import { initTriggers } from "./triggers.js";
import { initConfig } from "./config.js";

// ---------- boot ----------

// Загружаем токен из localStorage и сразу проверяем его на 401.
const token = getToken();
if (token) {
  api.get("/api/v1/mesh").catch((err) => {
    if (err instanceof ApiUnauthorized) setToken("")
  });
}

// Роутер: #/network, #/agents, #/tasks, #/skills, #/triggers, #/config.
const routes = {
  "#/network":   initNetwork,
  "#/agents":    initAgents,
  "#/tasks":     initTasks,
  "#/skills":    initSkills,
  "#/triggers":  initTriggers,
  "#/config":    initConfig,
};

let currentRoute = "";
let pollInterval = 5000; // ms
let pollTimer = null;
let lastPoll = 0;

// ---------- init ----------

window.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initTokenModal();
  initRefresh();
  route();
  window.addEventListener("hashchange", route);
  subscribe(renderRefresh);
  renderRefresh(store);
});

function initTabs() {
  const tabs = document.getElementById("tabs");
  if (!tabs) return;
  tabs.innerHTML = Object.keys(routes).map((path) => {
    const label = path.slice(2);
    return `<button class="tab" data-path="${path}">${label}</button>`;
  }).join("");
  tabs.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      location.hash = btn.dataset.path;
    });
  });
}

function initTokenModal() {
  const modal = document.getElementById("token-modal");
  const input = document.getElementById("token-input");
  const saveBtn = document.getElementById("token-save");
  const clearBtn = document.getElementById("token-clear");
  const closeBtn = document.getElementById("token-close");
  const tokenBtn = document.getElementById("token-btn");

  if (token) input.value = token;

  function show() {
    modal.classList.remove("hidden");
    input.focus();
  }
  function hide() { modal.classList.add("hidden"); }

  tokenBtn.addEventListener("click", show);
  closeBtn.addEventListener("click", hide);
  saveBtn.addEventListener("click", () => {
    const tok = input.value.trim();
    setToken(tok);
    hide();
    // Перезагружаем снимок сети, чтобы проверить токен.
    poll();
  });
  clearBtn.addEventListener("click", () => {
    setToken("");
    hide();
    poll();
  });
}

function initRefresh() {
  const pauseBtn = document.getElementById("pause-btn");
  pauseBtn.addEventListener("click", () => {
    setPaused(!store.paused);
    if (!store.paused) poll();
  });
}

// ---------- роутинг ----------

function route() {
  const hash = location.hash || "#/network";
  if (hash === currentRoute) return;
  currentRoute = hash;
  const init = routes[hash];
  if (!init) {
    location.hash = "#/network";
    return;
  }
  // #/tasks/zepto-3 → передаём узел в initTasks.
  const nodeHint = hash.startsWith("#/tasks/") ? hash.slice(8) : "";
  init(nodeHint);
  updateActiveTab();
}

function updateActiveTab() {
  const tabs = document.getElementById("tabs");
  if (!tabs) return;
  tabs.querySelectorAll(".tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.path === currentRoute);
  });
}

// ---------- поллинг ----------

function poll() {
  if (pollTimer) clearTimeout(pollTimer);
  if (store.paused) return;
  const now = Date.now();
  if (now - lastPoll < pollInterval) {
    pollTimer = setTimeout(poll, pollInterval - (now - lastPoll));
    return;
  }
  lastPoll = now;
  api.mesh().then((mesh) => {
    setMesh(mesh);
    pollInterval = mesh.poll_interval_ms || 5000;
  }).catch((err) => {
    setPollError(err);
    if (err instanceof ApiUnauthorized) {
      document.getElementById("token-modal").classList.remove("hidden");
    }
  }).finally(() => {
    pollTimer = setTimeout(poll, pollInterval);
  });
}

// ---------- рендер ----------

function renderRefresh(s) {
  const el = document.getElementById("refresh-indicator");
  if (!el) return;
  if (s.paused) {
    el.textContent = "пауза";
    el.className = "refresh";
    return;
  }
  if (s.error) {
    el.textContent = s.error.message;
    el.className = "refresh err";
    return;
  }
  if (!s.mesh) {
    el.textContent = "загрузка…";
    el.className = "refresh";
    return;
  }
  const now = Math.floor(Date.now() / 1000);
  const age = now - s.mesh.updated_unix;
  el.textContent = fmtDuration(age);
  el.className = age > 15 ? "refresh stale" : "refresh";
}

function fmtDuration(sec) {
  if (sec < 60) return `${sec}с`;
  const m = Math.floor(sec / 60);
  if (m < 60) return `${m}м`;
  const h = Math.floor(m / 60);
  return `${h}ч`;
}

// ---------- toast ----------

let toastTimer = null;
export function toast(msg, kind) {
  const el = document.getElementById("toast");
  if (!el) return;
  if (toastTimer) clearTimeout(toastTimer);
  el.textContent = msg;
  el.className = "toast" + (kind === "err" ? " err" : kind === "ok" ? " ok" : "");
  toastTimer = setTimeout(() => el.classList.add("hidden"), 5000);
}

// ---------- init ----------

poll();
