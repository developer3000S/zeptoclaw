// tasks.js — вкладка «Задачи»: список, фильтры, отправка, cancel/resubmit, результат.

import { api } from "./api.js";
import { store, subscribe, esc, firstReachable } from "./store.js";
import { toast } from "./main.js";

let selectedNode = "";
let statusFilter = "";
let openTask = null;       // task_id развёрнутой строки
let taskCache = new Map(); // task_id -> полный ответ GET /tasks/{id}

export function initTasks(nodeHint) {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Задачи</h1>
    <div class="filters">
      <label class="field"><span>Узел</span><select id="t-node"></select></label>
      <label class="field"><span>Статус</span>
        <select id="t-status">
          <option value="">все</option>
          <option value="pending">pending</option>
          <option value="running">running</option>
          <option value="completed">completed</option>
          <option value="failed">failed</option>
          <option value="canceled">canceled</option>
          <option value="timeout">timeout</option>
        </select>
      </label>
    </div>

    <div class="split">
      <div class="panel">
        <h3>Последние задачи</h3>
        <div id="task-list"><p class="hint">Загрузка…</p></div>
      </div>
      <div class="panel">
        <h3>Отправить задачу</h3>
        <form id="task-form">
          <label for="tf-instruction">Instruction</label>
          <textarea id="tf-instruction" placeholder="Собрать информацию о…" required></textarea>
          <div class="form-row">
            <div><label for="tf-skills">required_skills (через запятую)</label><input id="tf-skills" placeholder="research"></div>
            <div><label for="tf-priority">priority</label><input id="tf-priority" type="number" value="5" min="1" max="10"></div>
          </div>
          <div class="form-row">
            <div><label for="tf-ttl">ttl</label><input id="tf-ttl" type="number" value="5" min="1" max="10"></div>
            <div><label for="tf-timeout">timeout_seconds</label><input id="tf-timeout" type="number" value="300" min="1"></div>
          </div>
          <label class="check"><input type="checkbox" id="tf-shell"> allow_shell</label>
          <button class="btn btn-primary" type="submit" style="margin-top:10px">Отправить</button>
          <p class="hint">Задача отправляется без <code>wait</code>: статус отслеживается поллингом этого списка.</p>
        </form>
      </div>
    </div>
  `;

  const nodeSel = document.getElementById("t-node");
  nodeSel.addEventListener("change", (e) => {
    selectedNode = e.target.value;
    loadTasks();
  });
  document.getElementById("t-status").addEventListener("change", (e) => {
    statusFilter = e.target.value;
    loadTasks();
  });
  document.getElementById("task-form").addEventListener("submit", submitTask);

  if (nodeHint) selectedNode = nodeHint;

  subscribe(renderNodeSelector);
  renderNodeSelector(store);
  if (selectedNode) {
    nodeSel.value = selectedNode;
    loadTasks();
  }
}

function renderNodeSelector(s) {
  const sel = document.getElementById("t-node");
  if (!sel) return;
  const nodes = (s.mesh?.nodes ?? []).filter((n) => n.reachable);
  const prev = selectedNode || firstReachable(s.mesh);
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.name)}">${esc(n.name)}</option>`).join("") ||
    `<option value="">нет reachable-узлов</option>`;
  if (nodes.some((n) => n.name === prev)) sel.value = prev;
  selectedNode = sel.value;
}

async function loadTasks() {
  const el = document.getElementById("task-list");
  if (!el) return;
  if (!selectedNode) {
    el.innerHTML = `<p class="hint">Нет доступного узла. Добавьте узел на вкладке «Агенты».</p>`;
    return;
  }
  try {
    const res = await api.node(selectedNode).tasks(50, statusFilter || undefined);
    renderTasks(res?.tasks ?? []);
  } catch (err) {
    el.innerHTML = `<p class="hint" style="color:var(--red)">${esc(err.message)}</p>`;
  }
}

function renderTasks(tasks) {
  const el = document.getElementById("task-list");
  if (!tasks.length) {
    el.innerHTML = `<p class="hint">Задач нет.</p>`;
    return;
  }
  el.innerHTML = `<table>
    <thead><tr><th>task_id</th><th>статус</th><th>навыки</th><th>приоритет</th><th>обновлён</th><th></th></tr></thead>
    <tbody>${tasks.map((t) => taskRow(t)).join("")}</tbody></table>`;
  for (const t of tasks) bindRow(t);
}

function taskRow(t) {
  const cls = taskStatusClass(t.status);
  const open = openTask === t.task_id;
  const detail = open
    ? `<tr><td colspan="6"><div id="task-detail">${renderDetail(taskCache.get(t.task_id))}</div></td></tr>`
    : "";
  return `<tr data-id="${esc(t.task_id)}">
    <td class="mono">${esc(t.task_id)}</td>
    <td><span class="pill ${cls}">${esc(t.status)}</span></td>
    <td>${(t.required_skills ?? []).map(esc).join(", ") || "—"}</td>
    <td>${t.priority ?? "—"}</td>
    <td class="mono">${esc(t.received_at || "—")}</td>
    <td style="white-space:nowrap">
      <button class="btn btn-sm" data-act="open">${open ? "свернуть" : "результат"}</button>
      <button class="btn btn-sm" data-act="cancel">cancel</button>
      <button class="btn btn-sm" data-act="resubmit">resubmit</button>
    </td>
  </tr>${detail}`;
}

function taskStatusClass(status) {
  const s = (status || "").toLowerCase();
  if (s === "completed") return "ok";
  if (s === "failed" || s === "timeout" || s === "canceled") return "bad";
  if (s === "running" || s === "pending" || s === "accepted") return "warn";
  return "";
}

function renderDetail(detail) {
  if (!detail) return `<p class="hint">Загрузка результата…</p>`;
  const task = detail.task || detail;
  const result = detail.result || {};
  const text = result.text ? `<h4>Результат</h4><pre class="json">${esc(result.text)}</pre>` : "";
  const arts = (result.artifacts ?? []).length
    ? `<h4>Артефакты</h4><ul>${result.artifacts.map((a) =>
        `<li class="mono">${esc(a.name)} — ${esc(a.hash)} (${a.size} байт)</li>`).join("")}</ul>` : "";
  const err = task.error || detail.error
    ? `<h4>Ошибка</h4><pre class="json">${esc(task.error || detail.error)}</pre>` : "";
  return `${err}${text}${arts}
    <dl class="kv" style="margin-top:8px">
      <dt>worker</dt><dd class="mono">${esc(task.worker_peer_id || "—")}</dd>
      <dt>error_class</dt><dd>${esc(detail.error_class || "—")}</dd>
      <dt>attempts</dt><dd>${task.attempts ?? "—"}</dd>
      <dt>finished</dt><dd class="mono">${esc(task.finished_at || "—")}</dd>
    </dl>`;
}

async function bindRow(t) {
  const row = document.querySelector(`tr[data-id="${CSS.escape(t.task_id)}"]`);
  if (!row) return;
  row.querySelector('[data-act="open"]').addEventListener("click", async () => {
    openTask = openTask === t.task_id ? null : t.task_id;
    if (openTask) {
      try {
        taskCache.set(t.task_id, await api.node(selectedNode).task(t.task_id));
      } catch (err) {
        toast("не удалось получить задачу: " + err.message, "err");
        openTask = null;
      }
    }
    loadTasks();
  });
  row.querySelector('[data-act="cancel"]').addEventListener("click", async () => {
    try {
      await api.node(selectedNode).cancelTask(t.task_id);
      toast(`задача ${t.task_id} отменена`, "ok");
      loadTasks();
    } catch (err) {
      toast("cancel: " + err.message, "err");
    }
  });
  row.querySelector('[data-act="resubmit"]').addEventListener("click", async () => {
    try {
      const res = await api.node(selectedNode).resubmitTask(t.task_id);
      const id = res?.task_id || "(id в ответе узла)";
      toast(`перезапуск: новый task_id ${id}`, "ok");
      loadTasks();
    } catch (err) {
      toast("resubmit: " + err.message, "err");
    }
  });
}

async function submitTask(e) {
  e.preventDefault();
  if (!selectedNode) { toast("выберите узел", "err"); return; }
  const skills = document.getElementById("tf-skills").value
    .split(",").map((s) => s.trim()).filter(Boolean);
  const body = {
    instruction: document.getElementById("tf-instruction").value.trim(),
    required_skills: skills.length ? skills : undefined,
    ttl: +document.getElementById("tf-ttl").value || undefined,
    priority: +document.getElementById("tf-priority").value || undefined,
    timeout_seconds: +document.getElementById("tf-timeout").value || undefined,
    allow_shell: document.getElementById("tf-shell").checked || undefined,
  };
  try {
    const res = await api.node(selectedNode).submitTask(body);
    toast(`задача отправлена: task_id ${res?.task_id ?? "—"}`, "ok");
    document.getElementById("task-form").reset();
    loadTasks();
  } catch (err) {
    toast("отправка задачи: " + err.message, "err");
  }
}
