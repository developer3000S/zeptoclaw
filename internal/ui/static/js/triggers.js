// triggers.js — вкладка «Триггеры»: список, создание и удаление триггеров узла.

import { api } from "./api.js";
import { store, subscribe, esc, firstReachable } from "./store.js";
import { toast } from "./main.js";

let selectedNode = "";

export function initTriggers() {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Триггеры</h1>
    <div class="filters">
      <label class="field"><span>Узел</span><select id="tr-node"></select></label>
    </div>
    <div class="split">
      <div class="panel">
        <h3>Триггеры узла</h3>
        <div id="trigger-list"><p class="hint">Загрузка…</p></div>
      </div>
      <div class="panel">
        <h3>Создать триггер</h3>
        <form id="trigger-form">
          <label for="trf-id">id</label><input id="trf-id" placeholder="daily-report" required>
          <label for="trf-cron">schedule (cron)</label><input id="trf-cron" placeholder="0 */6 * * *" required>
          <p class="hint">Команда задания:</p>
          <label for="trf-instruction">instruction</label>
          <textarea id="trf-instruction" placeholder="Собрать сводку статуса узлов" required></textarea>
          <div class="form-row">
            <div><label for="trf-skills">required_skills</label><input id="trf-skills" placeholder="research"></div>
            <div><label for="trf-priority">priority</label><input id="trf-priority" type="number" value="5" min="1" max="10"></div>
          </div>
          <div class="form-row">
            <div><label for="trf-ttl">ttl</label><input id="trf-ttl" type="number" value="5" min="1" max="10"></div>
            <div><label for="trf-enabled" class="check" style="margin-top:22px"><input type="checkbox" id="trf-enabled" checked> enabled</label></div>
          </div>
          <button class="btn btn-primary" type="submit" style="margin-top:10px">Создать</button>
        </form>
      </div>
    </div>
  `;

  const nodeSel = document.getElementById("tr-node");
  nodeSel.addEventListener("change", (e) => { selectedNode = e.target.value; loadTriggers(); });
  document.getElementById("trigger-form").addEventListener("submit", addTrigger);

  subscribe(renderNodeSelector);
  renderNodeSelector(store);
  if (selectedNode) { nodeSel.value = selectedNode; loadTriggers(); }
}

function renderNodeSelector(s) {
  const sel = document.getElementById("tr-node");
  if (!sel) return;
  const nodes = (s.mesh?.nodes ?? []).filter((n) => n.reachable);
  const prev = selectedNode || firstReachable(s.mesh);
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.name)}">${esc(n.name)}</option>`).join("") ||
    `<option value="">нет reachable-узлов</option>`;
  if (nodes.some((n) => n.name === prev)) sel.value = prev;
  selectedNode = sel.value;
}

async function loadTriggers() {
  const el = document.getElementById("trigger-list");
  if (!el) return;
  if (!selectedNode) { el.innerHTML = `<p class="hint">Нет доступного узла.</p>`; return; }
  try {
    const res = await api.node(selectedNode).triggers();
    renderTriggers(res?.triggers ?? res ?? []);
  } catch (err) {
    el.innerHTML = `<p class="hint" style="color:var(--red)">${esc(err.message)}</p>`;
  }
}

function renderTriggers(triggers) {
  const el = document.getElementById("trigger-list");
  if (!Array.isArray(triggers) || !triggers.length) {
    el.innerHTML = `<p class="hint">Триггеров нет.</p>`;
    return;
  }
  el.innerHTML = `<table>
    <thead><tr><th>id</th><th>schedule</th><th>next_fire</th><th>run_count</th><th>enabled</th><th>последняя ошибка</th><th></th></tr></thead>
    <tbody>${triggers.map((t) => `<tr>
      <td class="mono">${esc(t.id)}</td>
      <td class="mono">${esc(t.schedule)}</td>
      <td class="mono">${esc(t.next_fire || "—")}</td>
      <td>${t.run_count ?? "—"}</td>
      <td>${t.enabled ? '<span class="pill ok">да</span>' : '<span class="pill">нет</span>'}</td>
      <td class="mono" style="color:var(--red)">${esc(t.last_error || "—")}</td>
      <td><button class="btn btn-sm btn-danger" data-del="${esc(t.id)}">удалить</button></td>
    </tr>`).join("")}</tbody></table>`;

  el.querySelectorAll("[data-del]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.del;
      if (!confirm(`Удалить триггер «${id}» у узла «${selectedNode}»?`)) return;
      try {
        await api.node(selectedNode).deleteTrigger(id);
        toast(`триггер «${id}» удалён`, "ok");
        loadTriggers();
      } catch (err) {
        toast("удаление триггера: " + err.message, "err");
      }
    });
  });
}

async function addTrigger(e) {
  e.preventDefault();
  if (!selectedNode) { toast("выберите узел", "err"); return; }
  const skills = document.getElementById("trf-skills").value
    .split(",").map((s) => s.trim()).filter(Boolean);
  const body = {
    id: document.getElementById("trf-id").value.trim(),
    schedule: document.getElementById("trf-cron").value.trim(),
    enabled: document.getElementById("trf-enabled").checked,
    job: {
      instruction: document.getElementById("trf-instruction").value.trim(),
      required_skills: skills.length ? skills : undefined,
      ttl: +document.getElementById("trf-ttl").value || undefined,
      priority: +document.getElementById("trf-priority").value || undefined,
    },
  };
  try {
    await api.node(selectedNode).addTrigger(body);
    toast(`триггер «${body.id}» создан`, "ok");
    e.target.reset();
    document.getElementById("trf-enabled").checked = true;
    loadTriggers();
  } catch (err) {
    toast("создание триггера: " + err.message, "err");
  }
}
