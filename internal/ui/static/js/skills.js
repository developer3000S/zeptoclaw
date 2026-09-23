// skills.js — вкладка «Навыки»: просмотр, синхронизация и документирование навыков узла.

import { api } from "./api.js";
import { store, subscribe, esc, firstReachable } from "./store.js";
import { toast } from "./main.js";

let selectedNode = "";

export function initSkills() {
  const view = document.getElementById("view");
  view.innerHTML = `
    <h1>Навыки</h1>
    <div class="filters">
      <label class="field"><span>Узел</span><select id="s-node"></select></label>
      <button class="btn btn-sm" id="s-sync">skills-sync</button>
    </div>
    <div class="split">
      <div class="panel"><h3>Навыки узла</h3><div id="skills-list"><p class="hint">Загрузка…</p></div></div>
      <div class="panel">
        <h3>Документировать навык</h3>
        <form id="skill-form">
          <label for="sf-name">name</label><input id="sf-name" placeholder="research" required>
          <label for="sf-desc">description</label><textarea id="sf-desc" placeholder="Сбор и анализ информации"></textarea>
          <label for="sf-models">models (через запятую)</label><input id="sf-models" placeholder="gpt-4, claude-sonnet">
          <label for="sf-attrs">attributes (через запятую)</label><input id="sf-attrs" placeholder="needs-network">
          <button class="btn btn-primary" type="submit" style="margin-top:10px">Сохранить</button>
        </form>
      </div>
    </div>
  `;

  const nodeSel = document.getElementById("s-node");
  nodeSel.addEventListener("change", (e) => { selectedNode = e.target.value; loadSkills(); });
  document.getElementById("s-sync").addEventListener("click", syncSkills);
  document.getElementById("skill-form").addEventListener("submit", addSkillDoc);

  subscribe(renderNodeSelector);
  renderNodeSelector(store);
  if (selectedNode) { nodeSel.value = selectedNode; loadSkills(); }
}

function renderNodeSelector(s) {
  const sel = document.getElementById("s-node");
  if (!sel) return;
  const nodes = (s.mesh?.nodes ?? []).filter((n) => n.reachable);
  const prev = selectedNode || firstReachable(s.mesh);
  sel.innerHTML = nodes.map((n) =>
    `<option value="${esc(n.name)}">${esc(n.name)}</option>`).join("") ||
    `<option value="">нет reachable-узлов</option>`;
  if (nodes.some((n) => n.name === prev)) sel.value = prev;
  selectedNode = sel.value;
}

async function loadSkills() {
  const el = document.getElementById("skills-list");
  if (!el) return;
  if (!selectedNode) { el.innerHTML = `<p class="hint">Нет доступного узла.</p>`; return; }
  try {
    const res = await api.node(selectedNode).skills();
    renderSkills(res);
  } catch (err) {
    el.innerHTML = `<p class="hint" style="color:var(--red)">${esc(err.message)}</p>`;
  }
}

function renderSkills(res) {
  const el = document.getElementById("skills-list");
  const advertised = res?.advertised ?? res?.skills ?? [];
  const descriptors = res?.descriptors ?? [];
  const peerViews = res?.peer_skills ?? res?.peer_skill_views ?? [];
  const epoch = res?.epoch ?? res?.skills_epoch;

  const adv = advertised.length
    ? `<p class="hint">advertised: ${advertised.map((s) => `<span class="pill accent">${esc(s)}</span>`).join(" ")}</p>`
    : `<p class="hint">advertised: —</p>`;

  const desc = descriptors.length
    ? `<h4>Документированные</h4><table>
        <thead><tr><th>name</th><th>description</th><th>models</th><th></th></tr></thead>
        <tbody>${descriptors.map((d) => `<tr>
          <td class="mono">${esc(d.name)}</td>
          <td>${esc(d.description || "—")}</td>
          <td class="mono">${(d.models ?? []).map(esc).join(", ") || "—"}</td>
          <td><button class="btn btn-sm btn-danger" data-del="${esc(d.name)}">удалить</button></td>
        </tr>`).join("")}</tbody></table>`
    : `<p class="hint">Документированных навыков нет.</p>`;

  const peers = peerViews.length
    ? `<h4>Навыки пиров</h4><table>
        <thead><tr><th>peer</th><th>skills</th></tr></thead>
        <tbody>${peerViews.map((p) => `<tr>
          <td class="mono">${esc(p.peer_id || p.peer || "—")}</td>
          <td>${(p.skills ?? []).map((s) => `<span class="pill">${esc(s)}</span>`).join(" ") || "—"}</td>
        </tr>`).join("")}</tbody></table>`
    : "";

  el.innerHTML = `${adv}
    <p class="hint">epoch: ${esc(String(epoch ?? "—"))} · peers_queried: ${esc(String(res?.peers_queried ?? "—"))}</p>
    ${desc}${peers}`;

  el.querySelectorAll("[data-del]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const skill = btn.dataset.del;
      if (!confirm(`Удалить документацию навыка «${skill}» у узла «${selectedNode}»?`)) return;
      try {
        await api.node(selectedNode).deleteSkill(skill);
        toast(`документация навыка «${skill}» удалена`, "ok");
        loadSkills();
      } catch (err) {
        toast("удаление навыка: " + err.message, "err");
      }
    });
  });
}

async function syncSkills() {
  try {
    const res = await api.node(selectedNode).syncSkills();
    toast(`skills-sync: peers_queried=${res?.peers_queried ?? "?"}`, "ok");
    loadSkills();
  } catch (err) {
    toast("skills-sync: " + err.message, "err");
  }
}

async function addSkillDoc(e) {
  e.preventDefault();
  if (!selectedNode) { toast("выберите узел", "err"); return; }
  const body = {
    name: document.getElementById("sf-name").value.trim(),
    description: document.getElementById("sf-desc").value.trim() || undefined,
    models: document.getElementById("sf-models").value.split(",").map((s) => s.trim()).filter(Boolean),
    attributes: document.getElementById("sf-attrs").value.split(",").map((s) => s.trim()).filter(Boolean),
  };
  try {
    await api.node(selectedNode).addSkill(body);
    toast(`навык «${body.name}» задокументирован`, "ok");
    e.target.reset();
    loadSkills();
  } catch (err) {
    toast("сохранение навыка: " + err.message, "err");
  }
}
