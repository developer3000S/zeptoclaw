// api.js — обёртка над BFF API: fetch с bearer-токеном, обработка 401 и ошибок.
//
// Токен живёт в localStorage и подставляется в заголовки; при 401 вызывающему
// возвращается специальный класс ApiUnauthorized, чтобы main.js показал форму
// ввода. Сами ответы BFF — JSON, ошибки приходят в { "error": "..." }.

const TOKEN_KEY = "zeptomesh-ui-token";

export class ApiUnauthorized extends Error {
  constructor(msg) { super(msg); this.name = "ApiUnauthorized"; }
}

export function getToken() { return localStorage.getItem(TOKEN_KEY) ?? ""; }

export function setToken(tok) {
  if (tok) localStorage.setItem(TOKEN_KEY, tok);
  else localStorage.removeItem(TOKEN_KEY);
}

async function request(method, path, body) {
  const headers = {};
  if (body !== undefined && body !== null) headers["Content-Type"] = "application/json";
  const tok = getToken();
  if (tok) headers["Authorization"] = "Bearer " + tok;

  let resp;
  try {
    resp = await fetch(path, { method, headers, body: body === undefined ? null : JSON.stringify(body) });
  } catch (e) {
    // Сетевая ошибка BFF — не сервер узла, а сама панель недоступна.
    throw new Error("панель недоступна: " + e.message);
  }

  if (resp.status === 401) {
    throw new ApiUnauthorized("требуется токен доступа");
  }

  let payload = null;
  const text = await resp.text();
  if (text) {
    try { payload = JSON.parse(text); }
    catch { payload = { error: text.slice(0, 200) }; }
  }

  if (!resp.ok) {
    const msg = (payload && payload.error) || ("HTTP " + resp.status);
    throw new Error(msg);
  }
  return payload;
}

export const api = {
  get:  (p)    => request("GET",  p),
  post: (p, b) => request("POST", p, b === undefined ? null : b),
  del:  (p)    => request("DELETE", p),

  // Снимок сети — главный источник данных для всех вкладок.
  mesh: () => request("GET", "/api/v1/mesh"),

  // CRUD узлов (токены узлов не покидают бэкенд — только name/url).
  nodes:        () => request("GET", "/api/v1/nodes"),
  addNode:      (name, url, token) => request("POST", "/api/v1/nodes", { name, url, token }),
  deleteNode:   (name)             => request("DELETE", "/api/v1/nodes/" + encodeURIComponent(name)),

  // Прокси действий к узлу. Возвращает {data} или бросает Error с текстом.
  node: (name) => ({
    tasks:       (limit, status) => request("GET", `/api/v1/nodes/${encodeURIComponent(name)}/tasks`
                                  + query({ limit, status })),
    submitTask:  (body)          => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/tasks`, body),
    task:        (id)            => request("GET", `/api/v1/nodes/${encodeURIComponent(name)}/tasks/${encodeURIComponent(id)}`),
    cancelTask:  (id)            => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/tasks/${encodeURIComponent(id)}/cancel`),
    resubmitTask:(id)            => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/tasks/${encodeURIComponent(id)}/resubmit`),

    skills:      ()              => request("GET", `/api/v1/nodes/${encodeURIComponent(name)}/skills`),
    syncSkills:  ()              => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/skills/sync`),
    addSkill:    (body)          => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/skills`, body),
    deleteSkill: (skill)         => request("DELETE", `/api/v1/nodes/${encodeURIComponent(name)}/skills/${encodeURIComponent(skill)}`),

    triggers:    ()              => request("GET", `/api/v1/nodes/${encodeURIComponent(name)}/triggers`),
    addTrigger:  (body)          => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/triggers`, body),
    deleteTrigger:(id)           => request("DELETE", `/api/v1/nodes/${encodeURIComponent(name)}/triggers/${encodeURIComponent(id)}`),

    config:      ()              => request("GET", `/api/v1/nodes/${encodeURIComponent(name)}/config`),
    reloadConfig:()              => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/reload-config`),
    leave:       ()              => request("POST", `/api/v1/nodes/${encodeURIComponent(name)}/leave`),
  }),
};

function query(params) {
  const usp = new URLSearchParams();
  let has = false;
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") { usp.set(k, v); has = true; }
  }
  return has ? "?" + usp.toString() : "";
}
