// graph.js — самописный force-directed layout и отрисовка сети на SVG.
//
// Без внешних библиотек (CDL невозможен в docker-установке без интернета, а
// vendoring библиотеки эквивалентен этому коду по объёму поддержки). Физика
// простая: отталкивание всех вершин друг от друга, пружины по рёбрам,
// слабое притяжение к центру. Позиции хранятся по id вершины, поэтому при
// обновлении данных узлы не прыгают.
//
// Полный пересчёт запускается только когда меняется структура графа
// (состав вершин/рёбер) — между обновлениями позиции интерполируются.

const REPULSION = 9000;     // сила отталкивания
const SPRING = 0.045;       // жёсткость пружины рёбер
const SPRING_LEN = 110;     // целевая длина ребра
const CENTER = 0.012;       // притяжение к центру
const DAMPING = 0.82;       // затухание скорости
const MAX_VELOCITY = 12;    // ограничение скорости (стабильность)
const ITERATIONS = 220;     // тактов "прогона" при холодном старте

export class MeshGraph {
  constructor(svg, onSelect) {
    this.svg = svg;
    this.onSelect = onSelect || (() => {});
    this.nodes = new Map();   // id -> {x, y, vx, vy}
    this.structureKey = "";   // контрольная сумма структуры графа
    this.width = 600;
    this.height = 400;
    this._raf = null;
    this._bindResize();
  }

  _bindResize() {
    const ro = new ResizeObserver(() => {
      const r = this.svg.getBoundingClientRect();
      this.width = r.width || 600;
      this.height = r.height || 400;
      this._render();
    });
    ro.observe(this.svg.parentElement || this.svg);
  }

  // setData(graph, filters) — обновляет граф; возвращает true, если структура
  // изменилась и нужен прогон физики.
  setData(graph, filters = {}) {
    const vertices = (graph?.vertices ?? []).filter((v) => passFilter(v, filters));
    const visible = new Set(vertices.map((v) => v.id));
    const edges = (graph?.edges ?? []).filter((e) => visible.has(e.from) && visible.has(e.to));

    const key = structureKey(vertices, edges);
    const changed = key !== this.structureKey;

    if (changed) {
      // Удаляем пропавшие узлы, новых раскидываем по окружности.
      for (const id of [...this.nodes.keys()]) {
        if (!visible.has(id)) this.nodes.delete(id);
      }
      let i = 0;
      const cx = this.width / 2, cy = this.height / 2;
      const R = Math.min(this.width, this.height) / 3;
      for (const v of vertices) {
        if (!this.nodes.has(v.id)) {
          const a = (i / Math.max(1, vertices.length)) * Math.PI * 2;
          this.nodes.set(v.id, {
            x: cx + Math.cos(a) * R * (0.5 + Math.random() * 0.5),
            y: cy + Math.sin(a) * R * (0.5 + Math.random() * 0.5),
            vx: 0, vy: 0,
          });
        }
        i++;
      }
      this.structureKey = key;
      // Прогон физики "вхолостую" до рендера —Layout устаканится быстрее.
      for (let it = 0; it < ITERATIONS; it++) this._tick(vertices, edges);
    }
    this._vertices = vertices;
    this._edges = edges;
    this._render();
    return changed;
  }

  _tick(vertices, edges) {
    const n = vertices.length;
    const pos = new Map(vertices.map((v) => [v.id, this.nodes.get(v.id)]));

    // Отталкивание (O(n²) — для сотен узлов панели достаточно).
    for (let i = 0; i < n; i++) {
      const a = pos.get(vertices[i].id);
      for (let j = i + 1; j < n; j++) {
        const b = pos.get(vertices[j].id);
        let dx = a.x - b.x, dy = a.y - b.y;
        let d2 = dx * dx + dy * dy;
        if (d2 < 1) { dx = Math.random() - 0.5; dy = Math.random() - 0.5; d2 = 1; }
        const f = REPULSION / d2;
        const d = Math.sqrt(d2);
        const fx = (dx / d) * f, fy = (dy / d) * f;
        a.vx += fx; a.vy += fy;
        b.vx -= fx; b.vy -= fy;
      }
    }

    // Пружины по рёбрам.
    for (const e of edges) {
      const a = pos.get(e.from), b = pos.get(e.to);
      if (!a || !b) continue;
      const dx = b.x - a.x, dy = b.y - a.y;
      const d = Math.sqrt(dx * dx + dy * dy) || 1;
      const f = SPRING * (d - SPRING_LEN);
      const fx = (dx / d) * f, fy = (dy / d) * f;
      a.vx += fx; a.vy += fy;
      b.vx -= fx; b.vy -= fy;
    }

    // Притяжение к центру + интегрирование с затуханием.
    const cx = this.width / 2, cy = this.height / 2;
    const margin = 40;
    for (const v of vertices) {
      const p = pos.get(v.id);
      p.vx += (cx - p.x) * CENTER;
      p.vy += (cy - p.y) * CENTER;
      p.vx *= DAMPING; p.vy *= DAMPING;
      const sp = Math.hypot(p.vx, p.vy);
      if (sp > MAX_VELOCITY) { p.vx = (p.vx / sp) * MAX_VELOCITY; p.vy = (p.vy / sp) * MAX_VELOCITY; }
      p.x = clamp(p.x + p.vx, margin, this.width - margin);
      p.y = clamp(p.y + p.vy, margin, this.height - margin);
    }
  }

  // animate запускает плавный "до-ревер" физики на несколько тактов.
  animate() {
    if (this._raf) cancelAnimationFrame(this._raf);
    let frames = 40;
    const step = () => {
      if (!this._vertices) return;
      this._tick(this._vertices, this._edges);
      this._render();
      if (--frames > 0) this._raf = requestAnimationFrame(step);
      else this._raf = null;
    };
    this._raf = requestAnimationFrame(step);
  }

  _render() {
    const svg = this.svg;
    const NS = "http://www.w3.org/2000/svg";
    svg.setAttribute("viewBox", `0 0 ${this.width} ${this.height}`);
    while (svg.firstChild) svg.removeChild(svg.firstChild);

    // Рёбра — под вершинами.
    for (const e of this._edges ?? []) {
      const a = this.nodes.get(e.from), b = this.nodes.get(e.to);
      if (!a || !b) continue;
      const line = document.createElementNS(NS, "line");
      line.setAttribute("x1", a.x.toFixed(1)); line.setAttribute("y1", a.y.toFixed(1));
      line.setAttribute("x2", b.x.toFixed(1)); line.setAttribute("y2", b.y.toFixed(1));
      line.setAttribute("class", "graph-edge " + (e.connected ? "on" : "off"));
      svg.appendChild(line);
    }

    // Вершины.
    for (const v of this._vertices ?? []) {
      const p = this.nodes.get(v.id);
      if (!p) continue;
      const g = document.createElementNS(NS, "g");
      g.setAttribute("class", "graph-vertex" + (v.left ? " left" : ""));
      g.setAttribute("transform", `translate(${p.x.toFixed(1)},${p.y.toFixed(1)})`);
      g.addEventListener("click", () => this.onSelect(v));

      const c = document.createElementNS(NS, "circle");
      const managed = v.kind === "managed";
      c.setAttribute("r", managed ? 11 : 6);
      let color = managed ? "#3fb950" : "#7d8ea1";
      if (v.trust === "untrusted" || v.trust === "blocked") color = "#f85149";
      c.setAttribute("fill", v.left ? "transparent" : color);
      c.setAttribute("stroke", color);
      c.setAttribute("stroke-width", managed ? 2 : 1.2);
      if (v.left) c.setAttribute("stroke-dasharray", "3 3");
      if (!v.connected && !v.left) c.setAttribute("stroke-opacity", .45);
      g.appendChild(c);

      const label = document.createElementNS(NS, "text");
      label.setAttribute("x", (managed ? 15 : 10));
      label.setAttribute("y", 4);
      label.textContent = v.label || shortID(v.id);
      g.appendChild(label);

      svg.appendChild(g);
    }
  }
}

function shortID(id) {
  return id.length > 12 ? id.slice(0, 10) + "…" : id;
}

function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

// structureKey — компактная подпись состава вершин и рёбер: меняется только
// при добавлении/удалении узла или ребра, но не при сдвиге позиции.
function structureKey(vertices, edges) {
  const vs = vertices.map((v) => v.id).sort().join(",");
  const es = edges.map((e) => `${e.from}>${e.to}:${e.connected ? 1 : 0}`).sort().join(",");
  return vs + "|" + es;
}

// passFilter — фильтры вкладки «Сеть» (§5.1): по умолчанию скрыты
// untrusted/blocked и отключённые вершины — узлы знают случайные интернет-пиры
// из DHT, и без фильтра граф превращается в шум.
export function passFilter(v, f) {
  if (f.managedOnly && v.kind !== "managed") return false;
  if (f.connectedOnly && !v.connected) return false;
  const trust = f.trust || "all";
  if (trust !== "all") {
    if (trust === "trusted-known") {
      if (!["trusted", "known", "self"].includes(v.trust)) return false;
    } else if (v.trust !== trust) return false;
  }
  if (f.hideUntrusted && (v.trust === "untrusted" || v.trust === "blocked")) return false;
  if (f.hideDisconnected && !v.connected) return false;
  return true;
}
