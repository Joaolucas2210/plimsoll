// Plimsoll console (ADR 0135): browse what the signed-in user may read
// and run SQL, through the same /v1/sql as any client. No framework, no
// build step; every value is written with textContent, never as HTML.
"use strict";

const MAX_ROWS = 1000; // shown; reading stops there, which cancels the query
const $ = (id) => document.getElementById(id);
const store = {
  get: (k) => { try { return sessionStorage.getItem(k) || ""; } catch { return ""; } },
  set: (k, v) => { try { sessionStorage.setItem(k, v); } catch { /* private mode */ } },
  clear: () => { try { sessionStorage.clear(); } catch { /* private mode */ } },
};
let running = null; // { controller, queryId }

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) node.append(c);
  return node;
}

function quote(name) {
  return '"' + String(name).replaceAll('"', '""') + '"';
}

/** POST /v1/sql; resolves to the Response, or throws the API's error. */
async function post(sql, { params = [], format = "json", signal } = {}) {
  const body = { sql, format };
  if (params.length) body.params = params;
  const user = store.get("user");
  if (user) body.user = user;
  const res = await fetch("/v1/sql", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer " + store.get("token") },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok) {
    let message = res.status + " " + res.statusText;
    try {
      const e = (await res.json()).error;
      message = `${e.code}: ${e.message}`;
    } catch { /* not JSON */ }
    const err = new Error(message);
    err.status = res.status;
    throw err;
  }
  return res;
}

async function rows(sql, params) {
  return (await (await post(sql, { params })).json()).rows;
}

// ---- sign in -------------------------------------------------------------

function show(signedIn) {
  $("login").hidden = signedIn;
  $("app").hidden = !signedIn;
  $("logout").hidden = !signedIn;
  $("who").textContent = signedIn ? store.get("user") || "signed in" : "";
}

$("login").addEventListener("submit", async (e) => {
  e.preventDefault();
  store.set("token", $("token").value.trim());
  store.set("user", $("user").value.trim());
  $("login-error").textContent = "";
  try {
    await rows("SELECT 1");
    show(true);
    loadTree();
  } catch (err) {
    store.clear();
    $("login-error").textContent = err.status === 401 ? "Invalid token." : err.message;
  }
});

$("logout").addEventListener("click", () => {
  store.clear();
  $("token").value = "";
  show(false);
});

// ---- catalog tree ----------------------------------------------------------

async function loadTree() {
  const tree = $("tree");
  tree.replaceChildren(el("li", { class: "muted" }, "Loading…"));
  let tables;
  try {
    tables = await rows(
      "SELECT table_catalog, table_schema, table_name, table_type " +
        "FROM information_schema.tables ORDER BY 1, 2, 3",
    );
  } catch (err) {
    tree.replaceChildren(el("li", { class: "error" }, err.message));
    return;
  }
  const grouped = new Map();
  for (const [catalog, schema, table, kind] of tables) {
    const key = catalog + "\u0000" + schema;
    if (!grouped.has(key)) grouped.set(key, { catalog, schema, tables: [] });
    grouped.get(key).tables.push({ table, kind });
  }
  tree.replaceChildren();
  for (const { catalog, schema, tables } of grouped.values()) {
    const list = el("ul", { role: "group" });
    for (const { table, kind } of tables) list.append(tableNode(catalog, schema, table, kind));
    const open = schema !== "information_schema";
    const item = el("li", { role: "treeitem", "aria-expanded": String(open) });
    list.hidden = !open;
    item.append(
      el("button", {
        class: "node",
        title: `${catalog}.${schema}`,
        onclick: () => {
          list.hidden = !list.hidden;
          item.setAttribute("aria-expanded", String(!list.hidden));
        },
      }, `▸ ${schema} `, el("span", { class: "kind" }, catalog)),
      list,
    );
    tree.append(item);
  }
  filterTree();
}

function tableNode(catalog, schema, table, kind) {
  const columns = el("ul", { role: "group" });
  columns.hidden = true;
  const item = el("li", { role: "treeitem", "data-name": table.toLowerCase() });
  const name = (catalog === "datafusion" ? "" : quote(catalog) + ".") + quote(schema) + "." + quote(table);
  item.append(
    el("button", {
      class: "node",
      title: "Click: columns · Double-click: query it",
      onclick: () => toggleColumns(columns, catalog, schema, table),
      ondblclick: () => {
        $("sql").value = `SELECT * FROM ${name} LIMIT 100`;
        run();
      },
    }, `${table} `, el("span", { class: "kind" }, kind === "VIEW" ? "view" : "")),
    columns,
  );
  return item;
}

async function toggleColumns(list, catalog, schema, table) {
  list.hidden = !list.hidden;
  if (list.hidden || list.dataset.loaded) return;
  list.replaceChildren(el("li", { class: "col" }, "…"));
  try {
    const cols = await rows(
      "SELECT column_name, data_type, is_nullable FROM information_schema.columns " +
        "WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3 ORDER BY ordinal_position",
      [catalog, schema, table],
    );
    list.replaceChildren(
      ...cols.map(([c, t, n]) => el("li", { class: "col", title: t }, `${c}  ${t}${n === "YES" ? "" : " not null"}`)),
    );
    list.dataset.loaded = "1";
  } catch (err) {
    list.replaceChildren(el("li", { class: "col error" }, err.message));
  }
}

function filterTree() {
  const q = $("filter").value.trim().toLowerCase();
  for (const item of document.querySelectorAll("#tree li[data-name]")) {
    item.hidden = q !== "" && !item.dataset.name.includes(q);
  }
}

$("filter").addEventListener("input", filterTree);
$("refresh").addEventListener("click", loadTree);

// ---- running SQL -----------------------------------------------------------

function cell(v) {
  if (v === null) return el("td", { class: "null" }, "NULL");
  return el("td", {}, typeof v === "object" ? JSON.stringify(v) : String(v));
}

async function run() {
  const sql = $("sql").value.trim();
  if (!sql || running) return;
  const controller = new AbortController();
  running = { controller, queryId: null };
  $("run").disabled = true;
  $("cancel").disabled = false;
  $("status").textContent = "Running…";
  const out = $("out");
  out.replaceChildren();
  const started = performance.now();
  let shown = 0;
  let truncated = false;
  try {
    const res = await post(sql, { format: "ndjson", signal: controller.signal });
    const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
    const body = el("tbody");
    let buffer = "";
    let done = null;
    read: for (;;) {
      const { value, done: end } = await reader.read();
      if (end) break;
      buffer += value;
      let nl;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const line = JSON.parse(buffer.slice(0, nl));
        buffer = buffer.slice(nl + 1);
        if (line.columns) {
          running.queryId = line.query_id;
          const head = el("tr", {}, ...line.columns.map((c) => el("th", { scope: "col" }, c)));
          out.append(el("table", {}, el("thead", {}, head), body));
        } else if (line.error) {
          throw new Error(`${line.error.code}: ${line.error.message}`);
        } else if (line.done) {
          done = line;
        } else if (shown < MAX_ROWS) {
          body.append(el("tr", {}, ...line.map(cell)));
          shown += 1;
        } else {
          truncated = true;
          controller.abort(); // closing the stream cancels the query
          break read;
        }
      }
    }
    const ms = Math.round(performance.now() - started);
    const id = running.queryId ? ` · query ${running.queryId}` : "";
    $("status").textContent = truncated
      ? `First ${MAX_ROWS} rows shown (query stopped) · ${ms} ms${id}`
      : `${done ? done.rows : shown} rows · ${ms} ms${id}`;
    if (!out.firstChild) out.append(el("p", { class: "muted" }, "No result set."));
  } catch (err) {
    if (err.name === "AbortError" && !truncated) {
      $("status").textContent = "Cancelled";
    } else if (!truncated) {
      $("status").textContent = "Failed";
      out.replaceChildren(el("p", { class: "error" }, err.message));
    }
  } finally {
    running = null;
    $("run").disabled = false;
    $("cancel").disabled = true;
  }
}

async function cancel() {
  if (!running) return;
  const { controller, queryId } = running;
  if (queryId) {
    // Ends it on the server too, even if the stream is mid-batch.
    await fetch(`/v1/queries/${queryId}`, {
      method: "DELETE",
      headers: { authorization: "Bearer " + store.get("token") },
    }).catch(() => {});
  }
  controller.abort();
}

$("run").addEventListener("click", run);
$("cancel").addEventListener("click", cancel);
$("sql").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
    e.preventDefault();
    run();
  }
});

// ---- start -------------------------------------------------------------------

if (store.get("token") || store.get("user")) {
  show(true);
  loadTree();
} else {
  show(false);
}
