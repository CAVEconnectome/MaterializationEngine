// Admin page, organized by datastack. Shows each user only the sections their permissions
// allow (from /materialize/admin/api/capabilities); every action still goes through an
// endpoint that checks its own permission. Actions that can offer a dry run first.
(function () {
  "use strict";

  const API = {
    capabilities: "/materialize/admin/api/capabilities",
    databases: "/materialize/admin/api/databases",
    repackJobs: "/materialize/admin/api/repack/jobs",
    versions: (ds) => `/materialize/admin/api/datastack/${encodeURIComponent(ds)}/versions`,
    annotationTables: (ds, v) => `/materialize/admin/api/datastack/${encodeURIComponent(ds)}/version/${encodeURIComponent(v)}/annotation_tables`,
    virtualTargets: (ds) => `/materialize/admin/api/datastack/${encodeURIComponent(ds)}/virtual_targets`,
    virtualTarget: (ds, name) => `/materialize/admin/api/datastack/${encodeURIComponent(ds)}/virtual_target/${encodeURIComponent(name)}`,
    versionTables: (ds, v) => `/materialize/admin/api/datastack/${encodeURIComponent(ds)}/version/${encodeURIComponent(v)}/tables`,
    queues: "/materialize/admin/api/queues",
    tableOrder: (db) => `/materialize/api/v2/maintenance/table_order/${encodeURIComponent(db)}`,
    repack: (db, table) => `/materialize/api/v2/maintenance/repack/${encodeURIComponent(db)}/${encodeURIComponent(table)}`,
    run: (path) => `/materialize/api/v2/materialize/run/${path}`,
    active: "/materialize/api/v2/workflow/status/active",
    locks: "/materialize/api/v2/workflow/status/locks",
    workers: "/materialize/api/v2/celery/status/info",
    uploadJobs: "/materialize/upload/api/process/user-jobs",
    bulkCleanup: "/materialize/upload/api/admin/jobs/cleanup",
    jobCleanup: (id) => `/materialize/upload/api/admin/jobs/${encodeURIComponent(id)}/cleanup`,
  };

  // Superadmin workflows (materialize blueprint, auth_requires_admin). `path` builds the route
  // under /materialize/run/; params become query arguments unless inPath.
  const WORKFLOWS = [
    { id: "update_database", title: "Update live database",
      text: "Ingest new annotations and update expired root IDs, as the hourly schedule does.",
      path: (ds) => `update_database/datastack/${ds}` },
    { id: "ingest", title: "Ingest new annotations",
      text: "Look up supervoxel and root IDs for annotations added since the last run, in every table.",
      path: (ds) => `ingest_annotations/datastack/${ds}` },
    { id: "update_roots", title: "Update expired root IDs",
      text: "Replace root IDs that proofreading has made out of date.",
      path: (ds) => `update_roots/datastack/${ds}`,
      params: [{ name: "lookup_all_root_ids", label: "Look up all root IDs, not only expired ones", type: "checkbox" }] },
    { id: "lookup_root_ids", title: "Look up missing root IDs",
      text: "Find rows with no root ID in every table and look them up.",
      path: (ds) => `lookup_root_ids/datastack/${ds}` },
    { id: "sparse", title: "Look up NULL root IDs in one table",
      text: "Find rows whose root IDs are NULL in one table and look them up.",
      path: (ds, v) => `sparse_lookup_root_ids/datastack/${ds}/table/${encodeURIComponent(v.table_name)}`,
      params: [{ name: "table_name", label: "Table", type: "text", required: true, inPath: true }] },
    { id: "create_frozen", title: "Create frozen version",
      text: "Create a new materialized version from the live database.",
      path: (ds) => `create_frozen/datastack/${ds}`,
      params: [{ name: "days_to_expire", label: "Days to expire", type: "number", value: 2, required: true },
               { name: "merge_tables", label: "Merge tables", type: "checkbox" }] },
    { id: "complete", title: "Complete workflow",
      text: "Update the live database, then create a frozen version: the full scheduled materialization.",
      path: (ds) => `complete_workflow/datastack/${ds}`,
      params: [{ name: "days_to_expire", label: "Days to expire", type: "number", value: 2, required: true },
               { name: "merge_tables", label: "Merge tables", type: "checkbox" }] },
  ];
  const ACTIVE_REPACK = new Set(["queued", "checking", "running"]);
  const $ = (id) => document.getElementById(id);

  let caps = { superadmin: false, datastacks: [] };
  let current = null; // the selected datastack's capability entry

  function esc(value) {
    return String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }

  async function call(url, { method = "GET", query = null, body = null } = {}) {
    if (query) {
      const params = new URLSearchParams();
      Object.entries(query).forEach(([k, v]) => { if (v !== null && v !== undefined && v !== "") params.set(k, v); });
      url += (url.includes("?") ? "&" : "?") + params.toString();
    }
    const options = { method, headers: {} };
    // Always send a JSON body on writes: flask-restx parsers also read request.json.
    if (body || method !== "GET") { options.headers["Content-Type"] = "application/json"; options.body = JSON.stringify(body || {}); }
    const resp = await fetch(url, options);
    const text = await resp.text();
    let data;
    try { data = JSON.parse(text); } catch (e) { data = { message: text.slice(0, 300) }; }
    if (!resp.ok && resp.status !== 409) throw new Error(data.message || data.reason || `HTTP ${resp.status}`);
    return data;
  }

  const alertBox = (kind, html) => `<div class="alert alert-${kind} py-2 small">${html}</div>`;

  function corrCell(corr) {
    if (corr === null || corr === undefined) return '<span class="muted">not analyzed</span>';
    const a = Math.abs(corr);
    const cls = a >= 0.9 ? "corr-good" : a >= 0.5 ? "corr-mid" : "corr-bad";
    return `<span class="${cls}">${corr.toFixed(3)}</span>`;
  }

  // ---------------------------------------------------------------- datastacks & sections
  async function loadCapabilities() {
    try { caps = await call(API.capabilities); } catch (e) { $("ds-select").innerHTML = `<option>${esc(e.message)}</option>`; return; }
    if (caps.superadmin) $("deployment-tab-item").classList.remove("d-none");
    if (!caps.datastacks.length) { $("ds-select").innerHTML = "<option>No datastacks you administer</option>"; return; }
    $("ds-select").innerHTML = caps.datastacks.map((d) => `<option value="${esc(d.name)}">${esc(d.name)}</option>`).join("");
    selectDatastack();
  }

  function selectDatastack() {
    current = caps.datastacks.find((d) => d.name === $("ds-select").value);
    if (!current) return;
    const rights = caps.superadmin ? "superadmin" : [current.admin && "dataset admin", current.edit && "edit"].filter(Boolean).join(", ");
    $("ds-rights").textContent = `Auth dataset ${current.dataset || "?"} · your rights: ${rights}`;
    const allowed = { superadmin: caps.superadmin, admin: current.admin, edit: current.edit };
    let any = false;
    document.querySelectorAll("[data-needs]").forEach((el) => {
      const show = !!allowed[el.dataset.needs];
      el.classList.toggle("d-none", !show);
      any = any || show;
    });
    $("no-actions").classList.toggle("d-none", any);
    if (current.admin) { loadVersions(); loadVvTargets(); }
    if (caps.superadmin) { loadDatabases(); loadUploads(); }
  }

  // ---------------------------------------------------------------- workflows (superadmin)
  function renderWorkflows() {
    $("workflow-cards").innerHTML = WORKFLOWS.map((w) => `
      <div class="col-md-6"><div class="card h-100"><div class="card-body">
        <h6 class="card-title">${esc(w.title)}</h6>
        <p class="card-text small">${esc(w.text)}</p>
        ${(w.params || []).map((p) => p.type === "checkbox"
          ? `<div class="form-check mb-2"><input class="form-check-input" type="checkbox" id="wf-${w.id}-${p.name}">
               <label class="form-check-label small" for="wf-${w.id}-${p.name}">${esc(p.label)}</label></div>`
          : `<div class="mb-2"><label class="form-label small" for="wf-${w.id}-${p.name}">${esc(p.label)}</label>
               <input class="form-control form-control-sm" type="${p.type}" id="wf-${w.id}-${p.name}" value="${esc(p.value ?? "")}"></div>`).join("")}
        <button class="btn btn-sm btn-outline-danger" data-workflow="${w.id}">Start</button>
        <div class="mt-2" id="wf-${w.id}-result"></div>
      </div></div></div>`).join("");
    $("workflow-cards").querySelectorAll("[data-workflow]").forEach((b) =>
      b.addEventListener("click", () => startWorkflow(WORKFLOWS.find((w) => w.id === b.dataset.workflow))));
  }

  async function post(resultId, url, { query, body, summary }) {
    if (!confirm(summary)) return;
    const result = $(resultId);
    result.innerHTML = '<span class="muted small">Starting…</span>';
    try {
      const data = await call(url, { method: "POST", query, body });
      result.innerHTML = alertBox("success", `Started. ${esc(JSON.stringify(data)).slice(0, 400)}`);
    } catch (e) {
      result.innerHTML = alertBox("danger", esc(e.message));
    }
  }

  function startWorkflow(w) {
    const ds = current.name, values = {}, query = {};
    for (const p of w.params || []) {
      const el = $(`wf-${w.id}-${p.name}`);
      const v = p.type === "checkbox" ? el.checked : el.value.trim();
      if (p.required && v === "") { alert(`${p.label} is required`); return; }
      values[p.name] = v;
      if (!p.inPath) query[p.name] = v;
    }
    const detail = Object.entries(values).map(([k, v]) => `${k}=${v}`).join(", ");
    post(`wf-${w.id}-result`, API.run(w.path(encodeURIComponent(ds), values)),
      { query, summary: `Start "${w.title}" for ${ds}?${detail ? `\n\n${detail}` : ""}` });
  }

  // ---------------------------------------------------------------- tables (edit)
  function ingestTable() {
    const table = $("ingest-table").value.trim();
    if (!table) { alert("Table is required"); return; }
    post("ingest-table-result", API.run(`ingest_annotations/datastack/${encodeURIComponent(current.name)}/${encodeURIComponent(table)}`),
      { summary: `Ingest new annotations in ${current.name}.${table}?` });
  }

  // ---------------------------------------------------------------- versions (dataset admin)
  // Virtual versions: every input is chosen from what exists. Tables can also be pasted as a
  // comma separated list, which is checked against the version's tables.
  const NEW_DATASTACK = "__new__";
  let vvTables = [], vvTargets = [], vvStatus = null, vvStatusTimer = null;

  const parseTableList = (text) => [...new Set(text.split(/[\s,]+/).map((t) => t.trim()).filter(Boolean))];

  function regexFrom(input) {
    const pattern = input.value.trim();
    input.classList.remove("is-invalid");
    if (!pattern) return null;
    try { return new RegExp(pattern, "i"); } catch (e) { input.classList.add("is-invalid"); return undefined; }
  }

  async function loadVvTables() {
    const version = $("vv-target").value;
    vvTables = [];
    if (version) {
      $("vv-tables-check").textContent = "Loading tables…";
      try { vvTables = (await call(API.annotationTables(current.name, version))).tables.map((t) => t.name); }
      catch (e) { $("vv-tables-check").textContent = `Could not load tables: ${e.message}`; }
    }
    renderVvTables();
    renderTargetStatus();
  }

  function renderVvTables() {
    const re = regexFrom($("vv-filter"));
    if (re === undefined) return;
    const chosen = new Set(parseTableList($("vv-tables").value));
    $("vv-table-list").innerHTML = vvTables.filter((t) => !re || re.test(t))
      .map((t) => `<option value="${esc(t)}"${chosen.has(t) ? " selected" : ""}>${esc(t)}</option>`).join("");
    checkVvTables();
  }

  // The list's selection, merged with pasted names that the filter hides.
  function vvListChanged() {
    const shown = new Set([...$("vv-table-list").options].map((o) => o.value));
    const kept = parseTableList($("vv-tables").value).filter((t) => !shown.has(t));
    const picked = [...$("vv-table-list").selectedOptions].map((o) => o.value);
    $("vv-tables").value = [...kept, ...picked].join(", ");
    checkVvTables();
  }

  function checkVvTables() {
    const names = parseTableList($("vv-tables").value), known = new Set(vvTables);
    const unknown = names.filter((t) => !known.has(t));
    [...$("vv-table-list").options].forEach((o) => { o.selected = names.includes(o.value); });
    $("vv-tables").classList.toggle("is-invalid", unknown.length > 0);
    $("vv-tables-check").innerHTML = !vvTables.length ? "" : unknown.length
      ? `<span class="text-danger">Not in v${esc($("vv-target").value)}: ${unknown.map((t) => `<code>${esc(t)}</code>`).join(", ")}</span>`
      : `${names.length} of ${vvTables.length} tables chosen`;
    updateVvButton();
  }

  async function loadVvTargets() {
    const select = $("vv-name");
    select.innerHTML = '<option value="">Loading…</option>';
    try { vvTargets = (await call(API.virtualTargets(current.name))).targets; }
    catch (e) { vvTargets = []; select.innerHTML = `<option value="">Could not load datastacks: ${esc(e.message)}</option>`; }
    select.innerHTML = '<option value="">Choose a datastack</option>' +
      vvTargets.map((t) => `<option value="${esc(t.name)}">${esc(t.name)}${t.auth_dataset ? ` (auth dataset ${esc(t.auth_dataset)})` : ""}${t.ready ? "" : " – not ready"}</option>`).join("") +
      `<option value="${NEW_DATASTACK}">New datastack…</option>`;
    vvTargetChanged();
  }

  function vvTargetName() {
    return $("vv-name").value === NEW_DATASTACK ? $("vv-new-name").value.trim() : $("vv-name").value;
  }

  function vvTargetChanged() {
    const isNew = $("vv-name").value === NEW_DATASTACK;
    $("vv-new-name").classList.toggle("d-none", !isNew);
    vvStatus = isNew ? null : vvTargets.find((t) => t.name === $("vv-name").value) || null;
    if (isNew) { checkNewTarget(); $("vv-new-name").focus(); } else renderTargetStatus();
  }

  function checkNewTarget() {
    clearTimeout(vvStatusTimer);
    vvStatus = null;
    const name = vvTargetName();
    if (!name) { renderTargetStatus(); return; }
    if (!/^[A-Za-z0-9_]+$/.test(name)) {
      $("vv-target-status").innerHTML = alertBox("danger", "A datastack name may use only letters, digits and underscores.");
      updateVvButton();
      return;
    }
    $("vv-target-status").innerHTML = '<span class="muted small">Checking…</span>';
    vvStatusTimer = setTimeout(async () => {
      try { vvStatus = await call(API.virtualTarget(current.name, name)); }
      catch (e) { $("vv-target-status").innerHTML = alertBox("danger", esc(e.message)); return; }
      if (vvTargetName() === name) renderTargetStatus();
    }, 400);
  }

  const mark = (ok) => ok ? '<span class="corr-good">✓</span>' : '<span class="corr-bad">✗</span>';

  function renderTargetStatus() {
    const area = $("vv-target-status"), s = vvStatus;
    if (!s) { area.innerHTML = ""; updateVvButton(); return; }
    const isNew = $("vv-name").value === NEW_DATASTACK, info = s.infoservice || {};
    const version = Number($("vv-target").value);
    const duplicate = (s.versions || []).some((v) => v.version === version);
    const examples = vvTargets.filter((t) => t.auth_dataset).map((t) => `<code>${esc(t.name)}</code> → <code>${esc(t.auth_dataset)}</code>`).join(", ");
    const infoOk = info.exists && info.aligned_volume_matches;
    const steps = [
      `<li>${mark(infoOk)} <b>Infoservice.</b> ${infoOk
        ? `Registered on aligned volume <code>${esc(info.aligned_volume)}</code>${info.local_server && s.this_server && info.local_server !== s.this_server
            ? ` <span class="text-warning">(its local server is ${esc(info.local_server)}, not this deployment's ${esc(s.this_server)})</span>` : ""}.`
        : info.exists
          ? `Registered on aligned volume <code>${esc(info.aligned_volume)}</code>, but its versions are looked up on <code>${esc(s.aligned_volume)}</code>. It must use the same aligned volume.`
          : `Add datastack <code>${esc(s.name)}</code> in the <a href="${esc(s.global_server)}/info/admin/" target="_blank" rel="noopener">infoservice admin</a>
             on aligned volume <code>${esc(s.aligned_volume)}</code>, with a segmentation source (the same PCG table as ${esc(current.name)}, or a copy of it)
             and local server <code>${esc(s.this_server || "this deployment")}</code>.`}</li>`,
      `<li>${mark(!!s.auth_dataset)} <b>Auth.</b> ${s.auth_dataset
        ? `Mapped to auth dataset <code>${esc(s.auth_dataset)}</code>; whoever may view that dataset may read this version.`
        : `Ask an auth admin to add table <code>${esc(s.name)}</code> to service namespace <code>datastack</code> in the auth service
           (${esc(s.global_server)}/auth), mapped to an auth dataset, and to give the groups that should read it view permission on that dataset.
           For a public release, map it to a public dataset.${examples ? ` Here: ${examples}.` : ""}`}</li>`,
    ];
    area.innerHTML = `
      <ol class="small ps-3 mb-1">${steps.join("")}</ol>
      ${duplicate ? alertBox("danger", `<code>${esc(s.name)}</code> already has a version ${version}. Creating it again would add a duplicate.`) : ""}
      ${!s.ready ? `<p class="small text-warning mb-1">You can create the version now, but it cannot be queried under ${esc(s.name)} until the steps marked ✗ are done.</p>` : ""}
      ${isNew ? `<p class="small muted mb-0">It only needs this deployment's datastack list if it should appear on the Materialization home page.</p>` : ""}`;
    updateVvButton();
  }

  function vvProblems() {
    const names = parseTableList($("vv-tables").value), known = new Set(vvTables);
    const target = vvTargetName(), version = Number($("vv-target").value);
    const problems = [];
    if (!version) problems.push("choose a frozen version");
    if (!names.length) problems.push("choose at least one table");
    if (names.some((t) => !known.has(t))) problems.push("remove tables that are not in the version");
    if (!target || !/^[A-Za-z0-9_]+$/.test(target)) problems.push("choose the datastack to publish under");
    else if (!vvStatus || vvStatus.name !== target) problems.push("wait for the datastack check");
    else if ((vvStatus.versions || []).some((v) => v.version === version)) problems.push("that datastack already has this version");
    if (target && target === current.name) problems.push("publish under a different datastack");
    return problems;
  }

  function updateVvButton() {
    const problems = vvProblems();
    $("vv-run").disabled = problems.length > 0;
    $("vv-run").title = problems.join("; ");
  }

  function createVirtual() {
    const target = Number($("vv-target").value), name = vvTargetName(), tables = parseTableList($("vv-tables").value);
    const problems = vvProblems();
    if (problems.length) { alert(`Before creating: ${problems.join("; ")}`); return; }
    const notReady = vvStatus && !vvStatus.ready ? `\n\n${name} is not ready to serve it yet (see the steps marked ✗).` : "";
    post("vv-result", API.run(`create_virtual/datastack/${encodeURIComponent(current.name)}`), {
      body: { target_version: target, virtual_version_name: name, tables_to_include: tables },
      summary: `Publish ${current.name} v${target} under ${name} with ${tables.length} table(s)?\n\n${tables.join(", ")}${notReady}`,
    }).then(() => { if ($("vv-name").value === NEW_DATASTACK) checkNewTarget(); else loadVvTargets(); });
  }

  // The versions whose databases exist, then that version's tables and views, so nothing is guessed.
  let dumpTables = [];

  async function loadVersions() {
    const select = $("dump-version"), vvSelect = $("vv-target");
    select.innerHTML = vvSelect.innerHTML = '<option value="">Loading…</option>';
    dumpTables = [];
    renderDumpTables();
    let versions;
    try { versions = (await call(API.versions(current.name))).versions; }
    catch (e) { select.innerHTML = vvSelect.innerHTML = `<option value="">Could not load versions: ${esc(e.message)}</option>`; return; }
    // A virtual version must point at a valid frozen version.
    const valid = versions.filter((v) => v.valid);
    vvSelect.innerHTML = valid.length ? valid.map(versionOption).join("") : '<option value="">No valid frozen versions</option>';
    loadVvTables();
    if (!versions.length) { select.innerHTML = '<option value="">No frozen versions</option>'; return; }
    select.innerHTML = versions.map(versionOption).join("");
    loadDumpTables();
  }

  function versionOption(v) {
    const state = v.valid === undefined ? "" : v.valid ? "" : " (not valid)";
    const made = v.time_stamp ? ` – ${v.time_stamp.slice(0, 10)}` : "";
    const exp = v.expires_on ? `, expires ${v.expires_on.slice(0, 10)}` : "";
    return `<option value="${esc(v.version)}">v${esc(v.version)}${made}${exp}${state}</option>`;
  }

  async function loadDumpTables() {
    const version = $("dump-version").value;
    dumpTables = [];
    if (!version) { renderDumpTables(); return; }
    $("dump-table-count").textContent = "Loading tables…";
    try { dumpTables = (await call(API.versionTables(current.name, version))).tables; }
    catch (e) { $("dump-table").innerHTML = ""; $("dump-table-count").textContent = `Could not load tables: ${e.message}`; return; }
    renderDumpTables();
  }

  function renderDumpTables() {
    const select = $("dump-table"), selected = select.value, pattern = $("dump-filter").value.trim();
    let re = null;
    $("dump-filter").classList.remove("is-invalid");
    if (pattern) {
      try { re = new RegExp(pattern, "i"); } catch (e) { $("dump-filter").classList.add("is-invalid"); return; }
    }
    const shown = dumpTables.filter((t) => !re || re.test(t.name));
    select.innerHTML = shown.map((t) => {
      const detail = [t.kind !== "table" && t.kind, t.rows !== null && `${Number(t.rows).toLocaleString()} rows`].filter(Boolean).join(", ");
      return `<option value="${esc(t.name)}"${t.name === selected ? " selected" : ""}>${esc(t.name)}${detail ? ` (${esc(detail)})` : ""}</option>`;
    }).join("");
    if (!select.value && shown.length === 1) select.value = shown[0].name;
    $("dump-table-count").textContent = dumpTables.length ? `${shown.length} of ${dumpTables.length} tables and views` : "";
  }

  function dumpTable() {
    const version = $("dump-version").value, table = $("dump-table").value;
    if (!version || !table) { alert("Choose a version and a table"); return; }
    post("dump-result", API.run(`dump_csv_table/datastack/${encodeURIComponent(current.name)}/version/${encodeURIComponent(version)}/table_name/${encodeURIComponent(table)}/`),
      { summary: `Dump ${current.name} v${version} ${table} to CSV?` });
  }

  // ---------------------------------------------------------------- table maintenance (superadmin)
  let currentTables = [];

  async function loadDatabases() {
    const select = $("db-select");
    select.innerHTML = "<option>Loading…</option>";
    let dbs;
    try { dbs = (await call(API.databases, { query: { datastack: current.name } })).databases; }
    catch (e) { select.innerHTML = `<option>Could not load databases: ${esc(e.message)}</option>`; return; }
    const live = dbs.filter((d) => d.kind === "live");
    const frozen = dbs.filter((d) => d.kind === "frozen").sort((a, b) => (b.version || 0) - (a.version || 0));
    const option = (d) => {
      const size = d.size_gb === null ? "" : ` (${d.size_gb} GB)`;
      if (d.kind === "live") return `<option value="${esc(d.name)}">${esc(d.name)}${size}</option>`;
      const state = d.valid === undefined ? "" : d.valid ? " valid" : " not valid";
      const exp = d.expires_on ? `, expires ${d.expires_on.slice(0, 10)}` : "";
      return `<option value="${esc(d.name)}">v${esc(d.version)}${state}${exp} – ${esc(d.name)}${size}</option>`;
    };
    select.innerHTML =
      `<optgroup label="Live">${live.map(option).join("")}</optgroup>` +
      `<optgroup label="Frozen versions">${frozen.map(option).join("")}</optgroup>`;
    $("tables-area").innerHTML = "";
  }

  async function loadTables() {
    const db = $("db-select").value, area = $("tables-area");
    area.innerHTML = '<p class="muted">Loading…</p>';
    try {
      currentTables = (await call(API.tableOrder(db), { query: { min_rows: $("min-rows").value, max_correlation: $("max-corr").value } })).tables;
    } catch (e) { area.innerHTML = alertBox("danger", esc(e.message)); return; }
    if (!currentTables.length) { area.innerHTML = '<p class="muted">No tables match.</p>'; return; }
    currentTables.sort((a, b) => Math.abs(a.id_correlation ?? 0) - Math.abs(b.id_correlation ?? 0));
    area.innerHTML = `
      <table class="table table-sm table-hover">
        <thead><tr><th>Table</th><th>Kind</th><th class="text-end">Rows</th><th class="text-end">Table GB</th>
          <th class="text-end">Index GB</th><th>id correlation</th><th>Clustered</th><th>Options</th><th></th></tr></thead>
        <tbody>${currentTables.map((t, i) => `
          <tr><td><code>${esc(t.table_name)}</code></td><td>${esc(t.kind)}</td>
            <td class="text-end">${Number(t.rows).toLocaleString()}</td><td class="text-end">${t.table_gb}</td>
            <td class="text-end">${t.index_gb}</td><td>${corrCell(t.id_correlation)}</td>
            <td>${t.clustered_flag ? "yes" : ""}</td><td class="muted small">${esc((t.reloptions || []).join(", "))}</td>
            <td><button class="btn btn-sm btn-outline-danger" data-repack="${i}">Repack…</button></td></tr>`).join("")}
        </tbody>
      </table>`;
    area.querySelectorAll("[data-repack]").forEach((b) => b.addEventListener("click", () => openRepack(currentTables[b.dataset.repack])));
  }

  let repackTarget = null;
  const repackModal = () => bootstrap.Modal.getOrCreateInstance($("repack-modal"));

  function openRepack(table) {
    repackTarget = { db: $("db-select").value, table };
    const total = table.table_gb + table.index_gb;
    $("rp-table").textContent = `${repackTarget.db}.${table.table_name}`;
    $("rp-summary").innerHTML = `${Number(table.rows).toLocaleString()} rows, ${total.toFixed(2)} GB of table and indexes, ` +
      `id correlation ${corrCell(table.id_correlation)}. Needs about <b>${total.toFixed(1)} GB</b> of free disk while it runs.`;
    $("rp-fillfactor").value = table.kind === "segmentation" ? 90 : "";
    $("rp-force").checked = false;
    $("rp-force").parentElement.style.display = total > 100 ? "" : "none";
    $("rp-result").innerHTML = "";
    repackModal().show();
  }

  async function startRepack(dryRun) {
    const { db, table } = repackTarget, total = table.table_gb + table.index_gb;
    if (!dryRun && !confirm(`Repack ${db}.${table.table_name}?\n\nThis rewrites ${total.toFixed(1)} GB and needs that much free disk.`)) return;
    $("rp-result").innerHTML = '<p class="muted">Queued…</p>';
    try {
      const status = await call(API.repack(db, table.table_name), { method: "POST", query: {
        dry_run: dryRun, fillfactor: $("rp-fillfactor").value, jobs: $("rp-jobs").value,
        wait_timeout: $("rp-wait").value, force: $("rp-force").checked } });
      $("rp-result").innerHTML = alertBox("info", `Job <code>${esc(status.job_id)}</code> queued; see Recent repack jobs.`);
      loadJobs();
    } catch (e) { $("rp-result").innerHTML = alertBox("danger", esc(e.message)); }
  }

  let jobsTimer = null;
  function jobResult(j) {
    if (j.reason || j.error) return esc(j.reason || j.error);
    if (j.before && j.after) return `correlation ${j.before.id_correlation} → ${j.after.id_correlation}, ${j.seconds}s`;
    if (j.state === "dry run") return j.would_install_extension ? "would install the pg_repack extension first" : `ok, needs ${j.estimated_extra_disk_gb} GB`;
    return "";
  }

  async function loadJobs() {
    let jobs;
    try { jobs = (await call(API.repackJobs)).jobs; } catch (e) { $("jobs-area").innerHTML = alertBox("danger", esc(e.message)); return; }
    if (!jobs.length) { $("jobs-area").innerHTML = '<p class="muted">No repack jobs in the last 7 days.</p>'; return; }
    $("jobs-area").innerHTML = `
      <table class="table table-sm"><thead><tr><th>Updated</th><th>Table</th><th>Mode</th><th>State</th><th>Result</th><th></th></tr></thead>
        <tbody>${jobs.map((j, i) => `
          <tr><td class="small">${esc((j.updated_at || "").replace("T", " ").slice(0, 19))}</td>
            <td><code>${esc(j.database)}.${esc(j.table_name)}</code></td>
            <td>${j.options && j.options.dry_run ? "dry run" : "repack"}</td><td><b>${esc(j.state)}</b></td>
            <td class="small">${jobResult(j)}</td>
            <td>${j.output ? `<button class="btn btn-sm btn-link" data-output="${i}">output</button>` : ""}</td></tr>
          <tr class="d-none" id="job-output-${i}"><td colspan="6"><pre class="output">${esc(j.output || "")}</pre></td></tr>`).join("")}
        </tbody></table>`;
    $("jobs-area").querySelectorAll("[data-output]").forEach((b) => b.addEventListener("click", () => $(`job-output-${b.dataset.output}`).classList.toggle("d-none")));
    clearTimeout(jobsTimer);
    if (jobs.some((j) => ACTIVE_REPACK.has(j.state))) jobsTimer = setTimeout(loadJobs, 10000);
  }

  // ---------------------------------------------------------------- uploads (superadmin)
  async function loadUploads() {
    let jobs;
    try { jobs = ((await call(API.uploadJobs)).jobs || []).filter((j) => !current || j.datastack_name === current.name); }
    catch (e) { $("uploads-area").innerHTML = alertBox("danger", esc(e.message)); return; }
    if (!jobs.length) { $("uploads-area").innerHTML = '<p class="muted">No upload job records for this datastack.</p>'; return; }
    $("uploads-area").innerHTML = `
      <table class="table table-sm"><thead><tr><th>Job</th><th>Status</th><th>Phase</th><th>Staging cleanup</th><th></th></tr></thead>
        <tbody>${jobs.map((j) => `
          <tr><td><code class="small">${esc(j.job_id)}</code></td><td>${esc(j.status)}</td><td class="small">${esc(j.phase)}</td>
            <td class="small">${esc(j.staging_cleanup || "")}</td>
            <td class="text-nowrap">
              <button class="btn btn-sm btn-outline-primary" data-cleanup="${esc(j.job_id)}" data-dry="1">Dry run</button>
              <button class="btn btn-sm btn-outline-danger" data-cleanup="${esc(j.job_id)}" data-dry="0">Clean up</button></td></tr>
          <tr class="d-none" id="cleanup-${esc(j.job_id)}"><td colspan="5"></td></tr>`).join("")}
        </tbody></table>`;
    $("uploads-area").querySelectorAll("[data-cleanup]").forEach((b) => b.addEventListener("click", () => cleanupJob(b.dataset.cleanup, b.dataset.dry === "1")));
  }

  async function cleanupJob(jobId, dryRun) {
    let includeProduction = false;
    if (!dryRun) {
      if (!confirm(`Remove everything upload ${jobId} left behind (tasks, job record, checkpoints, staging tables)?`)) return;
      includeProduction = confirm("Also drop its tables from the PRODUCTION database?\n\nCancel = staging only.");
    }
    const cell = $(`cleanup-${jobId}`);
    cell.classList.remove("d-none");
    cell.firstElementChild.innerHTML = '<span class="muted">Working…</span>';
    try {
      let data = await call(API.jobCleanup(jobId), { method: "POST", query: { dry_run: dryRun, include_production: includeProduction } });
      if (data.status === "refused" && !dryRun && confirm(`Refused: ${data.report.reason}\n\nForce it?`)) {
        data = await call(API.jobCleanup(jobId), { method: "POST", query: { dry_run: false, include_production: includeProduction, force: true } });
      }
      cell.firstElementChild.innerHTML = `<pre class="output">${esc(JSON.stringify(data.report, null, 1))}</pre>`;
      if (!dryRun) loadUploads();
    } catch (e) { cell.firstElementChild.innerHTML = alertBox("danger", esc(e.message)); }
  }

  // ---------------------------------------------------------------- deployment (superadmin)
  async function bulkCleanup(dryRun) {
    const area = $("bulk-report");
    area.innerHTML = `<p class="muted">${dryRun ? "Looking for" : "Removing"} failed uploads and orphans… this can take a few minutes.</p>`;
    let report;
    try {
      report = (await call(API.bulkCleanup, { method: "POST", query: {
        dry_run: dryRun, statuses: $("cleanup-statuses").value,
        include_orphans: $("include-orphans").checked, orphan_min_age_hours: $("orphan-age").value } })).report;
    } catch (e) { area.innerHTML = alertBox("danger", esc(e.message)); return; }
    const jobs = report.purged_jobs || [], orphans = report.orphaned_staging_tables || [];
    const tables = orphans.reduce((n, o) => n + (o.staging_tables_dropped || []).length, 0);
    area.innerHTML = `
      ${alertBox(dryRun ? "info" : "success", `${dryRun ? "Would remove" : "Removed"} ${jobs.length} failed upload(s) and ${orphans.length} orphaned upload(s) (${tables} staging tables).`)}
      ${orphans.length ? `<p class="small">${orphans.map((o) => `<code>${esc(o.table_name)}</code>`).join(", ")}</p>` : ""}
      ${jobs.length ? `<p class="small">${jobs.map((j) => `<code>${esc(j.job_id)}</code> (${esc(j.result)})`).join(", ")}</p>` : ""}
      ${dryRun && (jobs.length || orphans.length) ? '<button id="bulk-run" class="btn btn-danger btn-sm">Remove these</button>' : ""}`;
    const run = $("bulk-run");
    if (run) run.addEventListener("click", () => { if (confirm("Remove these from staging?")) bulkCleanup(false); });
  }

  async function loadQueues() {
    let data;
    try { data = await call(API.queues); } catch (e) { $("queues-area").innerHTML = alertBox("danger", esc(e.message)); return; }
    const pools = Object.entries(data.claimed);
    $("queues-area").innerHTML = `
      <table class="table table-sm w-auto"><thead><tr><th>Queue</th><th class="text-end">Waiting</th></tr></thead>
        <tbody>${Object.entries(data.queues).map(([q, n]) => `<tr><td>${esc(q)}</td><td class="text-end">${n}</td></tr>`).join("")}</tbody></table>
      <h6 class="mt-3">In flight (claimed by a worker)</h6>
      ${pools.length ? `<table class="table table-sm"><thead><tr><th>Pool</th><th class="text-end">Claimed</th><th>Oldest claim</th><th>Tasks</th></tr></thead>
        <tbody>${pools.map(([pool, c]) => `<tr><td>${esc(pool)}</td><td class="text-end">${c.count}</td>
          <td>${c.oldest_claim_seconds === null ? "" : `${Math.round(c.oldest_claim_seconds / 60)} min`}</td>
          <td class="small">${Object.entries(c.tasks).map(([t, n]) => `${esc(t)} × ${n}`).join("<br>")}</td></tr>`).join("")}</tbody></table>`
        : '<p class="muted">Nothing claimed.</p>'}`;
  }

  async function showJson(areaId, url, options) {
    const area = $(areaId);
    area.innerHTML = '<span class="muted">Loading…</span>';
    try {
      const data = await call(url, options);
      const empty = data === null || (typeof data === "object" && !Object.keys(data).length);
      area.innerHTML = empty ? '<p class="muted">None.</p>' : `<pre class="output">${esc(JSON.stringify(data, null, 1))}</pre>`;
    } catch (e) { area.innerHTML = alertBox("danger", esc(e.message)); }
  }

  // ---------------------------------------------------------------- wiring
  document.addEventListener("DOMContentLoaded", () => {
    $("ds-select").addEventListener("change", selectDatastack);
    $("ingest-table-run").addEventListener("click", ingestTable);
    $("vv-run").addEventListener("click", createVirtual);
    $("vv-target").addEventListener("change", loadVvTables);
    $("vv-tables").addEventListener("input", checkVvTables);
    $("vv-tables").addEventListener("change", renderVvTables);
    $("vv-filter").addEventListener("input", renderVvTables);
    $("vv-table-list").addEventListener("change", vvListChanged);
    $("vv-select-shown").addEventListener("click", (e) => {
      e.preventDefault();
      const shown = [...$("vv-table-list").options].map((o) => o.value);
      $("vv-tables").value = [...new Set([...parseTableList($("vv-tables").value), ...shown])].join(", ");
      checkVvTables();
    });
    $("vv-clear").addEventListener("click", (e) => { e.preventDefault(); $("vv-tables").value = ""; checkVvTables(); });
    $("vv-name").addEventListener("change", vvTargetChanged);
    $("vv-new-name").addEventListener("input", checkNewTarget);
    $("dump-run").addEventListener("click", dumpTable);
    $("dump-version").addEventListener("change", loadDumpTables);
    $("dump-filter").addEventListener("input", renderDumpTables);
    $("load-tables").addEventListener("click", loadTables);
    $("rp-dry").addEventListener("click", () => startRepack(true));
    $("rp-run").addEventListener("click", () => startRepack(false));
    $("refresh-uploads").addEventListener("click", loadUploads);
    $("bulk-dry-run").addEventListener("click", () => bulkCleanup(true));
    $("refresh-queues").addEventListener("click", loadQueues);
    $("refresh-active").addEventListener("click", () => showJson("active-area", API.active));
    $("refresh-locks").addEventListener("click", () => showJson("locks-area", API.locks));
    $("refresh-workers").addEventListener("click", () => showJson("workers-area", API.workers));
    $("release-locks").addEventListener("click", () => {
      if (confirm("Release all task locks? Only do this if a workflow died while holding one.")) showJson("locks-area", API.locks, { method: "PUT" });
    });
    document.querySelector('[data-bs-target="#tab-deployment"]').addEventListener("shown.bs.tab", loadQueues);
    renderWorkflows();
    loadCapabilities().then(() => { if (caps.superadmin) loadJobs(); });
  });
})();
