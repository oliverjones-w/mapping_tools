# BBG Frontend Build Instructions
## Agent Instruction Manual — Surgical Integration into BankSt OS Shell

---

## 0. Context & Constraints

You are adding **two new workspace views** to the BankSt OS Shell at `C:/dev/unified_css/`.
Touch **only** the files listed in each step. Do not refactor, reformat, or touch anything else.
Read every target file fully before editing it.

**Project root:** `C:/dev/unified_css/`

**API helper available:** `mappingGet(path)` in `js/api.js`
- Already defined. Calls `window.APP_CONFIG.MAPPING_API_BASE` (`/api/mapping`) + path
- Returns parsed JSON. Throws on non-2xx.
- Pattern: `const data = await mappingGet('/bbg/firms')`

**Views to build:**
| View ID | Tab type | Title | Description |
|---|---|---|---|
| `bbg.firms` | `bbg.firms` | BBG Extraction | Global summary table of all firms |
| `bbg.firm` | `bbg.firm` | (firm name) | Per-firm detail: run selector + 3 tabs |

---

## 1. Read These Files First (Do Not Skip)

Before writing a single line, read these files in full so you understand the patterns exactly:

```
C:/dev/unified_css/js/views.js        — full file (all registerWorkspaceView calls)
C:/dev/unified_css/js/nav.js          — full file
C:/dev/unified_css/js/api.js          — full file
C:/dev/unified_css/index.html         — full file
C:/dev/unified_css/js/workspace.js    — lines 1–80 (imports + registerWorkspaceView signature)
```

---

## 2. API Endpoints & Data Shapes

All calls go through `mappingGet()`. Paths are relative to `/api/mapping`.

### `GET /bbg/firms`
Returns array. One object per firm (latest run only):
```json
[
  {
    "firm_id": "019cea10-7252-72f8-86e3-18f571cf7c36",
    "firm_name": "Alphadyne Asset Management",
    "run_at": "2026-03-18T19:21:18.350578+00:00",
    "rows_processed": 159,
    "confirmed_count": 155,
    "discrepancy_count": 0,
    "addition_count": 4,
    "latest_run_id": 31,
    "tracking_pct": 97.5
  }
]
```

**Filter rule:** Only show rows where `firm_id` matches UUID pattern
(`/^[0-9a-f]{8}-[0-9a-f]{4}/i`). The old pre-fix runs have simple string IDs
(e.g. `"alphadyne"`) — exclude these from the table.

### `GET /bbg/firms/{firm_id}/runs`
Returns array, newest first. Each object:
```json
{
  "run_id": 31,
  "firm_id": "019cea10-...",
  "firm_name": "Alphadyne Asset Management",
  "csv_filename": "alphadyne.csv",
  "source_type": "archive",
  "run_at": "2026-03-18T19:21:18.350578+00:00",
  "rows_processed": 159,
  "confirmed_count": 155,
  "discrepancy_count": 0,
  "addition_count": 4
}
```

### `GET /bbg/runs/{run_id}/confirmed`
Returns array:
```json
[
  {
    "id": 463,
    "run_id": 31,
    "firm_id": "019cea10-...",
    "hf_record_id": "HF_0021425",
    "name": "Adam Shukovsky",
    "firm": "Alphadyne Asset Management",
    "title": "Portfolio Manager",
    "location": "New York, NY",
    "function": "Portfolio Manager",
    "strategy": "Global Rates RV",
    "products": "IRD, IRO, Exotics",
    "reports_to": null
  }
]
```

### `GET /bbg/runs/{run_id}/discrepancies`
Returns array:
```json
[
  {
    "id": 12,
    "run_id": 31,
    "firm_id": "019cea10-...",
    "name": "John Smith",
    "master_record_uids": "HF_0012345",
    "discrepancy_field": "firm",
    "new_file_value": "Alphadyne Capital",
    "master_file_values": "Alphadyne Asset Management",
    "alias_check_info": "No alias match found",
    "source_file": "alphadyne.csv",
    "status": "Active",
    "first_seen": "2026-03-18T19:21:18.350578+00:00"
  }
]
```

### `GET /bbg/runs/{run_id}/additions`
Returns array:
```json
[
  {
    "id": 8,
    "run_id": 31,
    "firm_id": "019cea10-...",
    "name": "Jane Doe",
    "company": "Alphadyne Capital LLC",
    "canonical_company": "Alphadyne Asset Management",
    "title": "Analyst",
    "location": "London",
    "focus": null,
    "source_file": "alphadyne.csv",
    "first_seen": "2026-03-18T19:21:18.350578+00:00"
  }
]
```

---

## 3. File 1 — `js/views.js`

### Where to insert
Append both views at the **end of the file**, after the last existing `registerWorkspaceView({...})` block and before any closing comments. Do not move or touch existing views.

### Imports / helpers available inside views.js
Look at the top of `views.js` — it already imports:
- `mappingGet` from `./api.js`
- `updateActiveTabState` from `./workspace.js`
- `escapeHtml` from `./utils.js`
- `fetchingTabs` Set (already declared in the file)

Use these. Do not redeclare them.

---

### View 1: `bbg.firms` — Global Summary Table

**Behaviour:**
- On activate: fetch `/bbg/firms`, filter to UUID firm_ids only, store in `tab.state.data`
- Render: summary table + global aggregate stats row at top
- Clicking a firm row opens `bbg.firm` tab for that firm
- Toolbar: single "Summary" mode button (no other modes needed yet)

**Table columns:** Firm | Confirmed | Discrepancies | Additions | Tracking % | Last Run

**Tracking % display:** render as a plain inline bar (a `<div>` with width set to `tracking_pct%`) + the numeric value. Do not use any external charting library. Example pattern from existing views: just a styled `<div class="pct-bar-track"><div class="pct-bar-fill" style="width:${pct}%"></div></div>` with inline CSS on the element.

**Global stats:** Before the table, show 4 stat tiles in a row:
- Total Firms
- Total Confirmed (sum of confirmed_count)
- Total Discrepancies (sum of discrepancy_count)
- Total Additions (sum of addition_count)

Use the `.meta-grid` + `.meta-item` pattern you see in other detail views for the stat tiles, or model them after how `perf.dashboard` renders metric blocks — pick whichever fits the existing CSS classes.

**Loading state:** While data is null, render:
```html
<div class="table-shell view-placeholder"><span>BBG Extraction</span><p>Loading firms…</p></div>
```

**Code to append to views.js:**

```javascript
// ---------------------------------------------------------------------------
// BBG Extraction — Firms Summary
// ---------------------------------------------------------------------------
registerWorkspaceView({
  id: "bbg.firms",
  match: (tab) => tab.type === "bbg.firms",
  toolbar: (_tab) => ({
    left:  [{ id: "bbg.firms.summary", label: "Summary", active: true }],
    right: [{ id: "bbg.firms.refresh", label: "Refresh" }],
  }),
  render: (tab) => {
    const firms = tab.state?.data;
    if (!firms) {
      return `<div class="table-shell view-placeholder"><span>BBG Extraction</span><p>Loading firms…</p></div>`;
    }

    const totalConfirmed    = firms.reduce((s, f) => s + (f.confirmed_count    || 0), 0);
    const totalDiscrepancies = firms.reduce((s, f) => s + (f.discrepancy_count || 0), 0);
    const totalAdditions    = firms.reduce((s, f) => s + (f.addition_count     || 0), 0);

    const statTiles = `
      <div class="meta-grid" style="margin-bottom:16px;">
        <div class="meta-item"><div class="meta-label">Firms</div><div class="meta-value">${firms.length}</div></div>
        <div class="meta-item"><div class="meta-label">Confirmed</div><div class="meta-value">${totalConfirmed.toLocaleString()}</div></div>
        <div class="meta-item"><div class="meta-label">Discrepancies</div><div class="meta-value">${totalDiscrepancies.toLocaleString()}</div></div>
        <div class="meta-item"><div class="meta-label">Additions</div><div class="meta-value">${totalAdditions.toLocaleString()}</div></div>
      </div>
    `;

    const firmRow = (f) => {
      const pct = (f.tracking_pct || 0).toFixed(1);
      const runDate = f.run_at ? new Date(f.run_at).toLocaleDateString() : "—";
      return `
        <div class="table-row-wrap">
          <div class="table-row-grid bbg-firms-grid">
            <button class="cell-link" data-open-bbg-firm="${escapeHtml(f.firm_id)}" data-firm-name="${escapeHtml(f.firm_name)}">${escapeHtml(f.firm_name)}</button>
            <div class="cell-mono">${(f.confirmed_count || 0).toLocaleString()}</div>
            <div class="cell-mono">${(f.discrepancy_count || 0).toLocaleString()}</div>
            <div class="cell-mono">${(f.addition_count || 0).toLocaleString()}</div>
            <div>
              <div style="display:flex;align-items:center;gap:6px;">
                <div style="flex:1;height:4px;background:var(--surface-2,rgba(255,255,255,.08));border-radius:2px;overflow:hidden;">
                  <div style="width:${pct}%;height:100%;background:var(--accent,#4a90d9);border-radius:2px;"></div>
                </div>
                <span class="cell-mono" style="min-width:36px;">${pct}%</span>
              </div>
            </div>
            <div class="cell-mono">${runDate}</div>
          </div>
        </div>
      `;
    };

    const sorted = [...firms].sort((a, b) => (b.tracking_pct || 0) - (a.tracking_pct || 0));

    return `
      <div class="table-shell">
        ${statTiles}
        <div class="table-header-grid bbg-firms-grid">
          <div>Firm</div>
          <div>Confirmed</div>
          <div>Discrepancies</div>
          <div>Additions</div>
          <div>Tracking %</div>
          <div>Last Run</div>
        </div>
        ${sorted.map(firmRow).join("")}
      </div>
    `;
  },
  onActivate: async (tab) => {
    if (fetchingTabs.has(tab.id)) return;
    if (tab.state?.data) return;
    fetchingTabs.add(tab.id);
    try {
      const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}/i;
      const all  = await mappingGet("/bbg/firms");
      const data = all.filter(f => UUID_RE.test(f.firm_id));
      updateActiveTabState({ data }, tab.id);
    } catch (e) {
      console.error("[bbg.firms] fetch failed:", e);
    } finally {
      fetchingTabs.delete(tab.id);
    }
  },
});
```

---

### View 2: `bbg.firm` — Per-Firm Detail

**Behaviour:**
- `tab.entityId` = the firm's UUID (`firm_id`)
- `tab.title` = firm name (set at open time)
- On activate: fetch `/bbg/firms/{entityId}/runs` → store as `tab.state.runs`; then fetch the confirmed/discrepancies/additions for `runs[0].run_id` → store as `tab.state.runData`
- Tab state also tracks `tab.state.selectedRunId` (defaults to `runs[0].run_id`)
- Toolbar modes: `confirmed` | `discrepancies` | `additions`
- When user clicks a different run in the run history list (right rail widget — see Section 4), it dispatches a custom event. The view listens for toolbar mode changes via the existing toolbar action pattern.

**Layout:**
```
┌─────────────────────────────────────────────────────────────┐
│  Firm Name                                                  │
│  Run: [dropdown selector] — rows_processed rows            │
│  [Confirmed N] [Discrepancies N] [Additions N]  stat tiles  │
├─────────────────────────────────────────────────────────────┤
│  [Confirmed tab]  [Discrepancies tab]  [Additions tab]     │
│                                                             │
│  ← table for the active tab →                              │
└─────────────────────────────────────────────────────────────┘
```

**Column specs per tab:**

*Confirmed:* Name | Firm | Title | Function | Strategy | Products | Location

*Discrepancies:* Name | Field | BBG Value | Master Value | Alias Info | Status | First Seen

*Additions:* Name | Company (BBG) | Canonical | Title | Location | First Seen

**Run selector:** A `<select>` element inside the view header area. Its `onchange` should dispatch:
```javascript
document.dispatchEvent(new CustomEvent("bankst:bbgRunChange", {
  detail: { tabId: tab.id, runId: parseInt(e.target.value) }
}));
```

**Search filter:** A text `<input>` above each table that filters rows client-side on `name` field (case-insensitive contains). Keep this simple — no multiselect. Debounce not required.

**Code to append to views.js immediately after the bbg.firms view:**

```javascript
// ---------------------------------------------------------------------------
// BBG Extraction — Firm Detail
// ---------------------------------------------------------------------------
registerWorkspaceView({
  id: "bbg.firm",
  match: (tab) => tab.type === "bbg.firm",
  toolbar: (tab) => ({
    left: [
      { id: "bbg.firm.confirmed",     label: `Confirmed${tab.state?.runData ? ` (${tab.state.runData.confirmed?.length ?? 0})` : ""}`,     active: (tab.state?.mode || "confirmed") === "confirmed" },
      { id: "bbg.firm.discrepancies", label: `Discrepancies${tab.state?.runData ? ` (${tab.state.runData.discrepancies?.length ?? 0})` : ""}`, active: tab.state?.mode === "discrepancies" },
      { id: "bbg.firm.additions",     label: `Additions${tab.state?.runData ? ` (${tab.state.runData.additions?.length ?? 0})` : ""}`,     active: tab.state?.mode === "additions" },
    ],
    right: [],
  }),
  render: (tab) => {
    const mode    = tab.state?.mode || "confirmed";
    const runs    = tab.state?.runs;
    const runData = tab.state?.runData;
    const selRunId = tab.state?.selectedRunId;
    const firmName = tab.title || "Firm";

    if (!runs) {
      return `<div class="detail-view-shell view-placeholder"><span>${escapeHtml(firmName)}</span><p>Loading extraction data…</p></div>`;
    }

    // Run selector header
    const runSelector = `
      <div class="detail-header" style="margin-bottom:12px;">
        <div>
          <div class="detail-title">${escapeHtml(firmName)}</div>
          <div class="detail-subtitle" style="display:flex;align-items:center;gap:8px;margin-top:4px;">
            <label style="font-size:var(--font-size-label,9px);text-transform:uppercase;opacity:.6;">Run</label>
            <select class="bbg-run-selector" data-tab-id="${escapeHtml(tab.id)}" style="font-size:var(--font-size-data,11px);background:var(--surface-2);border:1px solid var(--border);color:inherit;border-radius:4px;padding:2px 6px;">
              ${(runs || []).map(r => {
                const label = `${new Date(r.run_at).toLocaleDateString()} — ${r.csv_filename} (${r.rows_processed} rows)`;
                return `<option value="${r.run_id}" ${r.run_id === selRunId ? "selected" : ""}>${escapeHtml(label)}</option>`;
              }).join("")}
            </select>
          </div>
        </div>
      </div>
    `;

    if (!runData) {
      return `<div class="detail-view-shell">${runSelector}<p style="opacity:.5">Loading run data…</p></div>`;
    }

    // Stat tiles
    const statRow = `
      <div class="meta-grid" style="margin-bottom:12px;">
        <div class="meta-item"><div class="meta-label">Confirmed</div><div class="meta-value">${(runData.confirmed?.length || 0).toLocaleString()}</div></div>
        <div class="meta-item"><div class="meta-label">Discrepancies</div><div class="meta-value">${(runData.discrepancies?.length || 0).toLocaleString()}</div></div>
        <div class="meta-item"><div class="meta-label">Additions</div><div class="meta-value">${(runData.additions?.length || 0).toLocaleString()}</div></div>
        <div class="meta-item"><div class="meta-label">Rows Processed</div><div class="meta-value">${(runs.find(r => r.run_id === selRunId)?.rows_processed || 0).toLocaleString()}</div></div>
      </div>
    `;

    // Search input
    const searchInput = `
      <div style="margin-bottom:8px;">
        <input class="bbg-search-input" data-tab-id="${escapeHtml(tab.id)}" data-mode="${mode}" type="text" placeholder="Filter by name…"
          style="width:100%;max-width:320px;font-size:var(--font-size-data,11px);padding:4px 8px;border-radius:4px;border:1px solid var(--border);background:var(--surface-2);color:inherit;"
          value="${escapeHtml(tab.state?.searchQuery || "")}" />
      </div>
    `;

    const q = (tab.state?.searchQuery || "").toLowerCase();
    const filterByName = (arr) => q ? arr.filter(r => (r.name || "").toLowerCase().includes(q)) : arr;
    const esc = escapeHtml;

    let tableHtml = "";

    if (mode === "confirmed") {
      const rows = filterByName(runData.confirmed || []);
      tableHtml = `
        <div class="table-header-grid bbg-confirmed-grid">
          <div>Name</div><div>Firm</div><div>Title</div><div>Function</div><div>Strategy</div><div>Products</div><div>Location</div>
        </div>
        ${rows.map(r => `
          <div class="table-row-wrap">
            <div class="table-row-grid bbg-confirmed-grid">
              <div>${esc(r.name || "")}</div>
              <div>${esc(r.firm || "")}</div>
              <div>${esc(r.title || "")}</div>
              <div>${esc(r.function || "")}</div>
              <div>${esc(r.strategy || "")}</div>
              <div>${esc(r.products || "")}</div>
              <div>${esc(r.location || "")}</div>
            </div>
          </div>
        `).join("")}
        ${rows.length === 0 ? `<div style="padding:16px;opacity:.5;">No records match filter.</div>` : ""}
      `;
    } else if (mode === "discrepancies") {
      const rows = filterByName(runData.discrepancies || []);
      tableHtml = `
        <div class="table-header-grid bbg-disc-grid">
          <div>Name</div><div>Field</div><div>BBG Value</div><div>Master Value</div><div>Alias Info</div><div>Status</div><div>First Seen</div>
        </div>
        ${rows.map(r => `
          <div class="table-row-wrap">
            <div class="table-row-grid bbg-disc-grid">
              <div>${esc(r.name || "")}</div>
              <div class="cell-mono">${esc(r.discrepancy_field || "")}</div>
              <div>${esc(r.new_file_value || "")}</div>
              <div>${esc(r.master_file_values || "")}</div>
              <div style="font-size:10px;opacity:.7;">${esc(r.alias_check_info || "")}</div>
              <div><span class="alias-tag ${r.status === "Active" ? "" : "alias-tag--platform"}">${esc(r.status || "")}</span></div>
              <div class="cell-mono">${r.first_seen ? new Date(r.first_seen).toLocaleDateString() : "—"}</div>
            </div>
          </div>
        `).join("")}
        ${rows.length === 0 ? `<div style="padding:16px;opacity:.5;">No discrepancies match filter.</div>` : ""}
      `;
    } else if (mode === "additions") {
      const rows = filterByName(runData.additions || []);
      tableHtml = `
        <div class="table-header-grid bbg-add-grid">
          <div>Name</div><div>BBG Company</div><div>Canonical</div><div>Title</div><div>Location</div><div>First Seen</div>
        </div>
        ${rows.map(r => `
          <div class="table-row-wrap">
            <div class="table-row-grid bbg-add-grid">
              <div>${esc(r.name || "")}</div>
              <div>${esc(r.company || "")}</div>
              <div>${esc(r.canonical_company || "")}</div>
              <div>${esc(r.title || "")}</div>
              <div>${esc(r.location || "")}</div>
              <div class="cell-mono">${r.first_seen ? new Date(r.first_seen).toLocaleDateString() : "—"}</div>
            </div>
          </div>
        `).join("")}
        ${rows.length === 0 ? `<div style="padding:16px;opacity:.5;">No additions match filter.</div>` : ""}
      `;
    }

    return `
      <div class="detail-view-shell">
        ${runSelector}
        ${statRow}
        ${searchInput}
        <div class="table-shell" style="margin-top:0;">
          ${tableHtml}
        </div>
      </div>
    `;
  },
  onActivate: async (tab) => {
    if (fetchingTabs.has(tab.id)) return;
    if (tab.state?.runs) return;
    fetchingTabs.add(tab.id);
    try {
      const runs = await mappingGet(`/bbg/firms/${tab.entityId}/runs`);
      if (!runs || runs.length === 0) {
        updateActiveTabState({ runs: [] }, tab.id);
        return;
      }
      const latestRunId = runs[0].run_id;
      const [confirmed, discrepancies, additions] = await Promise.all([
        mappingGet(`/bbg/runs/${latestRunId}/confirmed`),
        mappingGet(`/bbg/runs/${latestRunId}/discrepancies`),
        mappingGet(`/bbg/runs/${latestRunId}/additions`),
      ]);
      updateActiveTabState({
        runs,
        selectedRunId: latestRunId,
        runData: { confirmed, discrepancies, additions },
        mode: "confirmed",
      }, tab.id);
    } catch (e) {
      console.error("[bbg.firm] fetch failed:", e);
    } finally {
      fetchingTabs.delete(tab.id);
    }
  },
});
```

---

## 4. File 2 — `js/nav.js`

### What to add

Append two exported functions at the end of the file:

```javascript
export function openBbgFirmsTab() {
  openTab({
    id:    "tab-bbg-firms",
    type:  "bbg.firms",
    title: "BBG Extraction",
    state: {},
  });
}

export function openBbgFirmTab(firmId, firmName) {
  openTab({
    id:         `tab-bbg-firm-${firmId}`,
    type:       "bbg.firm",
    entityType: "bbg_firm",
    entityId:   firmId,
    title:      firmName || "BBG Firm",
    state:      { mode: "confirmed" },
  });
}
```

---

## 5. File 3 — `js/app.js`

You need to do **three things** in this file:

### 5a. Import the new nav functions
Find the existing import line that imports from `./nav.js`. It will look something like:
```javascript
import { openPersonTab, openFirmTab, openFirmCard, openFinraTab, openHfTab, runCommand } from "./nav.js";
```
Add `openBbgFirmsTab` and `openBbgFirmTab` to that import. Do not rewrite the line — just append the two names to the existing destructure.

### 5b. Wire up `data-open-bbg-firm` click delegation
Find the section in `app.js` where click events are handled (look for `data-open-person`, `data-open-firm`, etc. — it's in an event delegation block on `document` or `.workspace`).

In that same block, add a handler for `data-open-bbg-firm`:

```javascript
const bbgFirmBtn = e.target.closest("[data-open-bbg-firm]");
if (bbgFirmBtn) {
  const firmId   = bbgFirmBtn.dataset.openBbgFirm;
  const firmName = bbgFirmBtn.dataset.firmName || firmId;
  openBbgFirmTab(firmId, firmName);
  return;
}
```

### 5c. Wire up run selector change + search input

Find where `app.js` handles delegated `change` or `input` events (if a delegated listener exists). If not, add new listeners on `document`:

**Run selector change:**
```javascript
document.addEventListener("change", (e) => {
  const sel = e.target.closest(".bbg-run-selector");
  if (!sel) return;
  const tabId = sel.dataset.tabId;
  const runId = parseInt(sel.value, 10);
  document.dispatchEvent(new CustomEvent("bankst:bbgRunChange", { detail: { tabId, runId } }));
});
```

**Search input:**
```javascript
document.addEventListener("input", (e) => {
  const inp = e.target.closest(".bbg-search-input");
  if (!inp) return;
  const tabId = inp.dataset.tabId;
  updateTabState(tabId, { searchQuery: inp.value });
});
```

**Run change handler** (responds to the custom event dispatched by the selector):

Find where other `bankst:*` custom events are listened to (e.g. `bankst:toggleRightRail`). In that same area, add:

```javascript
document.addEventListener("bankst:bbgRunChange", async (e) => {
  const { tabId, runId } = e.detail;
  // Clear existing runData so view shows loading state
  updateTabState(tabId, { selectedRunId: runId, runData: null });
  try {
    const [confirmed, discrepancies, additions] = await Promise.all([
      mappingGet(`/bbg/runs/${runId}/confirmed`),
      mappingGet(`/bbg/runs/${runId}/discrepancies`),
      mappingGet(`/bbg/runs/${runId}/additions`),
    ]);
    updateTabState(tabId, { runData: { confirmed, discrepancies, additions } });
  } catch (err) {
    console.error("[bbgRunChange] fetch failed:", err);
  }
});
```

**Check what `updateTabState` is called in app.js.** It may be `updateActiveTabState` (imported from workspace.js). Use whichever form the existing code uses — do not invent a new function.

Also check whether `mappingGet` is already imported in `app.js`. If not, add it to the existing api.js import line.

---

## 6. File 4 — `index.html`

### Left rail navigation entry

Find the `<nav class="nav-list">` block inside the left rail (`<aside class="left-rail">`).

Locate the existing HF Map and IR Map entries — they look like:
```html
<button class="nav-item" data-open-nav-tab="hf.table"><span class="nav-icon">◈</span><span class="nav-label">HF Map</span></button>
<button class="nav-item" data-open-nav-tab="ir.table"><span class="nav-icon">◈</span><span class="nav-label">IR Map</span></button>
```

Add the BBG entry **immediately after** the IR Map line:
```html
<button class="nav-item" data-open-nav-tab="bbg.firms"><span class="nav-icon">⊛</span><span class="nav-label">BBG Extraction</span></button>
```

---

## 7. File 5 — `js/app.js` — `data-open-nav-tab` handler

Find where `data-open-nav-tab` attribute values are dispatched to open tabs. It will look something like a switch or a series of `if` checks on the tab type string. Add a case for `"bbg.firms"`:

```javascript
if (navTab === "bbg.firms") { openBbgFirmsTab(); return; }
```

Place it alongside the existing `hf.table`, `ir.table` etc. cases.

---

## 8. CSS — Grid Column Templates

The three table grids need `grid-template-columns` definitions.

Look at how existing table grids are defined — search `js/views.js` for `table-row-grid` to see if column counts are set via inline style or a CSS class.

If via inline style on the `.table-row-grid` element, add `style="grid-template-columns: ..."` directly to the div. If via CSS class (like `.hf-grid`), add the equivalent classes to `css/data-views.css`.

**Check `css/data-views.css` first** and follow whichever pattern already exists there.

Suggested column templates (adjust widths to match existing table proportions):
```css
.bbg-firms-grid     { grid-template-columns: 2fr 80px 100px 80px 140px 90px; }
.bbg-confirmed-grid { grid-template-columns: 1.5fr 1.5fr 1.5fr 1fr 1fr 1fr 1fr; }
.bbg-disc-grid      { grid-template-columns: 1.5fr 100px 1fr 1fr 1fr 80px 90px; }
.bbg-add-grid       { grid-template-columns: 1.5fr 1.5fr 1.5fr 1fr 1fr 90px; }
```

Add these to `css/data-views.css` at the end, in a clearly labelled block:
```css
/* ── BBG Extraction views ─────────────────────────────────── */
```

---

## 9. Verification Checklist

After making all edits, verify each of these without running the app:

- [ ] `views.js` — `bbg.firms` view registered, uses `mappingGet`, `fetchingTabs`, `updateActiveTabState`, `escapeHtml` (all already imported)
- [ ] `views.js` — `bbg.firm` view registered, uses same helpers, references `tab.entityId` for firm UUID
- [ ] `nav.js` — `openBbgFirmsTab` and `openBbgFirmTab` exported
- [ ] `app.js` — both nav functions imported from `./nav.js`
- [ ] `app.js` — `data-open-bbg-firm` click handled in delegation block
- [ ] `app.js` — `.bbg-run-selector` change event wired
- [ ] `app.js` — `.bbg-search-input` input event wired
- [ ] `app.js` — `bankst:bbgRunChange` event listener added
- [ ] `app.js` — `"bbg.firms"` case in `data-open-nav-tab` handler
- [ ] `index.html` — BBG nav button added to left rail after IR Map
- [ ] `css/data-views.css` — 4 grid classes added

---

## 10. What NOT to do

- Do not touch `workspace.js`, `api.js`, `widgets.js`, `palette.js`, `cards.js`, or any CSS file other than `data-views.css`
- Do not modify existing view registrations
- Do not add right-rail widgets (can be added in a later pass)
- Do not add download/export buttons (can be added later)
- Do not add command palette entries (can be added later)
- Do not change the mock data or any existing entity data
- Do not introduce any new JS dependencies or imports from outside the existing modules
