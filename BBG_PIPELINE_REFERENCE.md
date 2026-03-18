# BBG Extraction Pipeline — As-Built Reference
**Last updated:** 2026-03-18

---

## Overview

The BBG extraction pipeline ingests Bloomberg contact CSVs, matches records against the HF master database, and classifies each row as Confirmed, Discrepancy, or Addition. Results are stored in SQLite and surfaced in the BankSt OS Shell.

**Repos:**
| Repo | Purpose |
|---|---|
| `services/mapping_tools` | FastAPI backend — extraction engine + API |
| `apps/bankst-os-frontend` | Vanilla JS shell — workspace views + widgets |

Both repos are on `main`. The backend is served via PM2 process `mapping-tools-api`.

---

## Backend (`services/mapping_tools`)

### Key files
| File | Purpose |
|---|---|
| `src/api.py` | FastAPI app — all `/api/bbg/*` endpoints |
| `src/bbg_pipeline.py` | Extraction logic — matching, discrepancy detection |
| `src/bbg_db.py` | SQLite read/write — runs, confirmed, discrepancies, additions |

### SQLite database
Located at the path configured in `api.py` as `BBG_DB`. Tables:
- `bbg_runs` — one row per extraction run
- `bbg_confirmed` — confirmed matches per run
- `bbg_discrepancies` — field-level discrepancies per run
- `bbg_additions` — net-new names found in BBG not in master

### API endpoints

#### Firm/run summary
```
GET  /api/bbg/firms                              — all firms, latest run stats
GET  /api/bbg/firms/{firm_id}/runs               — all runs for a firm, newest first
GET  /api/bbg/firms/{firm_id}/discrepancy-persistence
```

#### Run data
```
GET  /api/bbg/runs/{run_id}/confirmed
GET  /api/bbg/runs/{run_id}/discrepancies
GET  /api/bbg/runs/{run_id}/additions
```

#### Upload / extraction
```
POST /api/bbg/upload                             — sync upload, returns result JSON
POST /api/bbg/upload/stream                      — SSE streaming upload (preferred)
```

The streaming endpoint (`/upload/stream`) runs the pipeline in a daemon thread and emits SSE events of the form:
```
data: {"type": "log"|"done"|"error", "msg": "...", "result": {...}}
```
`result` is only present on `type: "done"` events and contains the full run summary.

**Important:** `load_hf_persons_from_db()` reads HF records directly from SQLite (not via HTTP) to avoid a self-call deadlock through the gateway.

#### Delta / comparison
```
GET  /api/bbg/delta?run_a={id}&run_b={id}        — diff two runs from the same firm
```
Returns `{ run_a, run_b, confirmed: {added, removed}, discrepancies: {added, resolved}, additions: {added, resolved} }`.

#### Discrepancy persistence
```
GET  /api/bbg/firms/{firm_id}/discrepancy-persistence
```
Groups discrepancies by `(name, discrepancy_field, new_file_value)` across all runs with `run_count`, `first_seen`, `last_seen`.

---

## Frontend (`apps/bankst-os-frontend`)

### Files modified
| File | What was added |
|---|---|
| `js/views.js` | `bbg.firms` + `bbg.firm` workspace views, all chart helpers |
| `js/app.js` | Upload handler, SSE streaming, event wiring, cache-bust logic |
| `js/api.js` | `mappingUploadStream()` — fetch + streaming SSE reader |
| `js/drag.js` | Full-pane file drop detection for BBG tabs |
| `js/widgets.js` | `bbg-firm-run-history` right rail widget |
| `js/workspace.js` | `getActiveContext()` + `handleToolbarAction()` for BBG modes |
| `css/data-views.css` | All BBG grid classes, stat tiles, terminal, run history, analytics |
| `css/workspace.css` | `.pane.bbg-file-drop::after` drop overlay |

### View: `bbg.firms` (tab type `bbg.firms`)

**Toolbar:** Summary | Refresh

**Features:**
- CSV drag-drop upload zone — accepts OS file drags anywhere on the active pane (via `drag.js` + `bankst:bbgCsvDrop` event)
- Live terminal output during streaming extraction (SSE, lines appended via DOM)
- Stat tiles: Total Firms / Confirmed / Discrepancies / Additions
- Firm summary table sorted by tracking %
- Clicking a firm opens `bbg.firm` tab for that firm

**Table columns:** Firm | Confirmed | Discrepancies | Additions | Tracking % | Last Run

**Upload state machine:** `idle` → `dragging` → `streaming` → `success` | `error`

After a successful upload, the corresponding `bbg.firm` tab has its runs cache cleared (`runs: undefined`) and is re-opened to force a refresh.

### View: `bbg.firm` (tab type `bbg.firm`)

**`tab.entityId`** = `firm_id` UUID
**`tab.title`** = firm name

**Toolbar tabs:** Confirmed (N) | Discrepancies (N) | Additions (N) | Analytics | Delta | Persistence

#### Confirmed tab
7-column table with name search filter.
Columns: Name | Firm | Title | Function | Strategy | Products | Location

#### Discrepancies tab
7-column table with name search filter.
Columns: Name | Field | BBG Value | Master Value | Alias Info | Status | First Seen

#### Additions tab
6-column table with name search filter.
Columns: Name | BBG Company | Canonical | Title | Location | First Seen

#### Analytics tab
Multi-section layout:
1. **Stat tiles** — Confirmed / Discrepancies / Additions / Tracking % (coloured, 20px)
2. **Extraction Trends chart** — SVG line chart, all runs, area fills, dots at every point, tracking % dashed overlay with right Y-axis
3. **2-column row:**
   - Location Distribution — top 10 locations from confirmed records
   - Discrepancy Fields — top 8 fields with the most discrepancies (current run)
4. **Run History table** — all runs: date/time, filename, rows, conf/disc/add, tracking % bar

#### Delta tab
Compares any two runs from the same firm. Six diff sections (colour-coded):
- New Confirmations (green) / Lost Confirmations (red)
- New Discrepancies (orange) / Resolved Discrepancies (blue)
- New Additions (purple) / Resolved Additions (slate)

Two run selectors (`From` / `To`) that dispatch `bankst:bbgDeltaFetch` on change.

#### Persistence tab
Cross-run discrepancy frequency table. Groups by `(name, field, bbg_value)` across all runs.
Columns: Name | Field | BBG Value | Master Value | First Seen | Last Seen | Runs
Badge colours: grey (1 run), orange (2–3), red (4+).

**Design note:** Same name appearing at different firms across runs does NOT imply a job move — two different people can share a name at different shops. Persistence duration is surfaced as a signal, not a conclusion.

### Right rail widget: `bbg-firm-run-history`
Shown when active tab is `bbg.firm`. Lists all runs with timestamp, filename, conf/disc/add stats, tracking % — clickable to switch active run.

### CSS classes added (`data-views.css`)
```
.cell-mono              — monospace font, muted color, no-wrap
.meta-item              — bordered stat tile (flex column, label + value)
.meta-value--lg         — 20px bold stat value for analytics tiles
.bbg-stat-row           — 4-column horizontal stat strip (padding aligns with table)
.detail-view-shell--compact   — gap:12px (data-dense views)
.detail-view-shell--analytics — gap:20px (multi-section analytics)
.bbg-analytics-section  — section wrapper (flex col)
.bbg-analytics-label    — section header (uppercase, border-bottom)
.bbg-analytics-2col     — 2-column chart grid
.bbg-firms-grid         — 6-col: 2fr 78px 100px 75px 150px 88px
.bbg-confirmed-grid     — 7-col: 2fr 1.4fr 1.5fr 1fr 1fr 85px 115px
.bbg-disc-grid          — 7-col: 1.8fr 85px 1.2fr 1.2fr 0.8fr 65px 90px
.bbg-add-grid           — 6-col: 1.5fr 1.5fr 1.5fr 1fr 1fr 90px
.bbg-delta-conf-grid    — 4-col: 2fr 1.5fr 1.5fr 1.2fr
.bbg-delta-disc-grid    — 4-col: 2fr 90px 1.2fr 1.2fr
.bbg-delta-add-grid     — 4-col: 2fr 1.5fr 1.5fr 1.2fr
.bbg-persist-grid       — 7-col: 1.8fr 80px 1.2fr 1.2fr 90px 90px 70px
.bbg-runs-hist-grid     — 7-col: 110px 2fr 65px 52px 52px 48px 130px
.bbg-terminal           — monospace extraction log panel
.bbg-run-history-item   — right rail run item
.delta-section-header / .delta-count / .delta-zero
.persist-badge / .persist-low / .persist-mid / .persist-high
```

All BBG grid cells have `min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap` to prevent row height expansion from long values. BBG table rows override the global `padding-right:90px` (row-actions clearance) back to `24px` since BBG tables have no row-actions.

---

## Known Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| Direct SQLite read in `load_hf_persons_from_db()` | Calling `MAPPING_API_BASE/hf/records` routes through the gateway back to the same uvicorn process — HTTP self-call deadlock |
| `fetch()` + `response.body.getReader()` for SSE | `EventSource` only supports GET; streaming POST requires manual reader |
| Runs cache cleared on upload success | `onActivate` skips re-fetch if `tab.state.runs` exists — must explicitly clear to pick up new run |
| No job-move assertion on persistent discrepancies | Same name at different firms could be two people, not one person who moved |
| Excel additions import on ice | The sheet is extremely delicate — manual BBG CSV only for now |
| FINRA / scheduled processing / title-drift on ice | Buyside focus; manual GUI workflow; data quality too poor for drift |

---

## What's Next (Parked)

- **Additions import** — Excel import from BBG additions list (data structure is fragile, deferred)
- **FINRA cross-reference** — buyside focus, on ice
- **Scheduled processing** — manual GUI upload only, on ice
