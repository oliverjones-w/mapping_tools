# mapping_tools

Dell-side mapping ingestion and mapping API for `bankst-os`.

## Macro Context

Part of `bankst-os`. For system context, read platform-docs first:
- GitHub: https://github.com/oliverjones-w/platform-docs
- Local (Dell): C:\dev\platform-docs
- Start at: `agent/ENTRY.md`

## What This Repo Owns

- HF / IR Excel → SQLite sync automation
- BBG pipeline and BBG results DB
- Mapping FastAPI implementation (`src/api.py`)
- SCP push to Mac runtime

## What This Repo Does Not Own

- Frontend runtime behavior (Mac `bankst-os-frontend`)
- Cross-repo system context (platform-docs)
- Live work tracking (GitHub Issues / board)

## Key Paths

| Path | Purpose |
|---|---|
| `scripts/sync_and_push.ps1` | Hourly scheduled sync — Excel → SQLite → SCP to Mac |
| `scripts/sync_hf_map.py` | HF map sync |
| `scripts/sync_ir_map.py` | IR map sync |
| `src/api.py` | Mapping FastAPI entry point |
| `src/bbg_pipeline.py` | BBG pipeline |
| `logs/sync.log` | Sync log — check here for transport failures |
| `hf_map.db` / `ir_map.db` / `bbg_results.db` | SQLite outputs |

## Running Locally

```bash
# activate venv
source .venv/Scripts/activate   # Windows/bash
.venv\Scripts\activate           # Windows/PowerShell

# start mapping API
uvicorn src.api:app --reload --port 8003
```

## SCP Push Targets (Dell → Mac)

| File | Mac destination |
|---|---|
| `hf_map.db` | `macdev:/Users/dev-server/workspace/services/mapping_tools/hf_map.db` |
| `ir_map.db` | `macdev:/Users/dev-server/workspace/services/mapping_tools/ir_map.db` |
| `bbg_results.db` | `macdev:/Users/dev-server/workspace/services/mapping_tools/bbg_results.db` |
