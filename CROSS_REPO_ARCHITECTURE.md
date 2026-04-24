# Bay Street Cross-Repo Architecture

Last updated: 2026-04-24

This document captures current runtime reality across the Dell and Mac copies of the mapping / BBG / dashboard stack. It is intended to replace mixed-era assumptions from older docs.

## Current Reality

- Dell is the ingestion host for mapping data.
- Mac is the authoritative runtime host for the frontend, gateway, and app-serving stack.
- Tailscale connects Dell to Mac for private transport.
- Cloudflare Tunnel exposes the Mac-hosted application to `bankst.co`.
- The Dell `unified_css` clone is a stale local mirror and is not the authoritative frontend source.
- BBG backend and BBG frontend are both live and wired.

## Spatial Map

```text
+----------------------------------+    +--------------------------------------------------+    +----------------------+
| DELL (Windows / C:\dev)          |    | MAC (dev-server / Tailscale peer)                |    | PUBLIC EDGE          |
+----------------------------------+    +--------------------------------------------------+    +----------------------+

  INGESTION                               RUNTIME APIS
  ---------                               ------------

  Excel maps (HF / IR contacts)           gateway :7842
    -> scripts/sync_hf_map.py               /api/core    -> core API :8765
    -> scripts/sync_ir_map.py               /api/finra   -> finra :8001
    -> scripts/sync_and_push.ps1            /api/mapping -> mapping :8003
    -> hf_map.db                            /api/encore  -> encore :5050
    -> ir_map.db                            /system      -> :9000 (unused route)
    -> bbg_results.db

  BBG CSV upload                          mapping_tools :8003
    -> bankst-os-frontend UI               - /api/hf/*
    -> /api/mapping/bbg/upload             - /api/ir/*
    -> bbg_pipeline.py                     - /api/bbg/*
    -> bbg_results.db                      - reads hf_map.db / ir_map.db / bbg_results.db

  TRANSPORT
  ---------

  Hourly SCP over Tailscale/SSH
    Dell hf_map.db      -> Mac runtime copy
    Dell ir_map.db      -> Mac runtime copy
    Dell bbg_results.db -> Mac runtime copy

  FRONTEND / DEV PROXY                     FRONTEND
  --------------------                     --------

  Dell Caddyfile                           bankst-os-frontend :3000
    localhost file_server                    - authoritative frontend
    /api/*    -> Mac :7842                   - active dev via VS Code remote
    /system/* -> Mac :7842

  Dell unified_css clone                  DATA
    - stale local mirror                   PostgreSQL :5432 / bankst_os
    - not source of truth                    canonical models:
                                               apps/hf_returns_app/models.py
                                             migrations:
                                               Alembic 0001 -> 0009 + merge

                                            OTHER LIVE INGESTION
                                            --------------------
                                            finra_scraper :8001
                                            bankst-swarm :5050
                                            encore_scraper :5050

                                                                              Cloudflare Tunnel
                                                                                -> bankst.co
```

## Repo / Component Roles

### Dell-side repos

- `C:\dev\tools\mapping_tools`
  - Active ingestion repo for HF / IR map sync.
  - Contains the mapping FastAPI implementation and BBG pipeline source.
  - Produces `hf_map.db`, `ir_map.db`, and `bbg_results.db`.

- `C:\dev\labs\unified_css`
  - Stale local clone of the frontend repo.
  - Useful as a local reference only.
  - Not the authoritative runtime/frontend source.

### Mac-side authoritative runtime

- `bankst-os-frontend`
  - Live frontend.
  - Contains BBG UI wiring, including `bbg.firms` and `bbg.firm`.
  - Uploads BBG CSVs to the mapping API through the gateway.

- Gateway on `:7842`
  - Entry point for Mac-hosted API routing.
  - Reaches core, FINRA, mapping, and Encore services.

## Status Table

| Component | Status | Notes |
|---|---|---|
| Dell HF/IR Excel -> SQLite sync | Live | Hourly PowerShell automation on Dell |
| Dell -> Mac SCP transport over Tailscale | Live | Operational transport leg for SQLite artifacts |
| Mapping API `:8003` | Live | Current endpoint surface is `hf/*`, `ir/*`, `bbg/*` |
| BBG FastAPI backend | Live | `src/api.py` + `src/bbg_pipeline.py` + `bbg_results.db` |
| BBG frontend UI | Live | `bbg.firms` + `bbg.firm`, drag-drop CSV upload |
| BBG upload path | Live | UI -> gateway -> `POST /api/bbg/upload` -> pipeline -> SQLite |
| Mac gateway `:7842` | Live | Main API entry point behind Tailscale |
| Mac frontend `bankst-os-frontend :3000` | Live | Authoritative frontend runtime |
| Cloudflare Tunnel -> `bankst.co` | Live | Public serving edge |
| Dell Caddyfile | Live (dev-only) | Local dev proxy to Mac, not production edge |
| `mapping_tools/dashboard.py` Streamlit app | Legacy | Superseded by FastAPI mapping API |
| Dell `unified_css` clone | Stale | Local mirror only |
| Gateway `/system -> :9000` | Unused | Route exists, no backing process |
| Extra map families in non-runtime branches | Planned / branch-specific | Not part of confirmed Mac runtime surface |

## Key Contrasts With Older Docs

- The old Streamlit / filesystem BBG workflow is no longer the primary runtime architecture.
- The Mac, not the Dell, is the authoritative frontend runtime host.
- Dell Caddy is a local development proxy, not the production serving layer.
- Public exposure happens through Cloudflare Tunnel from the Mac to `bankst.co`.
- BBG is not planned-only; the BBG backend and frontend are both live.

## Open Questions

- Whether any non-runtime branches still carry extra mapping API families that should be either promoted or removed.
- Whether the unused gateway `/system` route should be retired or documented with its intended future owner.
