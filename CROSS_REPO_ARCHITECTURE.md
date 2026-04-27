# mapping_tools Cross-Repo Pointer

Last updated: 2026-04-27

`mapping_tools` is part of the broader `bankst-os` stack, but this repo is not the canonical home for cross-repo architecture or project governance.

## What This Repo Owns

- Dell-side mapping ingestion automation
- HF / IR SQLite generation
- mapping API implementation source
- BBG extraction pipeline source

## Canonical Cross-Repo Context

For the current system map, operating rules, and live project context, use `platform-docs` first.

- GitHub: `https://github.com/oliverjones-w/platform-docs`
- Start with:
  1. `BOOTSTRAP.md`
  2. `CONTEXT_TREE.md`
  3. `SYSTEM_MAP.md`
  4. `INFRASTRUCTURE_MAP.md`

## Repo-Local Notes

- The authoritative frontend runtime lives on the Mac in `bankst-os-frontend`.
- The Dell `unified_css` clone is non-authoritative and should be treated as a reference only.
- Current production mapping surface is HF / IR / BBG.
- Additional map families are being isolated on `feat/map-expansion` until they are ready for promotion.
