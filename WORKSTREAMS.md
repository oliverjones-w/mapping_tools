# mapping_tools Workstreams

Last updated: 2026-04-27

This file explains how active work in `mapping_tools` is split between the default branch and feature branches.

## Default Branch

- Branch: `main`
- Purpose: current production mapping ingestion and API work
- Expected scope:
  - HF / IR sync
  - BBG extraction pipeline
  - scheduler / transport reliability
  - repo-local guidance

Do not use `main` as a scratch branch for speculative map expansion.

## Expansion Branch

- Branch: `feat/map-expansion`
- Purpose: staged expansion of the PowerShell / SQLite sync model to additional maps
- Current scope:
  - credit
  - commodities
  - equities
  - FX
  - investment banking
  - digital assets

The branch exists so the extra map families can evolve without polluting `main` before the ingestion paths, DB files, scheduling, and runtime promotion plan are ready.

## Cross-Repo Governance

Cross-repo issue tracking and system-wide project coordination live in `platform-docs`, not in this repo.

- GitHub: `https://github.com/oliverjones-w/platform-docs`
- Project board: `bankst-os Platform Board`
