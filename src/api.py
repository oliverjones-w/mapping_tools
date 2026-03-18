"""
Mapping Tools — JSON API

Exposes hf_map.db (Hedge Fund Map) and ir_map.db (Interest Rates Map)
as a read-only REST API for the BankSt OS shell.

Run:
    uvicorn src.api:app --host 0.0.0.0 --port 8003
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import json
import sqlite3

import db
import bbg_db

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HF_DB  = PROJECT_ROOT / "hf_map.db"
IR_DB  = PROJECT_ROOT / "ir_map.db"
BBG_DB = PROJECT_ROOT / "bbg_results.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    bbg_db.init_db(BBG_DB)
    yield


app = FastAPI(title="Mapping Tools API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# HF Map endpoints
# ---------------------------------------------------------------------------

@app.get("/api/hf/summary")
def hf_summary():
    return db.get_summary(HF_DB)


@app.get("/api/hf/records")
async def hf_records(include_inactive: bool = False, limit: Optional[int] = None, offset: int = 0):
    where = "" if include_inactive else "WHERE is_active = 1"
    pagination = f" LIMIT {limit} OFFSET {offset}" if limit is not None else (f" LIMIT -1 OFFSET {offset}" if offset else "")

    def generate_json():
        conn = sqlite3.connect(str(HF_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM records {where} ORDER BY firm, name{pagination}")
        yield "["
        first = True
        while True:
            row = cur.fetchone()
            if row is None:
                break
            if not first:
                yield ","
            yield json.dumps(dict(row), default=str)
            first = False
        yield "]"
        conn.close()

    return StreamingResponse(generate_json(), media_type="application/json")


@app.get("/api/hf/records/{record_id}")
def hf_record(record_id: str):
    row = db.hf_get_one(HF_DB, record_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    row["history"] = db.get_record_history(HF_DB, record_id)
    return row


@app.get("/api/hf/firms")
def hf_firms():
    return db.hf_get_firms(HF_DB)


@app.get("/api/hf/changes")
def hf_changes(limit: int = Query(default=50, le=500)):
    return db.get_recent_changes(HF_DB, limit=limit)


@app.get("/api/hf/moves")
def hf_moves(limit: int = Query(default=50, le=500)):
    return db.hf_get_recent_moves(HF_DB, limit=limit)


@app.get("/api/hf/search")
def hf_search(q: str = Query(default="", min_length=1), limit: int = Query(default=100, le=500)):
    return db.hf_search(HF_DB, q=q, limit=limit)


@app.get("/api/hf/daily-changes")
def hf_daily_changes(days: int = Query(default=60, le=365)):
    return db.get_daily_change_counts(HF_DB, days=days)


# ---------------------------------------------------------------------------
# IR Map endpoints
# ---------------------------------------------------------------------------

@app.get("/api/ir/summary")
def ir_summary():
    return db.get_summary(IR_DB)


@app.get("/api/ir/records")
async def ir_records(include_inactive: bool = False, limit: Optional[int] = None, offset: int = 0):
    where = "" if include_inactive else "WHERE is_active = 1"
    pagination = f" LIMIT {limit} OFFSET {offset}" if limit is not None else (f" LIMIT -1 OFFSET {offset}" if offset else "")

    def generate_json():
        conn = sqlite3.connect(str(IR_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM records {where} ORDER BY current_firm, name{pagination}")
        yield "["
        first = True
        while True:
            row = cur.fetchone()
            if row is None:
                break
            if not first:
                yield ","
            yield json.dumps(dict(row), default=str)
            first = False
        yield "]"
        conn.close()

    return StreamingResponse(generate_json(), media_type="application/json")


@app.get("/api/ir/records/{record_id}")
def ir_record(record_id: str):
    row = db.ir_get_one(IR_DB, record_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    row["history"] = db.get_record_history(IR_DB, record_id)
    return row


@app.get("/api/ir/firms")
def ir_firms():
    return db.ir_get_firms(IR_DB)


@app.get("/api/ir/changes")
def ir_changes(limit: int = Query(default=50, le=500)):
    return db.get_recent_changes(IR_DB, limit=limit)


@app.get("/api/ir/moves")
def ir_moves(limit: int = Query(default=50, le=500)):
    return db.ir_get_recent_moves(IR_DB, limit=limit)


@app.get("/api/ir/search")
def ir_search(q: str = Query(default="", min_length=1), limit: int = Query(default=100, le=500)):
    return db.ir_search(IR_DB, q=q, limit=limit)


@app.get("/api/ir/daily-changes")
def ir_daily_changes(days: int = Query(default=60, le=365)):
    return db.get_daily_change_counts(IR_DB, days=days)


# ---------------------------------------------------------------------------
# BBG Extraction endpoints
# ---------------------------------------------------------------------------

@app.get("/api/bbg/firms")
def bbg_firms():
    """All firms with their latest run stats and tracking %."""
    return bbg_db.get_firms_summary(BBG_DB)


@app.get("/api/bbg/firms/{firm_id}/runs")
def bbg_firm_runs(firm_id: str):
    """Full run history for a single firm, newest first."""
    return bbg_db.get_runs_for_firm(BBG_DB, firm_id)


@app.get("/api/bbg/firms/{firm_id}/latest")
def bbg_firm_latest(firm_id: str):
    """Metadata + all data for the latest run for a firm."""
    run_id = bbg_db.get_latest_run_id(BBG_DB, firm_id)
    if run_id is None:
        raise HTTPException(status_code=404, detail=f"No runs found for firm '{firm_id}'")
    run = bbg_db.get_run(BBG_DB, run_id)
    run["confirmed"]    = bbg_db.get_confirmed_for_run(BBG_DB, run_id)
    run["discrepancies"] = bbg_db.get_discrepancies_for_run(BBG_DB, run_id)
    run["additions"]    = bbg_db.get_additions_for_run(BBG_DB, run_id)
    return run


@app.get("/api/bbg/runs/{run_id}")
def bbg_run(run_id: int):
    """Metadata for a single run."""
    run = bbg_db.get_run(BBG_DB, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/api/bbg/runs/{run_id}/confirmed")
def bbg_run_confirmed(run_id: int):
    return bbg_db.get_confirmed_for_run(BBG_DB, run_id)


@app.get("/api/bbg/runs/{run_id}/discrepancies")
def bbg_run_discrepancies(run_id: int):
    return bbg_db.get_discrepancies_for_run(BBG_DB, run_id)


@app.get("/api/bbg/runs/{run_id}/additions")
def bbg_run_additions(run_id: int):
    return bbg_db.get_additions_for_run(BBG_DB, run_id)
