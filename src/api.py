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

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import db

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HF_DB  = PROJECT_ROOT / "hf_map.db"
IR_DB  = PROJECT_ROOT / "ir_map.db"

app = FastAPI(title="Mapping Tools API")

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
def hf_records(include_inactive: bool = False):
    return db.hf_get_all(HF_DB, include_inactive=include_inactive)


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
def ir_records(include_inactive: bool = False):
    return db.ir_get_all(IR_DB, include_inactive=include_inactive)


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
