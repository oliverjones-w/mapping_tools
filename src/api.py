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

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import json
import sqlite3
from datetime import date

import db
import bbg_db
import bbg_pipeline

import threading
from queue import Queue as _Queue

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
    allow_methods=["GET", "POST"],
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


@app.get("/api/bbg/runs/{run_id}/csv")
def bbg_run_csv(run_id: int):
    """Download the original CSV that was uploaded for this run."""
    raw, filename = bbg_db.get_csv_raw(BBG_DB, run_id)
    if raw is None:
        raise HTTPException(status_code=404, detail="No CSV stored for this run")
    return StreamingResponse(
        iter([raw]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/bbg/delta")
def bbg_delta(run_a: int = Query(...), run_b: int = Query(...)):
    """Diff between two runs. run_a = older (from), run_b = newer (to)."""
    meta_a = bbg_db.get_run(BBG_DB, run_a)
    meta_b = bbg_db.get_run(BBG_DB, run_b)
    if not meta_a or not meta_b:
        raise HTTPException(status_code=404, detail="One or both runs not found")
    if meta_a["firm_id"] != meta_b["firm_id"]:
        raise HTTPException(status_code=422, detail="Runs must be from the same firm")
    delta = bbg_db.get_delta(BBG_DB, run_a, run_b)
    delta["run_a"] = meta_a
    delta["run_b"] = meta_b
    return delta


@app.get("/api/bbg/firms/{firm_id}/discrepancy-persistence")
def bbg_discrepancy_persistence(firm_id: str):
    """All discrepancies for a firm grouped by persistence across runs."""
    return bbg_db.get_discrepancy_persistence(BBG_DB, firm_id)


@app.post("/api/bbg/upload")
async def bbg_upload(file: UploadFile = File(...)):
    """
    Accept a BBG CSV upload, validate it, run the extraction pipeline,
    and write the results to bbg_results.db.

    Firm is resolved from the filename: {firm_key}.csv or {firm_key}_YYYYMMDD.csv
    The firm_key must match a known firm_id from the BankSt firm registry.
    """
    content  = await file.read()
    filename = file.filename or "upload.csv"

    # 1. Validate CSV format
    ok, err = bbg_pipeline.validate_csv_columns(content)
    if not ok:
        raise HTTPException(status_code=422, detail=f"Invalid CSV format: {err}")

    # 2. Load firm registry
    try:
        alias_map, id_map, blacklist_map, name_map = bbg_pipeline.load_firm_aliases()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not load firm registry: {exc}")

    # 3. Resolve firm from filename
    firm_id = bbg_pipeline.resolve_firm_from_filename(filename, id_map)
    if not firm_id:
        from pathlib import Path as _Path
        stem = _Path(filename).stem.split("_")[0]
        raise HTTPException(
            status_code=422,
            detail=(
                f"Filename '{filename}' does not match a known firm key. "
                f"'{stem}' was not found in the firm registry. "
                f"Rename the file to {{firm_key}}.csv (e.g. alphadyne.csv)."
            ),
        )

    firm_name = name_map.get(firm_id, firm_id)

    # 4. Load HF map — read directly from SQLite to avoid HTTP self-call deadlock
    try:
        person_map, _ = bbg_pipeline.load_hf_persons_from_db(HF_DB)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not load HF map: {exc}")

    # 5. Run pipeline
    blacklist                      = blacklist_map.get(firm_id, set())
    confirmed, disc_json, additions = bbg_pipeline.process_csv(
        content, filename, person_map, alias_map, blacklist
    )
    disc_rows = bbg_pipeline.flatten_discrepancies(disc_json)
    today     = str(date.today())

    # 6. Write to DB
    run_id = bbg_db.create_run(
        db_path           = BBG_DB,
        firm_id           = firm_id,
        firm_name         = firm_name,
        csv_filename      = filename,
        source_type       = "upload",
        rows_processed    = len(confirmed) + len(disc_rows) + len(additions),
        confirmed_count   = len(confirmed),
        discrepancy_count = len(disc_rows),
        addition_count    = len(additions),
        csv_raw           = content,
    )

    bbg_db.insert_confirmed(BBG_DB, run_id, [
        {
            "run_id": run_id, "firm_id": firm_id,
            "hf_record_id": r.get("id"),  "name": r.get("name"),
            "firm": r.get("firm"),         "title": r.get("title"),
            "location": r.get("location"), "function": r.get("function"),
            "strategy": r.get("strategy"), "products": r.get("products"),
            "reports_to": r.get("reports_to"),
        }
        for r in confirmed
    ])

    bbg_db.insert_discrepancies(BBG_DB, run_id, [
        {**row, "run_id": run_id, "firm_id": firm_id}
        for row in disc_rows
    ])

    bbg_db.insert_additions(BBG_DB, run_id, [
        {
            "run_id": run_id,           "firm_id": firm_id,
            "name": row["name"],         "company": row["company"],
            "canonical_company": row["canonical_company"],
            "title": row.get("title"),   "location": row.get("location"),
            "focus": row.get("focus"),   "source_file": row["source_file"],
            "first_seen": today,
        }
        for row in additions
    ])

    return {
        "run_id":           run_id,
        "firm_id":          firm_id,
        "firm_name":        firm_name,
        "csv_filename":     filename,
        "rows_processed":   len(confirmed) + len(disc_rows) + len(additions),
        "confirmed_count":  len(confirmed),
        "discrepancy_count": len(disc_rows),
        "addition_count":   len(additions),
    }


@app.post("/api/bbg/upload/stream")
async def bbg_upload_stream(file: UploadFile = File(...)):
    """
    Same extraction logic as /api/bbg/upload, but returns an SSE stream of
    real-time log messages so the client can show a live terminal view.
    """
    content  = await file.read()
    filename = file.filename or "upload.csv"

    q: _Queue = _Queue()

    def run() -> None:
        def log(msg: str) -> None:
            q.put({"type": "log", "payload": msg})

        try:
            # 1. Validate CSV
            log("Validating CSV format...")
            ok, err = bbg_pipeline.validate_csv_columns(content)
            if not ok:
                q.put({"type": "error", "payload": f"Invalid CSV: {err}"}); return

            import csv as _csv, io as _io
            row_count = max(0, sum(
                1 for _ in _csv.reader(_io.StringIO(content.decode("utf-8-sig", errors="replace")))
            ) - 1)
            log(f"CSV OK — {row_count} data rows detected")

            # 2. Load firm registry
            log("Loading firm registry...")
            alias_map, id_map, blacklist_map, name_map = bbg_pipeline.load_firm_aliases()
            log(f"Firm registry: {len(name_map)} firms, {len(id_map)} aliases")

            # 3. Resolve firm from filename
            log(f"Resolving firm from '{filename}'...")
            firm_id = bbg_pipeline.resolve_firm_from_filename(filename, id_map)
            if not firm_id:
                from pathlib import Path as _P
                stem = _P(filename).stem.split("_")[0]
                q.put({"type": "error", "payload": (
                    f"'{stem}' does not match any known firm key. "
                    f"Rename the file to {{firm_key}}.csv (e.g. alphadyne.csv)."
                )}); return
            firm_name = name_map.get(firm_id, firm_id)
            log(f"Firm identified: {firm_name}")

            # 4. Load HF map
            log("Loading HF map from database...")
            person_map, _ = bbg_pipeline.load_hf_persons_from_db(HF_DB)
            total_records = sum(len(v) for v in person_map.values())
            log(f"HF map: {len(person_map)} unique names, {total_records} records")

            # 5. Run extraction pipeline
            log("Running extraction pipeline...")
            blacklist = blacklist_map.get(firm_id, set())
            confirmed, disc_json, additions = bbg_pipeline.process_csv(
                content, filename, person_map, alias_map, blacklist
            )
            disc_rows = bbg_pipeline.flatten_discrepancies(disc_json)
            log(f"Pipeline complete: {len(confirmed)} confirmed / {len(disc_rows)} discrepancies / {len(additions)} additions")

            # 6. Write to database
            log("Writing results to database...")
            today = str(date.today())
            run_id = bbg_db.create_run(
                db_path=BBG_DB, firm_id=firm_id, firm_name=firm_name,
                csv_filename=filename, source_type="upload",
                rows_processed=len(confirmed) + len(disc_rows) + len(additions),
                confirmed_count=len(confirmed),
                discrepancy_count=len(disc_rows),
                addition_count=len(additions),
                csv_raw=content,
            )
            bbg_db.insert_confirmed(BBG_DB, run_id, [
                {
                    "run_id": run_id, "firm_id": firm_id,
                    "hf_record_id": r.get("id"),  "name": r.get("name"),
                    "firm": r.get("firm"),          "title": r.get("title"),
                    "location": r.get("location"),  "function": r.get("function"),
                    "strategy": r.get("strategy"),  "products": r.get("products"),
                    "reports_to": r.get("reports_to"),
                }
                for r in confirmed
            ])
            bbg_db.insert_discrepancies(BBG_DB, run_id, [
                {**row, "run_id": run_id, "firm_id": firm_id}
                for row in disc_rows
            ])
            bbg_db.insert_additions(BBG_DB, run_id, [
                {
                    "run_id": run_id, "firm_id": firm_id,
                    "name": row["name"], "company": row["company"],
                    "canonical_company": row["canonical_company"],
                    "title": row.get("title"), "location": row.get("location"),
                    "focus": row.get("focus"), "source_file": row["source_file"],
                    "first_seen": today,
                }
                for row in additions
            ])
            log(f"Run #{run_id} saved to database")

            q.put({"type": "done", "payload": {
                "run_id":            run_id,
                "firm_id":           firm_id,
                "firm_name":         firm_name,
                "csv_filename":      filename,
                "rows_processed":    len(confirmed) + len(disc_rows) + len(additions),
                "confirmed_count":   len(confirmed),
                "discrepancy_count": len(disc_rows),
                "addition_count":    len(additions),
            }})

        except Exception as exc:
            q.put({"type": "error", "payload": str(exc)})

    threading.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            event = q.get()
            yield f"data: {json.dumps(event)}\n\n"
            if event["type"] in ("done", "error"):
                break

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
