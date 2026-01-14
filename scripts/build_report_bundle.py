#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

PUBLIC_DIR = "public"

# Inputs we expect to already exist in public/
FILES = {
    "latest": "latest.json",
    "last-24h": "last-24h.json",
    "90d": "90d.json",
    "ytd": "ytd.json",
    "2023": "2023.json",
    "2024": "2024.json",
    "dashboard": "dashboard.json",
    "insights_local": "insights_local.json",
}

OUT_SMALL = os.path.join(PUBLIC_DIR, "report.json")
OUT_FULL = os.path.join(PUBLIC_DIR, "report_full.json")

# --- helpers ---

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=False)

def is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and x == x

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def rows_from_dataset(ds: Any) -> List[Dict[str, Any]]:
    """
    Your JSONs are shaped like:
      { ok:true, count:n, rows:[{...}] }
    """
    if not isinstance(ds, dict):
        return []
    rows = ds.get("rows")
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []

def last_n_rows(ds: Any, n: int) -> List[Dict[str, Any]]:
    r = rows_from_dataset(ds)
    return r[-n:] if len(r) > n else r

def compute_basic_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [safe_float(r.get("close")) for r in rows]
    closes = [c for c in closes if c is not None]
    if not closes:
        return {"count": len(rows), "close_min": None, "close_max": None, "close_last": None, "close_change_pct": None}

    close_min = min(closes)
    close_max = max(closes)
    close_last = closes[-1]
    close_first = closes[0]
    chg_pct = None
    if close_first and close_first != 0:
        chg_pct = ((close_last - close_first) / close_first) * 100.0

    return {
        "count": len(rows),
        "close_min": round(close_min, 2),
        "close_max": round(close_max, 2),
        "close_last": round(close_last, 2),
        "close_change_pct": round(chg_pct, 2) if chg_pct is not None else None,
    }

def summarize_labels(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    labels = [(r.get("composite_label") or "").lower() for r in rows]
    bull = sum(1 for l in labels if l == "bullish")
    bear = sum(1 for l in labels if l == "bearish")
    neu = len(labels) - bull - bear
    return {"bullish": bull, "neutral": neu, "bearish": bear, "total": len(labels)}

def dataset_summary(ds: Any, tail_rows: int = 72) -> Dict[str, Any]:
    """
    For large datasets, keep only:
      - stats over full set
      - label counts over full set
      - last N rows (tail)
      - latest timestamp
    """
    rows = rows_from_dataset(ds)
    tail = rows[-tail_rows:] if len(rows) > tail_rows else rows

    latest_time_utc = None
    if rows:
        latest_time_utc = rows[-1].get("time_utc")

    return {
        "ok": bool(ds.get("ok")) if isinstance(ds, dict) else None,
        "version": ds.get("version") if isinstance(ds, dict) else None,
        "count": len(rows),
        "latest_time_utc": latest_time_utc,
        "stats": compute_basic_stats(rows),
        "label_counts": summarize_labels(rows),
        "tail_rows": tail,  # keeps it interpretable without being huge
    }

def file_exists(path: str) -> bool:
    try:
        return os.path.exists(path) and os.path.getsize(path) > 0
    except Exception:
        return False

def main() -> None:
    # Validate inputs
    missing = []
    errors: Dict[str, str] = {}
    loaded: Dict[str, Any] = {}

    for key, fname in FILES.items():
        path = os.path.join(PUBLIC_DIR, fname)
        if not file_exists(path):
            missing.append(fname)
            continue
        try:
            loaded[key] = read_json(path)
        except Exception as e:
            errors[fname] = str(e)

    ok = (len(missing) == 0 and len(errors) == 0)

    # Always write something, even if not ok
    report: Dict[str, Any] = {
        "generated_utc": utc_now_iso(),
        "schema": "btc-data-report-v1",
        "status": {
            "ok": ok,
            "missing_files": missing,
            "errors": errors,
        },
        "data": {}
    }

    # SMALL report payload design:
    # - include full payload for: dashboard + insights_local (already small)
    # - include full rows for: latest (1 row) and last-24h (24 rows)
    # - summarize large sets: 90d, ytd, 2023, 2024 (with tail rows only)
    if "dashboard" in loaded:
        report["data"]["dashboard"] = loaded["dashboard"]
    if "insights_local" in loaded:
        report["data"]["insights_local"] = loaded["insights_local"]

    if "latest" in loaded:
        report["data"]["latest"] = loaded["latest"]  # already tiny (count=1)
    if "last-24h" in loaded:
        report["data"]["last-24h"] = loaded["last-24h"]  # 24 rows

    # Large datasets => summary + tail
    for k in ["90d", "ytd", "2023", "2024"]:
        if k in loaded:
            # Tail sizes: keep smaller for yearly archives
            tail_n = 72 if k in ["90d", "ytd"] else 48
            report["data"][k] = dataset_summary(loaded[k], tail_rows=tail_n)

    write_json(OUT_SMALL, report)

    # Optional: also write a full raw bundle if you want it (off by default)
    # Enable by setting environment variable REPORT_FULL=1 in workflow.
    if os.getenv("REPORT_FULL", "").strip() in ("1", "true", "TRUE", "yes", "YES"):
        full = {
            "generated_utc": report["generated_utc"],
            "schema": "btc-data-report-full-v1",
            "status": report["status"],
            "data": loaded,
        }
        write_json(OUT_FULL, full)

    print(f"Wrote {OUT_SMALL}")
    if os.path.exists(OUT_FULL):
        print(f"Wrote {OUT_FULL}")

if __name__ == "__main__":
    main()
