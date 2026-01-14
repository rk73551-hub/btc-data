#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

PUBLIC_DIR = "public"

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

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=False)

def file_exists(path: str) -> bool:
    try:
        return os.path.exists(path) and os.path.getsize(path) > 0
    except Exception:
        return False

def rows_from_dataset(ds: Any) -> List[Dict[str, Any]]:
    if not isinstance(ds, dict):
        return []
    rows = ds.get("rows")
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []

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

def strip_raw_recursive(obj: Any) -> Any:
    """
    Recursively remove any key named 'raw' anywhere in the object.
    This is what makes insights_local (and sometimes other feeds) explode in size.
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "raw":
                continue
            out[k] = strip_raw_recursive(v)
        return out
    if isinstance(obj, list):
        return [strip_raw_recursive(x) for x in obj]
    return obj

def summarize_dataset(ds: Any) -> Dict[str, Any]:
    """
    For big datasets: include only metadata + basic price stats + label counts.
    NO rows included here (keeps report.json small).
    """
    rows = rows_from_dataset(ds)
    closes = [safe_float(r.get("close")) for r in rows]
    closes = [c for c in closes if c is not None]

    labels = [(r.get("composite_label") or "").lower() for r in rows]
    bullish = sum(1 for l in labels if l == "bullish")
    bearish = sum(1 for l in labels if l == "bearish")
    neutral = len(labels) - bullish - bearish

    latest_time_utc = rows[-1].get("time_utc") if rows else None

    stats = {
        "count": len(rows),
        "latest_time_utc": latest_time_utc,
        "close_last": round(closes[-1], 2) if closes else None,
        "close_min": round(min(closes), 2) if closes else None,
        "close_max": round(max(closes), 2) if closes else None,
    }

    if closes and closes[0] and closes[0] != 0:
        stats["close_change_pct"] = round(((closes[-1] - closes[0]) / closes[0]) * 100.0, 2)
    else:
        stats["close_change_pct"] = None

    return {
        "ok": bool(ds.get("ok")) if isinstance(ds, dict) else None,
        "version": ds.get("version") if isinstance(ds, dict) else None,
        "summary": stats,
        "label_counts": {"bullish": bullish, "neutral": neutral, "bearish": bearish, "total": len(labels)},
    }

def main() -> None:
    loaded: Dict[str, Any] = {}
    missing: List[str] = []
    errors: Dict[str, str] = {}

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

    # 1) dashboard is already compact and important
    if "dashboard" in loaded:
        report["data"]["dashboard"] = loaded["dashboard"]

    # 2) insights_local: STRIP raw payloads so it stays small
    if "insights_local" in loaded:
        report["data"]["insights_local"] = strip_raw_recursive(loaded["insights_local"])

    # 3) latest (1 row) keep
    if "latest" in loaded:
        report["data"]["latest"] = loaded["latest"]

    # 4) last-24h (24 rows) keep
    if "last-24h" in loaded:
        report["data"]["last-24h"] = loaded["last-24h"]

    # 5) Big datasets: summary ONLY (NO ROWS)
    for k in ["90d", "ytd", "2023", "2024"]:
        if k in loaded:
            report["data"][k] = summarize_dataset(loaded[k])

    write_json(OUT_SMALL, report)
    print(f"Wrote {OUT_SMALL}")

if __name__ == "__main__":
    main()
