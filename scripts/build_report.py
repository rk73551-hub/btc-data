#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

PUBLIC_DIR = "public"
OUT_PATH = os.path.join(PUBLIC_DIR, "report.json")

FILES = {
    "dashboard": "dashboard.json",
    "insights_local": "insights_local.json",
    "latest": "latest.json",
    "last-24h": "last-24h.json",
    "90d": "90d.json",
    "ytd": "ytd.json",
    "2023": "2023.json",
    "2024": "2024.json",
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def safe_num(x):
    try:
        return float(x)
    except Exception:
        return None

def summarize_rows(obj):
    """
    Expects: { ok, version, count, rows:[{open,high,low,close,volume,time_utc,time_ny,composite_label,...}] }
    Returns summary only (no rows).
    """
    rows = obj.get("rows") or []
    closes = [safe_num(r.get("close")) for r in rows]
    closes = [c for c in closes if c is not None]
    if not rows or not closes:
        return {
            "ok": obj.get("ok", True),
            "version": obj.get("version"),
            "summary": {"count": len(rows)},
            "label_counts": {},
        }

    first = rows[0]
    last = rows[-1]

    highs = [safe_num(r.get("high")) for r in rows]
    lows  = [safe_num(r.get("low"))  for r in rows]
    highs = [h for h in highs if h is not None]
    lows  = [l for l in lows if l is not None]

    close_first = safe_num(first.get("close"))
    close_last  = safe_num(last.get("close"))
    change_pct = None
    if close_first and close_last and close_first != 0:
        change_pct = (close_last - close_first) / close_first * 100.0

    # label counts
    labels = [(r.get("composite_label") or "neutral").lower() for r in rows]
    lc = {"bullish": 0, "neutral": 0, "bearish": 0, "total": len(labels)}
    for L in labels:
        if L == "bullish":
            lc["bullish"] += 1
        elif L == "bearish":
            lc["bearish"] += 1
        else:
            lc["neutral"] += 1

    return {
        "ok": obj.get("ok", True),
        "version": obj.get("version"),
        "summary": {
            "count": len(rows),
            "first_time_utc": first.get("time_utc"),
            "last_time_utc": last.get("time_utc"),
            "close_first": close_first,
            "close_last": close_last,
            "close_change_pct": round(change_pct, 2) if change_pct is not None else None,
            "high_max": max(highs) if highs else None,
            "low_min": min(lows) if lows else None,
        },
        "label_counts": lc,
        "latest_label": last.get("composite_label"),
        "latest_signal_events": last.get("signal_events"),
    }

def summarize_timeseries_file(obj):
    """
    For your 90d/ytd/year files that are already compact,
    keep them as-is if they have summary/label_counts.
    If they accidentally include rows, summarize them.
    """
    if "rows" in obj:
        return summarize_rows(obj)
    return obj

def strip_raw_deep(d):
    """
    Remove large raw blocks if present.
    """
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        if k == "raw":
            continue
        out[k] = strip_raw_deep(v)
    return out

def main():
    os.makedirs(PUBLIC_DIR, exist_ok=True)

    status = {"ok": True, "missing_files": [], "errors": {}}
    data = {}

    # load what exists
    loaded = {}
    for key, fname in FILES.items():
        path = os.path.join(PUBLIC_DIR, fname)
        if not os.path.exists(path):
            status["ok"] = False
            status["missing_files"].append(fname)
            continue
        try:
            loaded[key] = load_json(path)
        except Exception as e:
            status["ok"] = False
            status["errors"][fname] = str(e)

    # dashboard: keep
    if "dashboard" in loaded:
        data["dashboard"] = loaded["dashboard"]

    # insights_local: keep but strip raw
    if "insights_local" in loaded:
        data["insights_local"] = strip_raw_deep(loaded["insights_local"])

    # latest: keep as-is (already 1 row)
    if "latest" in loaded:
        data["latest"] = loaded["latest"]

    # last-24h: SUMMARY ONLY (no rows)
    if "last-24h" in loaded:
        data["last-24h"] = summarize_rows(loaded["last-24h"])

    # 90d/ytd/years: keep compact or summarize if they contain rows
    for k in ["90d", "ytd", "2023", "2024"]:
        if k in loaded:
            data[k] = summarize_timeseries_file(loaded[k])

    out = {
        "generated_utc": utc_now_iso(),
        "schema": "btc-data-report-v1",
        "status": status,
        "data": data,
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
