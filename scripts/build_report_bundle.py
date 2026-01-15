#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

IN_DIR = "public"
OUT_PATH = os.path.join(IN_DIR, "report.json")

# keep only last N candles from last-24h to keep report tiny
LAST_24H_KEEP_ROWS = int(os.getenv("REPORT_LAST24H_ROWS", "3"))

FILES = {
    "dashboard": "dashboard.json",
    "tier1": "tier1.json",
    "latest": "latest.json",
    "last-24h": "last-24h.json",
    "90d": "90d.json",
    "ytd": "ytd.json",
    "2023": "2023.json",
    "2024": "2024.json",
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def compact_insights_local(obj):
    # Remove huge raw blobs if they exist (price/raw, funding/raw, etc.)
    if not isinstance(obj, dict):
        return obj
    out = dict(obj)
    for k in ("price", "funding", "macro"):
        if isinstance(out.get(k), dict) and "raw" in out[k]:
            out[k] = dict(out[k])
            out[k].pop("raw", None)
    return out

def summarize_last24h(obj):
    """
    Input shape (your dataset):
      { ok, version, count, rows:[{open,high,low,close,volume,...}, ...] }
    Output:
      {
        ok, version,
        summary:{open_first, close_last, high, low, change_pct, volume_sum, bars},
        rows_tail:[...last N rows...]
      }
    """
    if not isinstance(obj, dict):
        return {"ok": False, "error": "invalid_json"}

    rows = obj.get("rows") or []
    if not rows:
        return {
            "ok": bool(obj.get("ok")),
            "version": obj.get("version"),
            "summary": {"bars": 0},
            "rows_tail": [],
        }

    # guard numeric extraction
    def fnum(x):
        try:
            return float(x)
        except Exception:
            return None

    open_first = fnum(rows[0].get("open"))
    close_last = fnum(rows[-1].get("close"))

    highs = [fnum(r.get("high")) for r in rows if r.get("high") is not None]
    lows  = [fnum(r.get("low")) for r in rows if r.get("low") is not None]
    vols  = [fnum(r.get("volume")) for r in rows if r.get("volume") is not None]

    high = max([v for v in highs if v is not None], default=None)
    low  = min([v for v in lows if v is not None], default=None)
    volume_sum = sum([v for v in vols if v is not None]) if vols else None

    change_pct = None
    if open_first not in (None, 0) and close_last is not None:
        change_pct = (close_last / open_first - 1.0) * 100.0

    tail = rows[-LAST_24H_KEEP_ROWS:] if LAST_24H_KEEP_ROWS > 0 else []

    return {
        "ok": bool(obj.get("ok")),
        "version": obj.get("version"),
        "summary": {
            "bars": len(rows),
            "open_first": open_first,
            "close_last": close_last,
            "high": high,
            "low": low,
            "change_pct": change_pct,
            "volume_sum": volume_sum,
            "first_time_utc": rows[0].get("time_utc"),
            "last_time_utc": rows[-1].get("time_utc"),
        },
        "rows_tail": tail,
    }

def keep_summary_only(obj):
    """
    For 90d/ytd/2023/2024: you already store compact summary + label_counts.
    Just keep those top-level fields and never include rows.
    """
    if not isinstance(obj, dict):
        return {"ok": False, "error": "invalid_json"}

    out = {
        "ok": bool(obj.get("ok", True)),  # some of your year files don't have ok/version
        "version": obj.get("version"),
    }

    # prefer existing summary/label_counts if present
    if "summary" in obj:
        out["summary"] = obj.get("summary")
    if "label_counts" in obj:
        out["label_counts"] = obj.get("label_counts")

    # If the file is in the "rows shape", still summarize lightly:
    if "rows" in obj and isinstance(obj.get("rows"), list) and obj["rows"]:
        rows = obj["rows"]
        out["summary"] = out.get("summary") or {
            "count": len(rows),
            "latest_time_utc": rows[-1].get("time_utc"),
            "close_last": rows[-1].get("close"),
        }

    return out

def main():
    report = {
        "generated_utc": utc_now_iso(),
        "schema": "btc-data-report-v1",
        "status": {"ok": True, "missing_files": [], "errors": {}},
        "data": {},
    }

    # load each file if present
    loaded = {}
    for key, fname in FILES.items():
        path = os.path.join(IN_DIR, fname)
        if not os.path.exists(path):
            report["status"]["ok"] = False
            report["status"]["missing_files"].append(fname)
            continue
        try:
            loaded[key] = read_json(path)
        except Exception as e:
            report["status"]["ok"] = False
            report["status"]["errors"][fname] = str(e)

    # dashboard (already compact)
    if "dashboard" in loaded:
        report["data"]["dashboard"] = loaded["dashboard"]

    # tier1 (compact it)
    if "tier1" in loaded:
        t1 = compact_insights_local(loaded["tier1"])

    # Explicitly document Tier-1 semantics
    if isinstance(t1, dict):
        t1["note"] = (
            "Tier-1 data is fetched at workflow runtime (spot price, macro, funding). "
            "It is NOT the authoritative hourly candle close used by the Sheets engine."
        )

        # Compute spot vs latest hourly close
        spot = None
        try:
            spot = fnum((t1.get("price") or {}).get("btc_usd"))
        except Exception:
            spot = None

        if spot is not None and latest_close not in (None, 0):
            delta = spot - latest_close
            t1["spot_vs_latest_close_usd"] = round(delta, 2)
            t1["spot_vs_latest_close_pct"] = round((delta / latest_close) * 100.0, 3)
        else:
            t1["spot_vs_latest_close_usd"] = None
            t1["spot_vs_latest_close_pct"] = None

    report["data"]["tier1"] = t1


    # latest: keep ONLY the single candle (already 1 row)
    if "latest" in loaded:
        # Some earlier scripts put rows; keep the first row only if it exists
        latest = loaded["latest"]
        if isinstance(latest, dict) and isinstance(latest.get("rows"), list) and latest["rows"]:
            latest = dict(latest)
            latest["rows"] = [latest["rows"][-1]]
            latest["count"] = 1
        report["data"]["latest"] = latest

    latest_close = None
    if isinstance(latest, dict) and isinstance(latest.get("rows"), list) and latest["rows"]:
        latest_close = fnum(latest["rows"][0].get("close"))


    # last-24h: summarize + last N rows
    if "last-24h" in loaded:
        report["data"]["last-24h"] = summarize_last24h(loaded["last-24h"])

    # periods: summary only
    for k in ("90d", "ytd", "2023", "2024"):
        if k in loaded:
            report["data"][k] = keep_summary_only(loaded[k])

    ensure_dir(OUT_PATH)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=False)

    def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
