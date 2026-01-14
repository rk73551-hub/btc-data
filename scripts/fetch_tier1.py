#!/usr/bin/env python3
import os
import json
import time
from datetime import datetime, timezone
from io import StringIO

import requests
import pandas as pd

OUT_PATH = "public/insights_local.json"

# Farside page that contains the US spot BTC ETF flow table
FARSIDE_BTC_URL = "https://farside.co.uk/btc/"

# Coinbase spot price endpoint (simple + reliable)
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

# Use a realistic browser UA (helps avoid basic bot blocks)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def safe_get(url: str, headers=None, timeout=20, retries=2, backoff=2.0):
    headers = headers or {}
    last_exc = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            return r
        except Exception as e:
            last_exc = e
            if i < retries:
                time.sleep(backoff * (i + 1))
    raise last_exc

def fetch_coinbase_spot():
    try:
        r = safe_get(COINBASE_SPOT_URL, headers={"Accept": "application/json", **DEFAULT_HEADERS})
        r.raise_for_status()
        j = r.json()
        amt = float(j["data"]["amount"])
        return {
            "source": "coinbase_spot",
            "btc_usd": amt,
            "ts_utc": utc_now_iso(),
            "raw": j,
        }
    except Exception as e:
        return {
            "error": str(e),
        }

def _parse_farside_tables_from_html(html: str):
    # Parse all tables from HTML string
    tables = pd.read_html(StringIO(html))
    return tables

def fetch_etf_flows():
    """
    Attempts to fetch Farside BTC ETF flow table.
    Writes a compact, resilient output:
      - status: ok/unavailable
      - if ok: includes date + table preview
    """
    try:
        r = safe_get(FARSIDE_BTC_URL, headers=DEFAULT_HEADERS, timeout=25, retries=2)
        if r.status_code != 200:
            return {"status": "unavailable", "reason": f"HTTP {r.status_code}"}

        html = r.text
        tables = _parse_farside_tables_from_html(html)
        if not tables:
            return {"status": "unavailable", "reason": "no_tables_found"}

        # Heuristic: pick the largest table (usually the flows table)
        flows = max(tables, key=lambda df: df.shape[0] * df.shape[1])

        # Clean columns a bit
        flows.columns = [str(c).strip() for c in flows.columns]
        flows = flows.copy()

        # Keep it small so your JSON doesn't explode
        preview_rows = min(20, len(flows))
        preview = flows.head(preview_rows).to_dict(orient="records")

        return {
            "status": "ok",
            "source": "farside",
            "fetched_utc": utc_now_iso(),
            "table_shape": [int(flows.shape[0]), int(flows.shape[1])],
            "columns": flows.columns.tolist(),
            "preview": preview,
        }

    except Exception as e:
        # Most common here: still blocked, or table parsing changes
        return {"status": "unavailable", "reason": str(e)}

def main():
    out = {
        "generated_utc": utc_now_iso(),
        "tier": "tier1",
        "price": fetch_coinbase_spot(),
        "etf_flows": fetch_etf_flows(),
    }

    write_json(OUT_PATH, out)
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
