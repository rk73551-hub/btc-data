#!/usr/bin/env python3
import json
import os
import re
from datetime import datetime, timezone
from io import StringIO

import requests
import pandas as pd

OUT_PATH = "public/insights_local.json"

FARSIDE_BTC_URL = "https://farside.co.uk/btc/"
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s in ("", "-", "—", "–", "N/A", "na", "null", "None"):
            return None
        # remove commas and parentheses negatives like (123.4)
        s = s.replace(",", "")
        m = re.match(r"^\((.+)\)$", s)
        if m:
            return -float(m.group(1))
        return float(s)
    except Exception:
        return None


def fetch_btc_spot_coinbase():
    """
    Returns:
      { "source": "...", "btc_usd": 12345.67, "ts_utc": "...", "raw": {...} }
    """
    try:
        r = requests.get(COINBASE_SPOT_URL, headers={"User-Agent": UA_HEADERS["User-Agent"]}, timeout=20)
        r.raise_for_status()
        j = r.json()
        amt = safe_float(j.get("data", {}).get("amount"))
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


def fetch_farside_btc_etf_flows():
    """
    Scrapes https://farside.co.uk/btc/ and returns the latest available row.

    Output shape:
      {
        "source": "farside.co.uk/btc",
        "latest_date": "2026-01-13",
        "total_usd_m": 753.8,
        "funds_usd_m": { "IBIT": 126.3, "FBTC": 351.4, ... }
      }
    """
    try:
        r = requests.get(FARSIDE_BTC_URL, headers=UA_HEADERS, timeout=30)
        if r.status_code != 200:
            return {"status": "unavailable", "reason": f"HTTP {r.status_code}"}

        # pandas.read_html expects file-like or string; StringIO avoids it trying to refetch the URL itself.
        tables = pd.read_html(StringIO(r.text))
        if not tables:
            return {"status": "unavailable", "reason": "no_tables_found"}

        # First table on the page is the flows table.
        df = tables[0].copy()

        # The first column is the date; its name varies (sometimes unnamed).
        date_col = df.columns[0]
        df.rename(columns={date_col: "Date"}, inplace=True)

        # Normalize column names (strip)
        df.columns = [str(c).strip() for c in df.columns]

        # Farside uses "Total" in the last column
        if "Total" not in df.columns:
            return {"status": "unavailable", "reason": "missing_total_column"}

        # Filter to rows that actually have numeric totals
        df["_total_num"] = df["Total"].apply(safe_float)
        df2 = df[df["_total_num"].notna()].copy()
        if df2.empty:
            return {"status": "unavailable", "reason": "no_numeric_rows"}

        latest = df2.iloc[-1]

        # Parse date into YYYY-MM-DD if possible
        date_str = str(latest["Date"]).strip()
        # Farside formats like "13 Jan 2026" — keep it robust.
        latest_date_iso = None
        try:
            latest_date_iso = datetime.strptime(date_str, "%d %b %Y").date().isoformat()
        except Exception:
            latest_date_iso = date_str  # fallback

        # Build per-fund dict excluding Date/Total helpers
        funds = {}
        for c in df.columns:
            if c in ("Date", "Total", "_total_num"):
                continue
            v = safe_float(latest[c])
            if v is not None:
                funds[c] = v

        return {
            "source": "farside.co.uk/btc",
            "latest_date": latest_date_iso,
            "total_usd_m": safe_float(latest["Total"]),
            "funds_usd_m": funds,
        }

    except Exception as e:
        # If it’s still blocked, don’t kill the workflow
        return {"status": "unavailable", "reason": str(e)}


def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    out = {
        "generated_utc": utc_now_iso(),
        "tier": "tier1",
        "price": fetch_btc_spot_coinbase(),
        "etf_flows": fetch_farside_btc_etf_flows(),
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=False)

    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
