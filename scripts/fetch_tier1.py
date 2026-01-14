import json, math, re
from datetime import datetime, timezone
import requests
import pandas as pd

FARSIDE_BTC_URL = "https://farside.co.uk/btc/"  # public HTML table
BINANCE_FAPI = "https://fapi.binance.com"

OUT_DIR = "public"  # adjust if your pipeline writes elsewhere

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def safe_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)) and math.isfinite(x):
        return float(x)
    s = str(x).strip()
    if s in ("", "-", "—", "–", "NA", "N/A", "null", "None"):
        return None
    # remove commas and currency markers
    s = s.replace(",", "").replace("$", "")
    # handle parentheses negatives e.g. (123.4)
    if re.match(r"^\(.*\)$", s):
        s = "-" + s[1:-1]
    try:
        v = float(s)
        return v if math.isfinite(v) else None
    except:
        return None

def fetch_etf_flows():
    # Farside page includes a table with daily flows by ETF + total
    tables = pd.read_html(FARSIDE_BTC_URL)
    if not tables:
        raise RuntimeError("no_tables_found_on_farside")

    df = tables[0].copy()
    # Try to find a date column
    date_col = None
    for c in df.columns:
        if str(c).strip().lower() in ("date", "day"):
            date_col = c
            break
    if date_col is None:
        # fallback: first column
        date_col = df.columns[0]

    df = df.rename(columns={date_col: "date"})
    df["date"] = df["date"].astype(str).str.strip()

    # Normalize numeric columns
    numeric_cols = [c for c in df.columns if c != "date"]
    for c in numeric_cols:
        df[c] = df[c].apply(safe_float)

    # Prefer a total column if present
    total_col = None
    for c in df.columns:
        if str(c).strip().lower() in ("total", "net", "net flow", "net flows"):
            total_col = c
            break

    # Keep last ~90 rows (enough for recent context)
    df = df.tail(90)

    rows = []
    for _, r in df.iterrows():
        obj = {"date": r["date"]}
        for c in numeric_cols:
            obj[str(c)] = None if pd.isna(r[c]) else r[c]
        rows.append(obj)

    latest = rows[-1] if rows else None
    latest_total = latest.get(str(total_col)) if (latest and total_col) else None

    return {
        "ok": True,
        "source": "farside",
        "source_url": FARSIDE_BTC_URL,
        "updated_utc": utc_now_iso(),
        "latest_date": latest["date"] if latest else None,
        "latest_total": latest_total,
        "columns": [str(c) for c in df.columns],
        "rows": rows
    }

def fetch_binance_funding(symbol="BTCUSDT", limit=60):
    # Public endpoint, no key required
    url = f"{BINANCE_FAPI}/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=20)
    r.raise_for_status()
    data = r.json()

    rows = []
    for it in data:
        fr = safe_float(it.get("fundingRate"))
        ft = it.get("fundingTime")
        ts = None
        if ft is not None:
            try:
                ts = datetime.fromtimestamp(int(ft)/1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except:
                ts = None
        rows.append({"time_utc": ts, "funding_rate": fr})

    latest = rows[-1] if rows else None
    return {"symbol": symbol, "rows": rows, "latest": latest}

def fetch_binance_open_interest(symbol="BTCUSDT", period="1h", limit=48):
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    r = requests.get(url, params={"symbol": symbol, "period": period, "limit": limit}, timeout=20)
    r.raise_for_status()
    data = r.json()

    rows = []
    for it in data:
        oi = safe_float(it.get("sumOpenInterest"))
        t = it.get("timestamp")
        ts = None
        if t is not None:
            try:
                ts = datetime.fromtimestamp(int(t)/1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except:
                ts = None
        rows.append({"time_utc": ts, "open_interest": oi})

    latest = rows[-1] if rows else None
    chg = None
    if len(rows) >= 2 and rows[-1]["open_interest"] is not None and rows[0]["open_interest"] is not None:
        chg = rows[-1]["open_interest"] - rows[0]["open_interest"]

    return {"symbol": symbol, "period": period, "rows": rows, "latest": latest, "change_over_window": chg}

def main():
    import os
    os.makedirs(OUT_DIR, exist_ok=True)

    etf = fetch_etf_flows()

    funding = fetch_binance_funding("BTCUSDT", 60)
    oi = fetch_binance_open_interest("BTCUSDT", "1h", 48)

    deriv = {
        "ok": True,
        "source": "binance_futures",
        "base_url": BINANCE_FAPI,
        "updated_utc": utc_now_iso(),
        "funding": funding,
        "open_interest": oi,
    }

    with open(f"{OUT_DIR}/tier1_etf_flows.json", "w", encoding="utf-8") as f:
        json.dump(etf, f, ensure_ascii=False)

    with open(f"{OUT_DIR}/tier1_derivatives.json", "w", encoding="utf-8") as f:
        json.dump(deriv, f, ensure_ascii=False)

    with open(f"{OUT_DIR}/tier1_timestamp.txt", "w", encoding="utf-8") as f:
        f.write(utc_now_iso() + "\n")

if __name__ == "__main__":
    main()
