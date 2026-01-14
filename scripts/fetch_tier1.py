#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

import requests

OUT_PATH = "public/insights_local.json"

# ---- helpers ----
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_parent_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def http_get_json(url: str, headers=None, timeout=20):
    r = requests.get(url, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ---- sources ----

def fetch_coinbase_spot_btc_usd():
    """
    Coinbase spot price (public, no key)
    """
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    j = http_get_json(url, headers={"User-Agent": "btc-data-tier1/1.0"})
    amount = float(j["data"]["amount"])
    return {
        "source": "coinbase_spot",
        "btc_usd": amount,
        "ts_utc": utc_now_iso(),
        "raw": j,
    }

def fetch_yahoo_chart_last(symbol: str):
    """
    Yahoo Finance chart endpoint (public, no key)
    Fallback method: scan chart closes for last/prev non-null.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1h"
    j = http_get_json(url, headers={"User-Agent": "btc-data-tier1/1.0"})
    res = j.get("chart", {}).get("result", [])
    if not res:
        raise RuntimeError("yahoo_no_result")

    r0 = res[0]
    meta = r0.get("meta", {})
    ind = r0.get("indicators", {}).get("quote", [])
    if not ind:
        raise RuntimeError("yahoo_no_indicators")

    closes = ind[0].get("close", []) or []
    last = None
    prev = None

    # last non-null
    for v in reversed(closes):
        if v is not None:
            last = float(v)
            break
    if last is None:
        raise RuntimeError("yahoo_no_close")

    # prev non-null before last
    found_last = False
    for v in reversed(closes):
        if v is None:
            continue
        if not found_last:
            found_last = True
            continue
        prev = float(v)
        break

    return {
        "symbol": symbol,
        "last": last,
        "prev": prev,
        "currency": meta.get("currency"),
        "exchangeName": meta.get("exchangeName"),
        "instrumentType": meta.get("instrumentType"),
        "ts_utc": utc_now_iso(),
    }

def fetch_yahoo_meta_price(symbol: str):
    """
    Yahoo Finance chart endpoint (public, no key)
    Preferred for intraday: meta.regularMarketPrice + meta.previousClose
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1h"
    j = http_get_json(url, headers={"User-Agent": "btc-data-tier1/1.0"})
    res = j.get("chart", {}).get("result", [])
    if not res:
        raise RuntimeError("yahoo_no_result")

    r0 = res[0]
    meta = r0.get("meta", {}) or {}

    last = meta.get("regularMarketPrice")
    prev = meta.get("previousClose")

    # If either missing, fall back to scanning closes (keeps system resilient)
    if last is None:
        try:
            fallback = fetch_yahoo_chart_last(symbol)
            last = fallback.get("last")
            if prev is None:
                prev = fallback.get("prev")
        except Exception:
            pass

    if last is None:
        raise RuntimeError("yahoo_no_last")

    return {
        "symbol": symbol,
        "last": float(last),
        "prev": float(prev) if prev is not None else None,
        "currency": meta.get("currency"),
        "exchangeName": meta.get("exchangeName"),
        "instrumentType": meta.get("instrumentType"),
        "ts_utc": utc_now_iso(),
    }

def fetch_okx_btc_funding():
    """
    OKX perpetual funding (public, no key).
    Uses SWAP instrument BTC-USDT-SWAP.
    """
    url = "https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP"
    j = http_get_json(url, headers={"User-Agent": "btc-data-tier1/1.0"})
    data = (j.get("data") or [])
    if not data:
        raise RuntimeError("okx_no_data")
    d0 = data[0]

    # OKX returns strings
    fr = d0.get("fundingRate")
    ts = d0.get("ts")
    next_fr = d0.get("nextFundingRate")
    next_ts = d0.get("nextFundingTime")

    def fnum(x):
        try:
            return float(x)
        except Exception:
            return None

    return {
        "source": "okx_swap",
        "symbol": "BTC-USDT-SWAP",
        "fundingRate": fnum(fr),          # e.g., 0.0001
        "nextFundingRate": fnum(next_fr),
        "ts_exchange_ms": int(ts) if ts is not None else None,
        "nextFundingTime_ms": int(next_ts) if next_ts is not None else None,
        "ts_utc": utc_now_iso(),
        "raw": j,
    }

def fetch_etf_flows_stub():
    """
    Keep your current behavior: ETF flows blocked (403).
    Don’t break the pipeline — just report unavailable.
    """
    return {
        "status": "unavailable",
        "reason": "HTTP 403",
    }

# ---- main ----

def main():
    out = {
        "generated_utc": utc_now_iso(),
        "tier": "tier1",
    }

    # BTC spot
    try:
        out["price"] = fetch_coinbase_spot_btc_usd()
    except Exception as e:
        out["price"] = {"error": str(e)}

    # Macro (Yahoo)
    macro = {}

    # DXY: use DX-Y.NYB (more reliable) + meta-based pricing
    try:
        macro["dxy"] = fetch_yahoo_meta_price("DX-Y.NYB")
    except Exception as e:
        macro["dxy"] = {"error": str(e)}

    # US10Y: ^TNX already comes back as ~4.xx (= percent). Do NOT divide by 10.
    try:
        tnx = fetch_yahoo_meta_price("%5ETNX")  # ^TNX
        if isinstance(tnx.get("last"), (int, float)):
            tnx["last_yield_pct"] = tnx["last"]
        if isinstance(tnx.get("prev"), (int, float)):
            tnx["prev_yield_pct"] = tnx["prev"]
        macro["us10y"] = tnx
    except Exception as e:
        macro["us10y"] = {"error": str(e)}

    # Futures: ES and NQ
    try:
        macro["es_futures"] = fetch_yahoo_meta_price("ES=F")
    except Exception as e:
        macro["es_futures"] = {"error": str(e)}
    try:
        macro["nq_futures"] = fetch_yahoo_meta_price("NQ=F")
    except Exception as e:
        macro["nq_futures"] = {"error": str(e)}

    out["macro"] = macro

    # Funding: OKX instead of Binance (Binance often 451/geo-blocked in CI)
    try:
        out["funding"] = fetch_okx_btc_funding()
    except Exception as e:
        out["funding"] = {"error": str(e)}

    # ETF flows (still blocked)
    out["etf_flows"] = fetch_etf_flows_stub()

    ensure_parent_dir(OUT_PATH)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=False)

    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
