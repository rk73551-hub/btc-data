#!/usr/bin/env python3
import json
import os
import time
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

def http_get_text(url: str, headers=None, timeout=20):
    r = requests.get(url, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r.text

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
    Returns last close and prior close when available.
    """
    # 5d gives enough points even if markets closed recently
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
    # find last non-null close
    last = None
    prev = None
    for v in reversed(closes):
        if v is not None:
            last = float(v)
            break
    if last is None:
        raise RuntimeError("yahoo_no_close")

    # find prev non-null close before last
    found_last = False
    for v in reversed(closes):
        if v is None:
            continue
        if not found_last:
            # this is last
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

def fetch_binance_btc_funding():
    """
    Binance USDT-m perpetual funding (public, no key)
    Returns lastFundingRate and mark/index.
    """
    # premiumIndex includes lastFundingRate
    url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
    j = http_get_json(url, headers={"User-Agent": "btc-data-tier1/1.0"})
    # lastFundingRate is a string like "0.00010000" (per funding interval)
    rate = float(j.get("lastFundingRate")) if j.get("lastFundingRate") is not None else None
    mark = float(j.get("markPrice")) if j.get("markPrice") is not None else None
    index = float(j.get("indexPrice")) if j.get("indexPrice") is not None else None
    return {
        "source": "binance_futures",
        "symbol": "BTCUSDT",
        "lastFundingRate": rate,
        "markPrice": mark,
        "indexPrice": index,
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
    # DXY
    try:
        macro["dxy"] = fetch_yahoo_chart_last("%5EDXY")  # ^DXY
    except Exception as e:
        macro["dxy"] = {"error": str(e)}
    # US10Y (Yahoo: ^TNX is yield*10)
    try:
        tnx = fetch_yahoo_chart_last("%5ETNX")  # ^TNX
        # Convert to percent yield
        if isinstance(tnx.get("last"), (int, float)):
            tnx["last_yield_pct"] = tnx["last"] / 10.0
        if isinstance(tnx.get("prev"), (int, float)):
            tnx["prev_yield_pct"] = tnx["prev"] / 10.0
        macro["us10y"] = tnx
    except Exception as e:
        macro["us10y"] = {"error": str(e)}
    # Futures: ES and NQ
    try:
        macro["es_futures"] = fetch_yahoo_chart_last("ES=F")
    except Exception as e:
        macro["es_futures"] = {"error": str(e)}
    try:
        macro["nq_futures"] = fetch_yahoo_chart_last("NQ=F")
    except Exception as e:
        macro["nq_futures"] = {"error": str(e)}

    out["macro"] = macro

    # Funding
    try:
        out["funding"] = fetch_binance_btc_funding()
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
