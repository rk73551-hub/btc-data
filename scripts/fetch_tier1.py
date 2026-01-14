import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone

# --------------------
# Config
# --------------------
OUT_PATH = "public/insights_local.json"
FARSIDE_BTC_URL = "https://farside.co.uk/?p=997"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# --------------------
# Helpers
# --------------------
def utc_now():
    return datetime.now(timezone.utc).isoformat()

def safe_float(x):
    try:
        return float(x)
    except:
        return None

# --------------------
# Price / market data
# --------------------
def fetch_price_snapshot():
    url = "https://api.coindesk.com/v1/bpi/currentprice/BTC.json"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    price = safe_float(data["bpi"]["USD"]["rate"].replace(",", ""))
    return {
        "price_usd": price,
        "source": "coindesk"
    }

# --------------------
# ETF flows (GUARDED)
# --------------------
def fetch_etf_flows():
    try:
        tables = pd.read_html(FARSIDE_BTC_URL)
        df = tables[0]

        df.columns = [c.lower().strip() for c in df.columns]

        latest = df.iloc[0].to_dict()

        return {
            "status": "ok",
            "latest": latest
        }

    except Exception as e:
        return {
            "status": "unavailable",
            "reason": str(e)
        }

# --------------------
# Main
# --------------------
def main():
    out = {
        "generated_utc": utc_now(),
        "tier": "tier1",
    }

    # Price snapshot
    try:
        out["price"] = fetch_price_snapshot()
    except Exception as e:
        out["price"] = {"error": str(e)}

    # ETF flows (non-fatal)
    out["etf_flows"] = fetch_etf_flows()

    # Write output
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)

    print("Tier 1 data written:", OUT_PATH)

# --------------------
if __name__ == "__main__":
    main()
