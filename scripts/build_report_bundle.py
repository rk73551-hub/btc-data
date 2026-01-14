#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

PUBLIC_DIR = "public"
OUT_PATH = os.path.join(PUBLIC_DIR, "report.json")

FILES = [
    "latest.json",
    "last-24h.json",
    "90d.json",
    "ytd.json",
    "2023.json",
    "2024.json",
    "dashboard.json",
    "insights_local.json",
]

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    os.makedirs(PUBLIC_DIR, exist_ok=True)

    bundle = {
        "generated_utc": utc_now_iso(),
        "schema": "btc-data-report-v1",
        "status": {
            "ok": True,
            "missing_files": [],
            "errors": {}
        },
        "data": {}
    }

    for name in FILES:
        p = os.path.join(PUBLIC_DIR, name)
        if not os.path.exists(p):
            bundle["status"]["ok"] = False
            bundle["status"]["missing_files"].append(name)
            continue
        try:
            bundle["data"][name.replace(".json", "")] = read_json(p)
        except Exception as e:
            bundle["status"]["ok"] = False
            bundle["status"]["errors"][name] = str(e)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2, sort_keys=False)

    print(f"Wrote {OUT_PATH}")
    if not bundle["status"]["ok"]:
        print("Bundle built with warnings:", bundle["status"])

if __name__ == "__main__":
    main()
