#!/usr/bin/env python3
"""
Fetch data from openFDA APIs and save as JSON for EpiDataKit dashboard.

Endpoints:
  - Tobacco adverse events (by year, product, health problem)
  - Drug adverse events for GLP-1 drugs (ozempic, mounjaro, wegovy, jardiance)
  - Drug recall trends
  - Device 510(k) clearance trends
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_TIMEOUT = 30  # seconds per request
RETRY_ATTEMPTS = 2
RETRY_DELAY = 3  # seconds between retries

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "openfda-latest.json"

GLP1_DRUGS = ["ozempic", "mounjaro", "wegovy", "jardiance"]


def fetch(url: str) -> list | None:
    """Fetch a single openFDA endpoint with retries. Returns the 'results' array or None on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            print(f"  GET {url}")
            resp = requests.get(url, timeout=BASE_TIMEOUT)
            if resp.status_code == 404:
                print(f"    -> 404 (no data)")
                return []
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            print(f"    -> {len(results)} records")
            return results
        except requests.RequestException as exc:
            print(f"    -> attempt {attempt} failed: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
    print(f"    -> GIVING UP after {RETRY_ATTEMPTS} attempts")
    return None


def fetch_tobacco_ae() -> dict:
    """Tobacco adverse-event data: by year, by product, by health problem."""
    print("\n[Tobacco AE]")
    by_year = fetch(
        "https://api.fda.gov/tobacco/problem.json?count=date_submitted&limit=1000"
    )
    by_product = fetch(
        "https://api.fda.gov/tobacco/problem.json?count=tobacco_products.exact&limit=20"
    )
    by_problem = fetch(
        "https://api.fda.gov/tobacco/problem.json?count=reported_health_problems.exact&limit=20"
    )
    return {
        "by_year": by_year or [],
        "by_product": by_product or [],
        "by_problem": by_problem or [],
    }


def fetch_drug_ae() -> dict:
    """Drug AE data for each GLP-1 drug: trend + top reactions."""
    print("\n[Drug AE — GLP-1 focus]")
    results = {}
    for drug in GLP1_DRUGS:
        print(f"  -- {drug} --")
        trend = fetch(
            f'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.brand_name:"{drug}"&count=receivedate'
        )
        top_reactions = fetch(
            f'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.brand_name:"{drug}"'
            f"&count=patient.reaction.reactionmeddrapt.exact&limit=10"
        )
        results[drug] = {
            "trend": trend or [],
            "top_reactions": top_reactions or [],
        }
        # Be polite to the API
        time.sleep(0.5)
    return results


def fetch_drug_recalls() -> dict:
    """Drug enforcement / recall trend data."""
    print("\n[Drug Recalls]")
    by_year = fetch(
        "https://api.fda.gov/drug/enforcement.json?count=report_date&limit=100"
    )
    return {"by_year": by_year or []}


def fetch_device_510k() -> dict:
    """Device 510(k) clearance trend data."""
    print("\n[Device 510k]")
    by_year = fetch(
        "https://api.fda.gov/device/510k.json?count=decision_date&limit=100"
    )
    return {"by_year": by_year or []}


def main() -> None:
    print(f"openFDA data fetch started at {datetime.now(timezone.utc).isoformat()}")

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "tobacco_ae": fetch_tobacco_ae(),
        "drug_ae": fetch_drug_ae(),
        "drug_recalls": fetch_drug_recalls(),
        "device_510k": fetch_device_510k(),
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUTPUT_FILE}")

    # Quick validation
    size = OUTPUT_FILE.stat().st_size
    print(f"File size: {size:,} bytes")
    if size < 500:
        print("WARNING: output file is suspiciously small — check API responses above.")
        sys.exit(1)

    # Verify JSON is valid
    with open(OUTPUT_FILE) as f:
        data = json.load(f)
    sections = ["tobacco_ae", "drug_ae", "drug_recalls", "device_510k"]
    for s in sections:
        if s not in data:
            print(f"ERROR: missing section '{s}'")
            sys.exit(1)
    print("Validation passed.")


if __name__ == "__main__":
    main()
