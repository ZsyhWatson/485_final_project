"""
01_data_acquisition.py
======================
OmniStyle  ·  Acquisition layer (v2 — REES46 / Kaggle schema)

Primary dataset: **eCommerce behavior data from a multi-category store**
(Kaggle, REES46 Marketing Platform — `mkechinov/ecommerce-behavior-data-from-multi-category-store`).

  Schema (9 columns):
     event_time     UTC timestamp of the event
     event_type     view | cart | remove_from_cart | purchase
     product_id     int
     category_id    int
     category_code  dot-delimited hierarchy, e.g. "electronics.smartphone"  (≈30% null in real data)
     brand          string (≈15% null in real data)
     price          USD
     user_id        int
     user_session   UUID — same session can span hours

This source has **no ZIP / city / demographic fields**, which is exactly what we need:
the recommender bias the case study describes is an *exposure* bias (which products got
shown to which users), so we *synthesise* a sticky `user_location` table and inject
controlled bias into the event stream.

Three secondary sources (real, public APIs) enrich the analysis:
  · CFPB Consumer Complaint Database
  · U.S. Census Bureau ACS 5-Year (demographics by ZCTA)
  · HUD ZIP↔County crosswalk
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
KAGGLE_DIR = RAW / "kaggle"
RAW.mkdir(parents=True, exist_ok=True)
KAGGLE_DIR.mkdir(parents=True, exist_ok=True)

EVENT_COLS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
]
EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
EVENT_TYPE_PROBS = [0.945, 0.030, 0.010, 0.015]   # close to real funnel ratios

CATEGORY_TREE = {
    "electronics.smartphone":          "electronics",
    "electronics.audio.headphone":     "electronics",
    "electronics.video.tv":            "electronics",
    "electronics.clocks":              "electronics",
    "computers.notebook":              "computers",
    "computers.desktop":               "computers",
    "computers.peripherals.mouse":     "computers",
    "appliances.kitchen.refrigerators": "appliances",
    "appliances.kitchen.washer":       "appliances",
    "appliances.environment.vacuum":   "appliances",
    "furniture.living_room.sofa":      "furniture",
    "furniture.bedroom.bed":           "furniture",
    "apparel.shoes":                   "apparel",
    "apparel.tshirt":                  "apparel",
    "accessories.bag":                 "accessories",
    "kids.toys":                       "kids",
    "construction.tools.light":        "construction",
    "auto.accessories":                "auto",
}
BRANDS = [
    "samsung", "apple", "xiaomi", "huawei", "lg", "sony", "bosch",
    "philips", "hp", "lenovo", "asus", "polaris", "indesit", "redmond",
    "nike", "adidas", "puma", "zara", "hm",
]


# =========================================================================
# Real Kaggle download (requires `pip install kaggle` + ~/.kaggle/kaggle.json)
# =========================================================================
def download_kaggle() -> bool:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi  # type: ignore
        api = KaggleApi(); api.authenticate()
        api.dataset_download_files(
            "mkechinov/ecommerce-behavior-data-from-multi-category-store",
            path=str(KAGGLE_DIR), unzip=True,
        )
        print(f"[KAGGLE] dataset extracted into {KAGGLE_DIR}")
        return True
    except Exception as e:
        print(f"[KAGGLE] download skipped — {e}")
        return False


def load_kaggle_sample(n_rows: int = 500_000) -> pd.DataFrame | None:
    csvs = sorted(KAGGLE_DIR.glob("*.csv"))
    if not csvs:
        return None
    print(f"[KAGGLE] reading {csvs[0].name} (sampling {n_rows:,} rows)")
    df = pd.read_csv(csvs[0], nrows=n_rows * 4)
    df = df.sample(min(n_rows, len(df)), random_state=42).reset_index(drop=True)
    return df


# =========================================================================
# Synthetic generator that matches the Kaggle schema exactly
# =========================================================================
def generate_synthetic_events(n_events: int = 500_000, n_users: int = 30_000,
                               n_products: int = 4_000, seed: int = 42):
    rng = np.random.default_rng(seed)

    cat_codes = list(CATEGORY_TREE.keys())
    cat_ids   = rng.integers(2_000_000_000, 2_999_999_999, len(cat_codes))
    code_to_id = dict(zip(cat_codes, cat_ids))

    prod_cat   = rng.choice(cat_codes, n_products)
    prod_brand = rng.choice(BRANDS, n_products)
    cat_price_mu = {c: rng.uniform(2.5, 6.0) for c in cat_codes}
    prod_price = np.array([
        round(np.exp(rng.normal(cat_price_mu[c], 0.6)), 2) for c in prod_cat
    ])
    products = pd.DataFrame({
        "product_id":   np.arange(1_000_000, 1_000_000 + n_products),
        "category_code": prod_cat,
        "category_id":  [code_to_id[c] for c in prod_cat],
        "brand":        prod_brand,
        "base_price":   prod_price,
    })

    user_ids = rng.integers(500_000_000, 600_000_000, n_users)
    zips = pd.DataFrame({
        "zip": ["10027", "10021", "60619", "60611", "90011", "90210",
                "75216", "75205", "30310", "30327", "85001", "85254"],
        "city": ["NYC", "NYC", "Chicago", "Chicago", "LA", "LA",
                 "Dallas", "Dallas", "Atlanta", "Atlanta", "Phoenix", "Phoenix"],
        "median_income": [42000, 145000, 38000, 165000, 36000, 210000,
                          31000, 130000, 33000, 175000, 41000, 95000],
        "pct_minority":  [0.82, 0.18, 0.92, 0.21, 0.95, 0.15,
                          0.88, 0.22, 0.91, 0.19, 0.65, 0.30],
    })
    user_zip_idx = rng.integers(0, len(zips), n_users)
    user_loc = pd.DataFrame({
        "user_id":       user_ids,
        "zip":           zips["zip"].values[user_zip_idx],
        "pct_minority":  zips["pct_minority"].values[user_zip_idx],
        "median_income": zips["median_income"].values[user_zip_idx],
    })

    # ---- biased exposure: high-minority-ZIP users see priced-up products ----
    user_idx = rng.integers(0, n_users, n_events)
    pct_min  = user_loc["pct_minority"].values[user_idx]

    price_order = np.argsort(prod_price)
    rank_norm   = np.linspace(0, 1, n_products)
    rank_lookup = np.empty_like(rank_norm)
    rank_lookup[price_order] = rank_norm

    chosen_prod_idx = np.empty(n_events, dtype=int)
    tiers = np.digitize(pct_min, [0.30, 0.60])         # 0=low, 1=mid, 2=high minority
    for t in range(3):
        # tier 0 ≈ uniform; tier 2 strongly tilts toward priciest items
        w = np.exp(1.6 * rank_lookup * (0.15 + 0.35 * t))
        w = w / w.sum()
        mask = tiers == t
        if mask.any():
            chosen_prod_idx[mask] = rng.choice(n_products, mask.sum(), p=w)

    chosen_prod = products.iloc[chosen_prod_idx].reset_index(drop=True)

    base = datetime(2023, 11, 1)
    secs = rng.integers(0, 30 * 86400, n_events)
    event_time = pd.to_datetime([base + timedelta(seconds=int(s)) for s in secs])

    n_sessions = max(1, n_events // 8)
    # Use 64-bit chunks; UUIDs are 128 bits, so concatenate two 64-bit halves
    hi = rng.integers(0, 2**63, n_sessions, dtype=np.int64)
    lo = rng.integers(0, 2**63, n_sessions, dtype=np.int64)
    session_lookup = np.array([
        str(uuid.UUID(int=(int(h) << 64) | int(l))) for h, l in zip(hi, lo)
    ])
    session_idx = rng.integers(0, n_sessions, n_events)
    user_session = session_lookup[session_idx]

    # Funnel: views are unbiased; cart and purchase are SLIGHTLY suppressed for tier-2 users
    # (because higher prices reduce conversion — a real second-order effect of exposure bias).
    base_event_type = rng.choice(EVENT_TYPES, n_events, p=EVENT_TYPE_PROBS)
    # For tier-2 users, downgrade ~25% of would-be purchases to views, ~20% of carts to views
    suppress_p = (tiers == 2) & (base_event_type == "purchase") & (rng.random(n_events) < 0.25)
    suppress_c = (tiers == 2) & (base_event_type == "cart")     & (rng.random(n_events) < 0.20)
    base_event_type[suppress_p] = "view"
    base_event_type[suppress_c] = "view"

    df = pd.DataFrame({
        "event_time":    event_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "event_type":    base_event_type,
        "product_id":    chosen_prod["product_id"].values,
        "category_id":   chosen_prod["category_id"].values,
        "category_code": chosen_prod["category_code"].values,
        "brand":         chosen_prod["brand"].values,
        "price":         chosen_prod["base_price"].values,
        "user_id":       user_loc["user_id"].values[user_idx],
        "user_session":  user_session,
    })

    # ---- inject realistic data-quality defects ----
    df.loc[rng.choice(df.index, int(0.30 * len(df)), replace=False), "category_code"] = np.nan
    df.loc[rng.choice(df.index, int(0.15 * len(df)), replace=False), "brand"] = np.nan
    df.loc[rng.choice(df.index, int(0.002 * len(df)), replace=False), "price"] = 0.0
    df.loc[rng.choice(df.index, int(0.0005 * len(df)), replace=False), "price"] = -1.0
    df.loc[rng.choice(df.index, int(0.001 * len(df)), replace=False), "user_session"] = ""
    dup = df.sample(int(0.005 * len(df)), random_state=7).copy()
    df = pd.concat([df, dup], ignore_index=True)

    return df, user_loc, products, zips


# =========================================================================
# External enrichment (CFPB / Census / HUD)
# =========================================================================
CFPB_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
ACS_URL = "https://api.census.gov/data/2022/acs/acs5"
HUD_URL = "https://www.huduser.gov/hudapi/public/usps"


def fetch_cfpb(size: int = 2000):
    try:
        r = requests.get(CFPB_URL, params={"size": size, "format": "json", "no_aggs": "true"}, timeout=30)
        r.raise_for_status()
        rows = [h["_source"] for h in r.json().get("hits", {}).get("hits", [])]
        pd.DataFrame(rows).to_csv(RAW / "cfpb_complaints.csv", index=False)
        print(f"[CFPB] {len(rows):,} complaints saved")
    except Exception as e:
        print(f"[CFPB] skipped — {e}")


def fetch_census():
    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        print("[CENSUS] skipped — set CENSUS_API_KEY"); return
    try:
        params = {
            "get": "NAME,B01003_001E,B19013_001E,B02001_002E,B02001_003E,B03003_003E",
            "for": "zip code tabulation area:*", "key": key,
        }
        r = requests.get(ACS_URL, params=params, timeout=60); r.raise_for_status()
        raw = r.json()
        df = pd.DataFrame(raw[1:], columns=raw[0])
        df.to_csv(RAW / "census_acs_zcta.csv", index=False)
        print(f"[CENSUS] {len(df):,} ZCTAs saved")
    except Exception as e:
        print(f"[CENSUS] skipped — {e}")


def fetch_hud():
    tok = os.environ.get("HUD_API_TOKEN")
    if not tok:
        print("[HUD] skipped — set HUD_API_TOKEN"); return
    try:
        r = requests.get(HUD_URL,
                         params={"type": 2, "query": "All", "quarter": "1", "year": "2025"},
                         headers={"Authorization": f"Bearer {tok}"}, timeout=60)
        r.raise_for_status()
        df = pd.DataFrame(r.json()["data"]["results"])
        df.to_csv(RAW / "hud_zip_county.csv", index=False)
        print(f"[HUD] {len(df):,} rows saved")
    except Exception as e:
        print(f"[HUD] skipped — {e}")


# =========================================================================
# Main
# =========================================================================
if __name__ == "__main__":
    print(">>> OmniStyle data acquisition (Kaggle-schema) starting…\n")

    df = None
    if download_kaggle():
        df = load_kaggle_sample()

    if df is None:
        print("[SYNTH] generating realistic synthetic events with same schema…")
        df, user_loc, products, zips = generate_synthetic_events()
        user_loc.to_csv(RAW / "synthetic_user_location.csv", index=False)
        products.to_csv(RAW / "synthetic_product_catalogue.csv", index=False)
        zips.to_csv(RAW / "synthetic_zip_reference.csv", index=False)
    else:
        rng = np.random.default_rng(42)
        zips = pd.DataFrame({
            "zip": ["10027", "10021", "60619", "60611", "90011", "90210"],
            "pct_minority":  [0.82, 0.18, 0.92, 0.21, 0.95, 0.15],
        })
        uids = df["user_id"].dropna().unique()
        idx = rng.integers(0, len(zips), len(uids))
        user_loc = pd.DataFrame({
            "user_id":      uids,
            "zip":          zips["zip"].values[idx],
            "pct_minority": zips["pct_minority"].values[idx],
        })
        user_loc.to_csv(RAW / "synthetic_user_location.csv", index=False)
        zips.to_csv(RAW / "synthetic_zip_reference.csv", index=False)
        print("[NOTE] using real Kaggle data — synthetic location overlay built. "
              "Bias is NOT injected here; observed disparity reflects real platform behaviour.")

    df.to_csv(RAW / "ecommerce_events.csv", index=False)
    print(f"\n[OUT] events     → data/raw/ecommerce_events.csv ({len(df):,} rows)")
    print(f"[OUT] user_loc   → data/raw/synthetic_user_location.csv")

    fetch_cfpb()
    fetch_census()
    fetch_hud()

    print("\n>>> Acquisition complete.")
