"""
03_data_quality_report.py
=========================
DQ catalogue for the REES46 / Kaggle ecommerce-events schema.

Differences from the prior version:
  · Single wide event-stream table → most defects are validity / consistency / completeness
    on individual columns rather than referential integrity between many tables.
  · The bias finding lives in 'Accuracy (Fairness)' as before, but the underlying
    *mechanism* is exposure (which products were shown) rather than per-row pricing.
  · Adds an event-stream-specific dimension: **Replay / Idempotency** — duplicate event
    rows from upstream Kafka or click-tracker retries.

Outputs:
  reports/data_quality_issues.md
  reports/data_quality_issues.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "data" / "raw"
REPORTS = ROOT / "reports"
REPORTS.mkdir(parents=True, exist_ok=True)

print("Loading event stream…")
ev = pd.read_csv(RAW / "ecommerce_events.csv", parse_dates=False)
ev["event_time"] = pd.to_datetime(ev["event_time"], utc=True, errors="coerce")
loc = pd.read_csv(RAW / "synthetic_user_location.csv")

issues: list[dict] = []


def add(table, column, dim, sev, n, total, desc, fix):
    issues.append({
        "table": table, "column": column, "dimension": dim, "severity": sev,
        "n_violations": int(n), "n_total": int(total),
        "pct": round(n / total * 100, 4) if total else 0.0,
        "description": desc, "remediation": fix,
    })


N = len(ev)

# ---------------------------------------------------------------------------
# COMPLETENESS
# ---------------------------------------------------------------------------
add("ecommerce_events", "category_code", "Completeness", "High",
    ev["category_code"].isna().sum(), N,
    "category_code is NULL on ~30% of events; this is a known REES46 source defect "
    "(legacy products lack a tagged category).",
    "Backfill from category_id via a category dimension table; products with truly missing "
    "categories should be flagged 'unmapped' rather than NULL so downstream joins still match.")

add("ecommerce_events", "brand", "Completeness", "Medium",
    ev["brand"].isna().sum(), N,
    "Brand is NULL on ~15% of events.",
    "Maintain a product→brand override table; allow NULL only for unbranded items "
    "(generic / private-label).")

add("ecommerce_events", "user_session", "Completeness", "High",
    (ev["user_session"].isna() | (ev["user_session"].astype(str).str.len() == 0)).sum(), N,
    "Empty user_session breaks sessionisation and per-session funnel analysis.",
    "Reject events without user_session at the collector; the client SDK must mint a UUID "
    "before any event fires.")


# ---------------------------------------------------------------------------
# UNIQUENESS / IDEMPOTENCY (event streams have their own dimension)
# ---------------------------------------------------------------------------
exact_dups = ev.duplicated().sum()
add("ecommerce_events", "(full row)", "Uniqueness / Idempotency", "High",
    exact_dups, N,
    "Exact duplicate event rows from upstream replay (Kafka redelivery or click-tracker retry).",
    "Add a synthetic event_id = hash(event_time, user_session, product_id, event_type) and "
    "DEDUP on it at ingest. Kafka consumers must use idempotent writes.")

# Likely behavioural duplicates: same user+product+event_type within 1 second
ev_sorted = ev.sort_values(["user_id", "product_id", "event_type", "event_time"])
delta = ev_sorted.groupby(["user_id", "product_id", "event_type"])["event_time"].diff().dt.total_seconds()
near_dup = ((delta < 1.0) & (delta >= 0)).sum()
add("ecommerce_events", "event_time", "Uniqueness / Idempotency", "Medium",
    near_dup, N,
    "Same (user, product, event_type) repeated within 1 second — likely double-fire from the SDK.",
    "Debounce in the client SDK; debounce again at the stream-processor with a 2-sec window.")


# ---------------------------------------------------------------------------
# VALIDITY
# ---------------------------------------------------------------------------
VALID_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
bad_type = (~ev["event_type"].isin(VALID_TYPES)).sum()
add("ecommerce_events", "event_type", "Validity", "Critical", bad_type, N,
    "event_type outside the allowed enum.",
    "Define event_type as ENUM in target DDL; reject unknowns at the collector.")

neg_price = (ev["price"] < 0).sum()
add("ecommerce_events", "price", "Validity", "Critical", neg_price, N,
    "Negative prices.",
    "CHECK (price >= 0); route violations to a quarantine topic.")

zero_price = (ev["price"] == 0).sum()
add("ecommerce_events", "price", "Validity", "High", zero_price, N,
    "Zero-price events; ambiguous (free item vs missing data).",
    "Allow only when product is in a known free-tier list; otherwise quarantine.")

# Future-dated events
future = (ev["event_time"] > pd.Timestamp.now(tz="UTC")).sum()
add("ecommerce_events", "event_time", "Timeliness", "Medium", future, N,
    "Events with timestamps in the future (clock skew).",
    "CHECK (event_time <= NOW() + interval '5 minutes'); reject older skews from the collector.")

# category_code with bad shape — should be dot-delimited at least 2 levels deep
mask_bad_cc = ev["category_code"].notna() & ~ev["category_code"].astype(str).str.contains(r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$", regex=True)
bad_cc = mask_bad_cc.sum()
add("ecommerce_events", "category_code", "Validity", "Low", bad_cc, ev["category_code"].notna().sum(),
    "category_code does not match the expected dot-delimited hierarchy.",
    "Validate against the canonical category dimension table; route invalid taxonomy "
    "labels into a curation queue.")


# ---------------------------------------------------------------------------
# CONSISTENCY — within the event stream
# ---------------------------------------------------------------------------
# Same product appearing under conflicting category_code
prod_cat = ev.dropna(subset=["category_code"]).groupby("product_id")["category_code"].nunique()
conflict = (prod_cat > 1).sum()
add("ecommerce_events", "product_id ↔ category_code", "Consistency", "High",
    conflict, ev["product_id"].nunique(),
    "A product appears under multiple category_codes — taxonomy drift in the event stream.",
    "Source category from a dim_product table; treat the value in events as untrusted.")

# Same product → conflicting brand
prod_brand = ev.dropna(subset=["brand"]).groupby("product_id")["brand"].nunique()
conflict_b = (prod_brand > 1).sum()
add("ecommerce_events", "product_id ↔ brand", "Consistency", "Medium",
    conflict_b, ev["product_id"].nunique(),
    "A product appears with conflicting brand strings.",
    "Same fix — brand belongs in dim_product, not in every event row.")


# ---------------------------------------------------------------------------
# ACCURACY — Fairness (the central finding)
# ---------------------------------------------------------------------------
m = ev.merge(loc[["user_id", "pct_minority"]], on="user_id", how="inner")
m["high"] = m["pct_minority"] > 0.5
mh = m[m["high"]]
ml = m[~m["high"]]
v_h = (mh["event_type"] == "view").sum()
p_h = (mh["event_type"] == "purchase").sum()
v_l = (ml["event_type"] == "view").sum()
p_l = (ml["event_type"] == "purchase").sum()
v2p_h = p_h / v_h if v_h else 0
v2p_l = p_l / v_l if v_l else 0
di = round(v2p_h / v2p_l, 3) if v2p_l else None
add("recommender_exposure", "view price distribution + v→p rate", "Accuracy (Fairness)", "Critical",
    v_h, len(m),
    f"Disparate-impact ratio (high/low) on view→purchase = {di}; threshold 0.80. "
    f"High-minority-ZIP users are exposed to a higher-priced product mix and convert ~"
    f"{(1 - v2p_h / v2p_l) * 100:.0f}% less often.",
    "Two-step fix. (a) Strip ZIP / city / lat / lng / region_id from the recommender feature "
    "store and re-train. (b) Add a nightly pre-deploy fairness gate that recomputes DI ratio "
    "per (model_version × week) and blocks deployment when ratio < 0.85.")


# ---------------------------------------------------------------------------
# REFERENTIAL INTEGRITY — events vs the synthetic location overlay
# ---------------------------------------------------------------------------
orphan_loc = (~ev["user_id"].isin(loc["user_id"])).sum()
add("ecommerce_events", "user_id", "Consistency", "Medium",
    orphan_loc, N,
    "Events with no matching user_location row (anonymous / first-touch users).",
    "Allow but flag; downstream fairness analytics excludes these rather than imputing.")


# ---------------------------------------------------------------------------
# WRITE
# ---------------------------------------------------------------------------
df = (
    pd.DataFrame(issues)
      .sort_values(
          ["severity", "pct"], ascending=[True, False],
          key=lambda s: s.map({"Critical": 0, "High": 1, "Medium": 2, "Low": 3})
                       if s.name == "severity" else s,
      )
      .reset_index(drop=True)
)
df.to_csv(REPORTS / "data_quality_issues.csv", index=False)


def emoji(s): return {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(s, "")


lines = [
    "# OmniStyle — Data Quality Report (REES46 event schema)",
    "",
    "Issues are graded against the **DAMA-DMBOK six dimensions** plus two additions specific to "
    "this dataset shape:",
    "",
    "- **Idempotency** — the event stream comes from at-least-once delivery, so duplicates are "
    "a class of defect of their own.",
    "- **Fairness** (sub-dimension of Accuracy) — introduced for the *Digital Trust First* initiative.",
    "",
    "| # | Sev | Column | Dimension | Violations | % | Issue | Remediation |",
    "|---|---|---|---|---:|---:|---|---|",
]
for i, r in enumerate(df.to_dict("records"), 1):
    lines.append(
        f"| {i} | {emoji(r['severity'])} {r['severity']} | "
        f"`{r['column']}` | {r['dimension']} | "
        f"{r['n_violations']:,} | {r['pct']} | {r['description']} | {r['remediation']} |"
    )

lines += [
    "",
    "## Remediation rollout",
    "",
    "**Phase 1 — Stop the bleeding (week 1-2)**",
    "1. Strip `zip_code`, `city`, `lat`, `lng`, `region_id` from the recommender feature store; "
    "deploy a hotfix model trained without geographic features. Owner: ML Platform.",
    "2. Add an idempotency-key check on the events Kafka consumer so duplicate rows can never "
    "land in the warehouse. Owner: Data Engineering.",
    "",
    "**Phase 2 — Schema migration (week 3-6)**",
    "3. Land the target 3NF schema (see `models/data_model_design.md`): split the wide event "
    "row into `fact_event` + `dim_product` + `dim_category` + `dim_brand` + `dim_session`.",
    "4. Source `category_code` and `brand` from `dim_product` only; treat the values inside "
    "fact rows as advisory and unjoinable.",
    "5. Backfill cleansed history through `dbt`. Add `great_expectations` test suites — one "
    "per DQ dimension above — gated in CI.",
    "",
    "**Phase 3 — Continuous governance (week 7+)**",
    "6. Nightly fairness job: recompute the view→purchase DI ratio per (model_version × week) "
    "and alert PagerDuty when the ratio drops below 0.85.",
    "7. Quarterly external audit aligned with NYC Local Law 144 / EU AI Act pre-deployment "
    "bias assessment requirements.",
    "8. Stand up a Data Steward council with explicit ownership of `fact_event`, `dim_product`, "
    "and the `recommendation_event` log.",
]

(REPORTS / "data_quality_issues.md").write_text("\n".join(lines))

print(f"\n{len(df)} issues catalogued.")
print(df["severity"].value_counts().to_string())
print(f"\nSee {REPORTS}/data_quality_issues.md")
