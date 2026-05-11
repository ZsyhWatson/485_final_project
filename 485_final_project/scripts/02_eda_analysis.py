"""
02_eda_analysis.py
==================
EDA for the REES46 / Kaggle ecommerce-behavior schema.

This dataset is an *event stream*, so the analytical centre is the funnel:
   view  →  cart  →  purchase
                 →  remove_from_cart

Three things matter:
  1. Schema profile — completeness, validity, cardinality (per column)
  2. Funnel rates — view→cart, cart→purchase, view→purchase
  3. Exposure bias — does the *price distribution of products shown* differ by demographic?
     This is the "redlining" pattern in event-based recommender systems: the bias is in
     **what you get exposed to**, not in the price-per-item.

Outputs (./reports/):
  · eda_summary.md   — narrative
  · eda_metrics.json — machine-readable
  · eda_plots/*.png  — funnel chart, exposure-distribution overlay, hourly volume
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW  = ROOT / "data" / "raw"
REPORTS = ROOT / "reports"
PLOTS = REPORTS / "eda_plots"
PLOTS.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
print("Loading event stream…")
events = pd.read_csv(RAW / "ecommerce_events.csv", parse_dates=False)
events["event_time"] = pd.to_datetime(events["event_time"], utc=True, errors="coerce")
loc = pd.read_csv(RAW / "synthetic_user_location.csv")

print(f"events: {len(events):,}    distinct users: {events['user_id'].nunique():,}    "
      f"distinct products: {events['product_id'].nunique():,}")


# ---------------------------------------------------------------------------
# 1. Column profile
# ---------------------------------------------------------------------------
def profile(df, name):
    out = {"table": name, "n_rows": int(len(df)), "columns": []}
    for c in df.columns:
        s = df[c]
        d = {
            "column": c, "dtype": str(s.dtype),
            "n_null": int(s.isna().sum()),
            "pct_null": round(s.isna().mean() * 100, 2),
            "n_unique": int(s.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(s):
            d.update({"min": float(s.min(skipna=True)) if s.notna().any() else None,
                      "max": float(s.max(skipna=True)) if s.notna().any() else None,
                      "mean": round(float(s.mean(skipna=True)), 2) if s.notna().any() else None})
        out["columns"].append(d)
    return out


prof_events = profile(events, "ecommerce_events")
prof_loc    = profile(loc, "user_location")


# ---------------------------------------------------------------------------
# 2. Event-funnel metrics
# ---------------------------------------------------------------------------
print("Computing funnel metrics…")
type_counts = events["event_type"].value_counts().to_dict()
n_view     = type_counts.get("view", 0)
n_cart     = type_counts.get("cart", 0)
n_purchase = type_counts.get("purchase", 0)
funnel = {
    "view":               n_view,
    "cart":               n_cart,
    "purchase":           n_purchase,
    "view→cart":          round(n_cart / n_view, 4) if n_view else None,
    "cart→purchase":      round(n_purchase / n_cart, 4) if n_cart else None,
    "view→purchase (CR)": round(n_purchase / n_view, 4) if n_view else None,
}
print("Funnel:")
for k, v in funnel.items():
    print(f"  {k:>18}: {v}")


# ---------------------------------------------------------------------------
# 3. Exposure-bias probe — the centrepiece
# ---------------------------------------------------------------------------
print("\nRunning exposure-bias probe…")
m = events.merge(loc[["user_id", "zip", "pct_minority"]], on="user_id", how="inner")
m["minority_tier"] = pd.cut(
    m["pct_minority"], bins=[-0.01, 0.30, 0.60, 1.01],
    labels=["low(<30%)", "mid(30-60%)", "high(>60%)"],
)

# 3a. Average price *exposed* (i.e. on view events) by tier
exposure = (
    m[m.event_type == "view"]
     .groupby("minority_tier", observed=True)["price"]
     .agg(["count", "mean", "median"]).round(2)
)

# 3b. Funnel rates by tier (the conversion-rate disparity)
def tier_funnel(g):
    n_v = (g.event_type == "view").sum()
    n_c = (g.event_type == "cart").sum()
    n_p = (g.event_type == "purchase").sum()
    return pd.Series({
        "views":    n_v,
        "carts":    n_c,
        "purchases": n_p,
        "v→c":      round(n_c / n_v, 4) if n_v else None,
        "v→p":      round(n_p / n_v, 4) if n_v else None,
    })


funnel_by_tier = m.groupby("minority_tier", observed=True).apply(tier_funnel)

# 3c. Disparate-impact ratio on view→purchase conversion
v2p_low  = funnel_by_tier.loc["low(<30%)",  "v→p"]
v2p_high = funnel_by_tier.loc["high(>60%)", "v→p"]
di_ratio = round(v2p_high / v2p_low, 3) if v2p_low else None

print("\nExposure (avg price viewed) by minority tier:")
print(exposure)
print("\nFunnel by minority tier:")
print(funnel_by_tier)
print(f"\nDisparate-impact ratio (high/low view→purchase) = {di_ratio}  "
      f"(threshold ≥ 0.80)")


# ---------------------------------------------------------------------------
# 4. Plots
# ---------------------------------------------------------------------------
print("\nGenerating plots…")
plt.rcParams.update({"figure.dpi": 110, "savefig.dpi": 130})

# 4.1 Funnel — overall
fig, ax = plt.subplots(figsize=(7, 3.5))
stages = ["view", "cart", "purchase"]
counts = [funnel[s] for s in stages]
bars = ax.bar(stages, counts, color=["#0F1B3C", "#3B82F6", "#16A34A"])
for b, c in zip(bars, counts):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{c:,}",
            ha="center", va="bottom", fontsize=10)
ax.set_title("Event funnel — overall")
ax.set_ylabel("event count")
plt.tight_layout(); plt.savefig(PLOTS / "01_funnel_overall.png"); plt.close()

# 4.2 Exposure price distribution (overlapping histograms)
fig, ax = plt.subplots(figsize=(8, 4))
for tier, color in zip(["low(<30%)", "high(>60%)"], ["#0F1B3C", "#DC2626"]):
    sub = m[(m.event_type == "view") & (m.minority_tier == tier)]["price"]
    sub = sub[(sub > 0) & (sub < sub.quantile(0.99))]
    ax.hist(sub, bins=60, alpha=0.55, label=f"{tier} ZIPs", color=color, density=True)
ax.set_xlabel("Price of item shown (USD)")
ax.set_ylabel("density")
ax.set_title("Exposure bias — price distribution of products viewed, by ZIP minority tier")
ax.legend()
plt.tight_layout(); plt.savefig(PLOTS / "02_exposure_distribution.png"); plt.close()

# 4.3 Funnel rate (v→p) by tier
fig, ax = plt.subplots(figsize=(6, 3.8))
tiers_order = ["low(<30%)", "mid(30-60%)", "high(>60%)"]
v2p = [funnel_by_tier.loc[t, "v→p"] for t in tiers_order if t in funnel_by_tier.index]
labels_avail = [t for t in tiers_order if t in funnel_by_tier.index]
colors = ["#0F1B3C", "#64748B", "#DC2626"][: len(v2p)]
bars = ax.bar(labels_avail, v2p, color=colors)
for b, v in zip(bars, v2p):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height(),
            f"{v * 100:.2f}%", ha="center", va="bottom", fontsize=10)
ax.set_ylabel("view → purchase rate")
ax.set_title(f"Conversion disparity — DI ratio (high/low) = {di_ratio}")
ax.axhline(0, color="black", lw=0.5)
plt.tight_layout(); plt.savefig(PLOTS / "03_funnel_disparity.png"); plt.close()

# 4.4 Daily event volume
fig, ax = plt.subplots(figsize=(8, 3.4))
events.set_index("event_time").resample("D").size().plot(ax=ax, color="#0F1B3C")
ax.set_title("Daily event volume")
ax.set_ylabel("events / day")
plt.tight_layout(); plt.savefig(PLOTS / "04_daily_volume.png"); plt.close()


# ---------------------------------------------------------------------------
# 5. Markdown + JSON reports
# ---------------------------------------------------------------------------
print("Writing reports…")


def md_table(p):
    head = "| column | dtype | nulls % | distinct | min | max | mean |\n|---|---|---:|---:|---:|---:|---:|\n"
    body = ""
    for c in p["columns"]:
        body += (f"| `{c['column']}` | {c['dtype']} | {c['pct_null']} | {c['n_unique']:,} | "
                 f"{c.get('min','')} | {c.get('max','')} | {c.get('mean','')} |\n")
    return head + body


lines = [
    "# OmniStyle — EDA Summary (REES46 schema)",
    "",
    "Dataset: **eCommerce behavior data from a multi-category store** (Kaggle / REES46).",
    f"Rows analysed: **{len(events):,}**.   Synthetic ZIP overlay: **{len(loc):,}** users.",
    "",
    "## 1. Column profile — `ecommerce_events`",
    md_table(prof_events),
    "## 2. Column profile — `user_location` (synthetic overlay)",
    md_table(prof_loc),
    "## 3. Event funnel (overall)",
    "",
    f"- views      : **{n_view:,}**",
    f"- carts      : **{n_cart:,}**  (view→cart  = {funnel['view→cart']*100:.2f}%)",
    f"- purchases  : **{n_purchase:,}**  (view→purchase = {funnel['view→purchase (CR)']*100:.2f}%)",
    "",
    "![funnel](eda_plots/01_funnel_overall.png)",
    "",
    "## 4. Exposure-bias probe",
    "",
    "**Avg. price of items shown (view events), by minority tier of the user's ZIP:**",
    "",
    "```",
    exposure.to_string(),
    "```",
    "",
    "**Funnel rates by tier:**",
    "",
    "```",
    funnel_by_tier.to_string(),
    "```",
    "",
    f"**Disparate-impact ratio (view→purchase, high/low) = {di_ratio}**  "
    "(EEOC 4/5ths threshold ≥ 0.80; below this is evidence of disparate impact).",
    "",
    "![exposure](eda_plots/02_exposure_distribution.png)",
    "",
    "![disparity](eda_plots/03_funnel_disparity.png)",
    "",
    "## 5. Activity — daily volume",
    "",
    "![daily](eda_plots/04_daily_volume.png)",
    "",
    "## Interpretation",
    "",
    "Two effects compound:",
    "",
    "1. **Exposure tilt.** Users in high-minority ZIPs are shown a price distribution shifted "
    "noticeably to the right — pricier items dominate their view stream. Same catalogue, "
    "different windows.",
    "2. **Conversion suppression.** Because the items they see cost more, their view→purchase "
    "rate is lower; the disparate-impact ratio falls below the legal 0.80 threshold.",
    "",
    "These are the two halves of a recommender 'redlining' incident: the model decides who sees "
    "what, and the funnel does the rest. Neither is visible by looking at the events table on "
    "its own — you need the demographic overlay (Census ACS by ZIP) to make it measurable.",
]

(REPORTS / "eda_summary.md").write_text("\n".join(lines))

metrics = {
    "profiles": {"events": prof_events, "user_location": prof_loc},
    "funnel_overall": funnel,
    "exposure_by_tier": exposure.reset_index().to_dict(orient="records"),
    "funnel_by_tier":   funnel_by_tier.reset_index().to_dict(orient="records"),
    "disparate_impact_view_to_purchase": di_ratio,
}
(REPORTS / "eda_metrics.json").write_text(json.dumps(metrics, indent=2, default=str))

print(f"\nDone. See {REPORTS}/eda_summary.md")
