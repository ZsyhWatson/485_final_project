# OmniStyle — Data Quality Report (REES46 event schema)

Issues are graded against the **DAMA-DMBOK six dimensions** plus two additions specific to this dataset shape:

- **Idempotency** — the event stream comes from at-least-once delivery, so duplicates are a class of defect of their own.
- **Fairness** (sub-dimension of Accuracy) — introduced for the *Digital Trust First* initiative.

| # | Sev | Column | Dimension | Violations | % | Issue | Remediation |
|---|---|---|---|---:|---:|---|---|
| 1 | 🔴 Critical | `view price distribution + v→p rate` | Accuracy (Fairness) | 240,295 | 47.8029 | Disparate-impact ratio (high/low) on view→purchase = 0.761; threshold 0.80. High-minority-ZIP users are exposed to a higher-priced product mix and convert ~24% less often. | Two-step fix. (a) Strip ZIP / city / lat / lng / region_id from the recommender feature store and re-train. (b) Add a nightly pre-deploy fairness gate that recomputes DI ratio per (model_version × week) and blocks deployment when ratio < 0.85. |
| 2 | 🔴 Critical | `price` | Validity | 251 | 0.05 | Negative prices. | CHECK (price >= 0); route violations to a quarantine topic. |
| 3 | 🔴 Critical | `event_type` | Validity | 0 | 0.0 | event_type outside the allowed enum. | Define event_type as ENUM in target DDL; reject unknowns at the collector. |
| 4 | 🟠 High | `category_code` | Completeness | 150,733 | 29.9966 | category_code is NULL on ~30% of events; this is a known REES46 source defect (legacy products lack a tagged category). | Backfill from category_id via a category dimension table; products with truly missing categories should be flagged 'unmapped' rather than NULL so downstream joins still match. |
| 5 | 🟠 High | `(full row)` | Uniqueness / Idempotency | 2,500 | 0.4975 | Exact duplicate event rows from upstream replay (Kafka redelivery or click-tracker retry). | Add a synthetic event_id = hash(event_time, user_session, product_id, event_type) and DEDUP on it at ingest. Kafka consumers must use idempotent writes. |
| 6 | 🟠 High | `price` | Validity | 1,003 | 0.1996 | Zero-price events; ambiguous (free item vs missing data). | Allow only when product is in a known free-tier list; otherwise quarantine. |
| 7 | 🟠 High | `user_session` | Completeness | 502 | 0.0999 | Empty user_session breaks sessionisation and per-session funnel analysis. | Reject events without user_session at the collector; the client SDK must mint a UUID before any event fires. |
| 8 | 🟠 High | `product_id ↔ category_code` | Consistency | 0 | 0.0 | A product appears under multiple category_codes — taxonomy drift in the event stream. | Source category from a dim_product table; treat the value in events as untrusted. |
| 9 | 🟡 Medium | `brand` | Completeness | 75,346 | 14.9942 | Brand is NULL on ~15% of events. | Maintain a product→brand override table; allow NULL only for unbranded items (generic / private-label). |
| 10 | 🟡 Medium | `event_time` | Uniqueness / Idempotency | 2,500 | 0.4975 | Same (user, product, event_type) repeated within 1 second — likely double-fire from the SDK. | Debounce in the client SDK; debounce again at the stream-processor with a 2-sec window. |
| 11 | 🟡 Medium | `event_time` | Timeliness | 0 | 0.0 | Events with timestamps in the future (clock skew). | CHECK (event_time <= NOW() + interval '5 minutes'); reject older skews from the collector. |
| 12 | 🟡 Medium | `product_id ↔ brand` | Consistency | 0 | 0.0 | A product appears with conflicting brand strings. | Same fix — brand belongs in dim_product, not in every event row. |
| 13 | 🟡 Medium | `user_id` | Consistency | 0 | 0.0 | Events with no matching user_location row (anonymous / first-touch users). | Allow but flag; downstream fairness analytics excludes these rather than imputing. |
| 14 | 🟢 Low | `category_code` | Validity | 0 | 0.0 | category_code does not match the expected dot-delimited hierarchy. | Validate against the canonical category dimension table; route invalid taxonomy labels into a curation queue. |

## Remediation rollout

**Phase 1 — Stop the bleeding (week 1-2)**
1. Strip `zip_code`, `city`, `lat`, `lng`, `region_id` from the recommender feature store; deploy a hotfix model trained without geographic features. Owner: ML Platform.
2. Add an idempotency-key check on the events Kafka consumer so duplicate rows can never land in the warehouse. Owner: Data Engineering.

**Phase 2 — Schema migration (week 3-6)**
3. Land the target 3NF schema (see `models/data_model_design.md`): split the wide event row into `fact_event` + `dim_product` + `dim_category` + `dim_brand` + `dim_session`.
4. Source `category_code` and `brand` from `dim_product` only; treat the values inside fact rows as advisory and unjoinable.
5. Backfill cleansed history through `dbt`. Add `great_expectations` test suites — one per DQ dimension above — gated in CI.

**Phase 3 — Continuous governance (week 7+)**
6. Nightly fairness job: recompute the view→purchase DI ratio per (model_version × week) and alert PagerDuty when the ratio drops below 0.85.
7. Quarterly external audit aligned with NYC Local Law 144 / EU AI Act pre-deployment bias assessment requirements.
8. Stand up a Data Steward council with explicit ownership of `fact_event`, `dim_product`, and the `recommendation_event` log.