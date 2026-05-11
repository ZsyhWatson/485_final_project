-- ============================================================================
-- OmniStyle  ·  SOURCE schema (as-is, REES46 / Kaggle event stream)
-- ============================================================================
-- One wide table holds every behavioural event. This is the format click-stream
-- data lands in from the collector / Kafka. It is NOT in 3NF and mirrors the
-- design weaknesses behind the redlining incident.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS omnistyle_src;
SET search_path TO omnistyle_src;

CREATE TABLE ecommerce_events (
    event_time      TIMESTAMP WITH TIME ZONE,
    event_type      TEXT,                                -- view | cart | remove_from_cart | purchase
    product_id      BIGINT,
    category_id     BIGINT,
    category_code   TEXT,                                -- ~30% NULL, dot-delimited hierarchy
    brand           TEXT,                                -- ~15% NULL
    price           NUMERIC(12,2),                       -- can be 0 or negative in source
    user_id         BIGINT,
    user_session    TEXT                                 -- UUID, but unconstrained
);

-- Synthetic location overlay built by the acquisition script. It does NOT exist
-- in the original Kaggle file. Kept flat here to mirror the source-as-is shape;
-- it is normalised properly in the target schema.
CREATE TABLE user_location (
    user_id         BIGINT,
    zip             TEXT,
    pct_minority    NUMERIC(4,3),
    median_income   INTEGER
);

-- ============================================================================
-- Why this shape is not 3NF — and how each defect maps to a real problem
-- ============================================================================
-- 1) `category_code` + `brand` repeated on every event row → 1NF/3NF: non-key
--    product attributes are duplicated and can drift. Same product appears
--    with two category_codes after a re-tag.
--
-- 2) `category_code` is a dot-delimited string → repeating group, 1NF violation.
--    "electronics.audio.headphone" is really three normalised levels.
--
-- 3) No PK / FK anywhere — duplicate rows from at-least-once delivery cannot
--    be deduplicated without a synthetic key.
--
-- 4) `price` is the price at event time but isn't versioned: analysts can't
--    tell whether the item was on sale at that moment.
--
-- 5) `user_id` has no parent table — fairness audits, consent records, and
--    DSAR requests have nowhere to plug in.
--
-- 6) The **recommender's decisions are not represented at all**. We see what
--    the user clicked but not what was ranked above it, what features the
--    model used, or which model_version was deployed — making audits of the
--    redlining behaviour impossible from this table alone.
-- ============================================================================
