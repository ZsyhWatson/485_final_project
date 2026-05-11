-- ============================================================================
-- OmniStyle  ·  TARGET schema  (3NF + governance, event-stream world)
-- ============================================================================
-- Dialect: PostgreSQL 14+.
-- Layered design:
--   1. dim_*   — reference / dimensions (slowly-changing reference data)
--   2. master  — entities with history (user, product, session)
--   3. fact_*  — append-only event tables
--   4. ml_*    — recommender provenance (was missing before)
--   5. gov_*   — governance / audit / fairness  (in schema omnistyle_gov)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS omnistyle_tgt;
CREATE SCHEMA IF NOT EXISTS omnistyle_pii;
CREATE SCHEMA IF NOT EXISTS omnistyle_gov;
SET search_path TO omnistyle_tgt;

-- ----------------------------------------------------------------------------
-- 1. Reference / Dimension layer
-- ----------------------------------------------------------------------------
CREATE TABLE dim_zip (
    zip5        CHAR(5) PRIMARY KEY,
    city        TEXT NOT NULL,
    state_code  CHAR(2) NOT NULL,
    CONSTRAINT zip5_format CHECK (zip5 ~ '^[0-9]{5}$')
);

-- ACS demographic snapshot (read-only by audit pipelines; never by the recommender)
CREATE TABLE ref_census_acs (
    zip5              CHAR(5) NOT NULL REFERENCES dim_zip(zip5),
    year              SMALLINT NOT NULL,
    total_pop         INTEGER,
    median_hh_income  INTEGER,
    pct_white         NUMERIC(5,2),
    pct_black         NUMERIC(5,2),
    pct_hispanic      NUMERIC(5,2),
    PRIMARY KEY (zip5, year)
);

CREATE TABLE dim_brand (
    brand_id    SERIAL PRIMARY KEY,
    brand_name  TEXT NOT NULL UNIQUE
);

-- Category hierarchy normalised. The Kaggle string "electronics.audio.headphone"
-- becomes three rows joined by parent_id.
CREATE TABLE dim_category (
    category_id      SERIAL PRIMARY KEY,
    parent_id        INTEGER REFERENCES dim_category(category_id),
    name             TEXT NOT NULL,
    level            SMALLINT NOT NULL CHECK (level BETWEEN 1 AND 5),
    full_path        TEXT NOT NULL UNIQUE,                  -- e.g. "electronics.audio.headphone"
    rees46_category_id BIGINT                               -- preserved from source
);

CREATE TABLE dim_event_type (
    event_type_code  TEXT PRIMARY KEY                       -- view | cart | remove_from_cart | purchase
        CHECK (event_type_code IN ('view', 'cart', 'remove_from_cart', 'purchase')),
    description      TEXT NOT NULL
);

CREATE TABLE dim_model_version (
    model_version_id  SERIAL PRIMARY KEY,
    version_string    TEXT NOT NULL UNIQUE,
    deployed_at       TIMESTAMP NOT NULL,
    retired_at        TIMESTAMP,
    notes             TEXT
);

-- ----------------------------------------------------------------------------
-- 2. Master layer (entities with history)
-- ----------------------------------------------------------------------------
CREATE TABLE "user" (
    user_id              BIGINT PRIMARY KEY,                -- preserved from source
    email_hash           CHAR(64) UNIQUE,                   -- sha256(lower(email))
    signup_date          DATE,
    current_address_id   INTEGER,                           -- FK set after user_address insert
    created_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Restricted PII mirror — separate schema, RLS applies
CREATE TABLE omnistyle_pii.user_pii (
    user_id  BIGINT PRIMARY KEY REFERENCES omnistyle_tgt."user"(user_id),
    email    TEXT,
    full_name TEXT,
    phone    TEXT,
    CONSTRAINT email_format CHECK (email IS NULL OR email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

CREATE TABLE user_address (
    address_id   SERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES "user"(user_id),
    zip5         CHAR(5) NOT NULL REFERENCES dim_zip(zip5),
    valid_from   TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to     TIMESTAMP,
    is_current   BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (user_id, valid_from)
);

ALTER TABLE "user"
  ADD CONSTRAINT fk_user_current_addr FOREIGN KEY (current_address_id)
  REFERENCES user_address(address_id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE product (
    product_id        BIGINT PRIMARY KEY,                   -- preserved from source
    sku               TEXT UNIQUE,
    brand_id          INTEGER REFERENCES dim_brand(brand_id),
    category_id       INTEGER REFERENCES dim_category(category_id),
    is_unbranded      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Versioned price history (replaces the unversioned `price` column on every event)
CREATE TABLE product_price_history (
    product_id   BIGINT NOT NULL REFERENCES product(product_id),
    price        NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    list_price   NUMERIC(12,2),                              -- if differs from offered price (sale)
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    PRIMARY KEY (product_id, valid_from)
);

-- Sessions get a parent table — answers "is this session a real one?" and lets
-- us link multiple events together while still enforcing FK on every fact row.
CREATE TABLE dim_session (
    session_uuid   UUID PRIMARY KEY,
    user_id        BIGINT NOT NULL REFERENCES "user"(user_id),
    started_at     TIMESTAMP NOT NULL,
    last_event_at  TIMESTAMP NOT NULL,
    n_events       INTEGER NOT NULL DEFAULT 0
);

-- ----------------------------------------------------------------------------
-- 3. Fact layer (append-only)
-- ----------------------------------------------------------------------------
-- One row per behavioural event. The synthetic event_id makes deduplication
-- possible — the source stream had no key.
CREATE TABLE fact_event (
    event_id          BIGSERIAL PRIMARY KEY,                -- synthetic, monotonically increasing
    event_hash        CHAR(64) NOT NULL UNIQUE,             -- sha256(event_time, session, product, type)
    event_time        TIMESTAMP NOT NULL,
    event_type_code   TEXT NOT NULL REFERENCES dim_event_type(event_type_code),
    user_id           BIGINT NOT NULL REFERENCES "user"(user_id),
    session_uuid      UUID NOT NULL REFERENCES dim_session(session_uuid),
    product_id        BIGINT NOT NULL REFERENCES product(product_id),
    -- price snapshotted at event time (kept for fast analytics; reconcilable to product_price_history)
    price_at_event    NUMERIC(12,2) NOT NULL CHECK (price_at_event >= 0),
    CONSTRAINT chk_event_not_future
        CHECK (event_time <= NOW() + INTERVAL '5 minutes')
);

CREATE INDEX idx_fact_event_user_time ON fact_event (user_id, event_time);
CREATE INDEX idx_fact_event_session   ON fact_event (session_uuid);
CREATE INDEX idx_fact_event_product   ON fact_event (product_id, event_type_code);

-- ----------------------------------------------------------------------------
-- 4. Recommender provenance — the missing table from the incident
-- ----------------------------------------------------------------------------
-- One row for every recommendation impression the model emitted.
CREATE TABLE recommendation_event (
    rec_id            BIGSERIAL PRIMARY KEY,
    user_id           BIGINT NOT NULL REFERENCES "user"(user_id),
    session_uuid      UUID NOT NULL REFERENCES dim_session(session_uuid),
    product_id        BIGINT NOT NULL REFERENCES product(product_id),
    model_version_id  INTEGER NOT NULL REFERENCES dim_model_version(model_version_id),
    rec_timestamp     TIMESTAMP NOT NULL,
    rank              SMALLINT NOT NULL CHECK (rank > 0 AND rank <= 200),
    score             REAL,
    CONSTRAINT chk_rec_not_future CHECK (rec_timestamp <= NOW())
);

-- Feature snapshot for every recommendation. The CHECK guarantees no geo features.
CREATE TABLE ml_feature_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    rec_id          BIGINT NOT NULL REFERENCES recommendation_event(rec_id),
    feature_name    TEXT NOT NULL,
    feature_value   TEXT,
    CONSTRAINT no_geo_features
        CHECK (feature_name NOT IN
               ('zip','zip5','zip_code','city','state','lat','lng',
                'longitude','latitude','region_id','neighborhood'))
);

CREATE INDEX idx_ml_feature_rec ON ml_feature_snapshot (rec_id);

-- ----------------------------------------------------------------------------
-- 5. Governance layer
-- ----------------------------------------------------------------------------
SET search_path TO omnistyle_gov;

CREATE TABLE consent_event (
    consent_id    SERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    consent_type  TEXT NOT NULL,           -- 'marketing','behavioural_pricing','personalised_recs'
    granted       BOOLEAN NOT NULL,
    ts            TIMESTAMP NOT NULL DEFAULT NOW(),
    source        TEXT
);

CREATE TABLE dq_rule (
    rule_id     SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    dimension   TEXT NOT NULL CHECK (dimension IN
                ('Completeness','Uniqueness','Idempotency','Validity','Consistency',
                 'Accuracy','Timeliness','Fairness')),
    severity    TEXT NOT NULL CHECK (severity IN ('Critical','High','Medium','Low')),
    sql_test    TEXT NOT NULL,
    threshold   NUMERIC
);

CREATE TABLE dq_violation (
    violation_id   BIGSERIAL PRIMARY KEY,
    rule_id        INTEGER NOT NULL REFERENCES dq_rule(rule_id),
    table_name     TEXT NOT NULL,
    row_pk         TEXT NOT NULL,
    captured_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMP
);

-- The fairness ledger — populated by the nightly job
CREATE TABLE fairness_metric (
    metric_id          BIGSERIAL PRIMARY KEY,
    model_version_id   INTEGER NOT NULL,
    metric_type        TEXT NOT NULL,             -- 'disparate_impact_v2p','exposure_kl_div',
                                                  -- 'price_exposure_ratio','equal_opportunity'
    group_a            TEXT NOT NULL,             -- e.g. 'minority>50'
    group_b            TEXT NOT NULL,             -- e.g. 'minority<=50'
    metric_value       NUMERIC NOT NULL,
    threshold          NUMERIC NOT NULL,
    passed             BOOLEAN NOT NULL,
    evaluated_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE audit_log (
    event_id     BIGSERIAL PRIMARY KEY,
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,                   -- READ_PII | JOIN_DEMOGRAPHIC | DEPLOY_MODEL ...
    object_type  TEXT NOT NULL,
    object_id    TEXT,
    payload      JSONB,
    ts           TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE data_lineage (
    lineage_id      SERIAL PRIMARY KEY,
    source_table    TEXT NOT NULL,
    source_column   TEXT,
    target_table    TEXT NOT NULL,
    target_column   TEXT,
    transform_id    TEXT,
    captured_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 6. Derived views (replace fields removed from fact_event / events)
-- ----------------------------------------------------------------------------
SET search_path TO omnistyle_tgt;

-- Funnel view by user — replaces ad-hoc rollups
CREATE OR REPLACE VIEW v_user_funnel AS
SELECT
    e.user_id,
    COUNT(*) FILTER (WHERE e.event_type_code = 'view')      AS n_views,
    COUNT(*) FILTER (WHERE e.event_type_code = 'cart')      AS n_carts,
    COUNT(*) FILTER (WHERE e.event_type_code = 'purchase')  AS n_purchases
FROM fact_event e
GROUP BY e.user_id;

-- Demographic-overlaid view used ONLY by the audit pipeline
CREATE OR REPLACE VIEW v_event_with_demographics AS
SELECT
    e.event_id, e.event_time, e.event_type_code,
    e.user_id, e.product_id, e.price_at_event,
    ua.zip5,
    acs.median_hh_income,
    COALESCE(acs.pct_black, 0) + COALESCE(acs.pct_hispanic, 0) AS pct_minority
FROM fact_event e
JOIN "user" u                     ON u.user_id = e.user_id
LEFT JOIN user_address ua         ON ua.address_id = u.current_address_id
LEFT JOIN ref_census_acs acs      ON acs.zip5 = ua.zip5
                                  AND acs.year = EXTRACT(YEAR FROM e.event_time)::INT;
