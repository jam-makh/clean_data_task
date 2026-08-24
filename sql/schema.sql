-- The cleaned transaction store, and the one table the Spark pipeline writes.
--
-- Scoped to the `forecast_balance` profile, for the same reason
-- src/spark/pipeline.py is: that profile is ported end to end and the v4
-- workbook is not, so a column only v4 produces would be a column nothing
-- ever fills. TXN_TYPE and TERMINAL_ID are absent because the forecast
-- extract does not carry them -- not an omission, a property of the source.
--
-- Deliberately NOT here: the 35 columns of `AUDIT_COLUMNS`. They stay on the
-- frame and reach the workbook, and persisting them is a decision that can be
-- made later by adding a table; unpicking a table nobody reads is harder than
-- adding one. What IS here is the identity a run needs to be reconciled
-- against its own completion event -- see sync_job_id below.

CREATE TABLE IF NOT EXISTS cleaned_transactions (
    txn_id_cleaned               TEXT PRIMARY KEY,
    txn_seq                      BIGINT NOT NULL,
    account_id                   TEXT NOT NULL,
    user_id                      TEXT NOT NULL,
    txn_ts                       TIMESTAMP,
    settle_date_cleaned          DATE,

    -- Financials & Currency
    txn_amount_cleaned           NUMERIC(18, 4) NOT NULL,
    txn_ccy                      TEXT NOT NULL CHECK (txn_ccy ~ '^[A-Z]{3}$'),
    billing_amount               NUMERIC(18, 4) NOT NULL,
    billing_currency             TEXT NOT NULL CHECK (billing_currency ~ '^[A-Z]{3}$'),
    fx_rate                      NUMERIC(20, 10) NOT NULL,
    -- Nullable on purpose. The balance stage states a figure only where the
    -- arithmetic proves it; roughly a third of rows have no anchor to count
    -- from and are left null rather than repeating an unverified number. The
    -- workbook renders those as the word UNKNOWN, which is a rendering
    -- decision made by src/utils/io.py and never reaches this column.
    running_balance_cleaned      NUMERIC(18, 4),

    -- Merchant Information
    merchant_name_cleaned        TEXT NOT NULL,
    merchant_city_cleaned        TEXT NOT NULL,
    merchant_country_cleaned     TEXT CHECK (merchant_country_cleaned ~ '^[A-Z]{2}$'),

    -- Processing Codes & Identifiers
    processing_code_cleaned     TEXT NOT NULL CHECK (processing_code_cleaned ~ '^[0-9]{2}$'),
    processing_type_cleaned     TEXT NOT NULL,
    mcc_code_cleaned             TEXT CHECK (mcc_code_cleaned ~ '^[0-9]{4}$'),
    auth_code                    TEXT,

    -- Macro Indicators
    interest_rate_index_cleaned  NUMERIC(10, 4),
    inflation_index_cleaned      NUMERIC(10, 4),
    is_holiday_month_cleaned     BOOLEAN,

    -- Provenance ----------------------------------------------------------
    --
    -- Which load produced this row. One id per source file rather than per
    -- cleaning pass: a Spark micro-batch boundary is decided by how fast the
    -- consumer happened to be running, so an id minted per batch would split
    -- one CSV across a dozen ids and answer a question nobody asked. The
    -- producer mints it once for the file and stamps every message with it.
    --
    -- TEXT with a shape check rather than the UUID type, matching how this
    -- file already handles currency and MCC codes. The reason is practical:
    -- Spark's JDBC writer sends a string, and Postgres will not implicitly
    -- cast text to uuid on insert, so a uuid column forces a cast into every
    -- write path that touches this table. The check keeps the guarantee.
    sync_job_id                  TEXT NOT NULL
        CHECK (sync_job_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
    -- When the pipeline wrote this row, which is a different question from
    -- when the transaction happened (txn_ts) and from which load carried it
    -- (sync_job_id). Three times, three columns: collapsing any two of them
    -- is the kind of thing that is discovered six months later.
    cleaned_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_cleaned_txn_ts
    ON cleaned_transactions (txn_ts);
-- Also serves the balance seed: a streaming batch holding part of an account
-- needs that account's last known balance to anchor its chain, which is a
-- lookup on exactly this pair.
CREATE INDEX IF NOT EXISTS idx_cleaned_account_seq
    ON cleaned_transactions (account_id, txn_seq);
CREATE INDEX IF NOT EXISTS idx_cleaned_merchant
    ON cleaned_transactions (merchant_name_cleaned);
-- "Show me everything from run X" is the only question sync_job_id exists to
-- answer, and it is the one asked when a run looks wrong.
CREATE INDEX IF NOT EXISTS idx_cleaned_sync_job
    ON cleaned_transactions (sync_job_id);
