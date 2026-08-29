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
    -- The reconstructed running balance. Nullable, but only just: the stage
    -- states a figure on every row it can reach an anchor from, in either
    -- direction, so this is null exactly where running_balance_status says
    -- UNAVAILABLE -- no trusted balance anywhere in the account is reachable
    -- from the row. The CHECK at the foot of this table enforces that pairing
    -- rather than leaving it to the writer to remember.
    --
    -- It is NOT null merely because a value was unproven. An unproven figure
    -- is published with a status that says so, because a null downstream is a
    -- decision (drop it, or impute it) taken further from the evidence than
    -- this pipeline is.
    running_balance_filled       NUMERIC(18, 4),
    -- How that figure was arrived at, and the column that makes the one above
    -- safe to read. Constrained against the cleaner's vocabulary, in the order
    -- src/cleaners/balance.py declares it -- strongest evidence first, so a
    -- consumer's eligibility rule is a prefix of this list:
    --
    --   OBSERVED          stated by the source and confirmed by a neighbour
    --   DERIVED           blank, and both bracketing anchors agree on it
    --   FORWARD_DERIVED   counted forward from the last anchor; nothing after
    --   BACKWARD_DERIVED  counted back from the first anchor; nothing before
    --   UNVERIFIED        stated, with no reachable neighbour to test it
    --   CONTRADICTED      the bracketing anchors disagree; see the
    --                     discrepancy column for by how much
    --   UNAVAILABLE       no reachable anchor at all; the only null balance
    --
    -- The CHECK is a whitelist rather than a comment because this column is
    -- what every downstream consumer branches on. A typo introduced upstream
    -- would otherwise arrive as a status nobody handles and be read as
    -- "unrecognised, therefore probably fine".
    -- NOT NULL is safe to declare here because this file reaches an existing
    -- database only through `make db-reset`, which drops and rebuilds. See
    -- src/db/migrate.py: `migrate` is CREATE TABLE IF NOT EXISTS and will not
    -- alter a table it finds.
    running_balance_status       TEXT NOT NULL CHECK (
        running_balance_status IN (
            'OBSERVED', 'DERIVED', 'FORWARD_DERIVED', 'BACKWARD_DERIVED',
            'UNVERIFIED', 'CONTRADICTED', 'UNAVAILABLE'
        )
    ),
    -- What currency that figure is in. NATIVE-regime rows carry TXN_CCY;
    -- BILLING-regime rows carry the billing denomination, USD. Null exactly
    -- where the balance is, because a withheld figure has no denomination.
    -- Not derivable from TXN_CCY alone, which is why it is stored: the two
    -- disagree on every row where the source changed which column moves the
    -- balance.
    running_balance_currency     TEXT,
    -- The same balance valued in USD, so that anything aggregating across
    -- accounts is adding comparable figures. Equal to running_balance_filled
    -- wherever the currency is already USD -- at an effective rate of exactly
    -- 1.0, asserted rather than read from fx_rate, which is not 1 on most USD
    -- rows of this source. A point-in-time valuation at the row's own rate,
    -- not a replay of the history that built the balance.
    running_balance_normalized   NUMERIC(18, 4),
    -- On a CONTRADICTED row, what the two reconstructions disagree by, signed
    -- as backward minus forward. Adding it to running_balance_filled gives the
    -- other direction's answer exactly, which is why it is signed and why one
    -- column suffices where two balances would otherwise be needed. Null on
    -- every other status: nowhere else are there two answers to differ.
    --
    -- The tolerance these were judged against is not a column. It is one
    -- number for the whole run (policy.balance.reconcile_tolerance), recorded
    -- with the rest of the run's configuration rather than repeated 265,195
    -- times.
    running_balance_discrepancy  NUMERIC(18, 4),

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
    cleaned_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The balance invariant, enforced by the database rather than trusted to
    -- the writer. Every row carries either a numeric balance with a status
    -- explaining where it came from, or the single status that admits there
    -- is no number -- and never one without the other. A row with a null
    -- balance and a status of DERIVED, or a figure stamped UNAVAILABLE, is
    -- incoherent whichever direction the mistake came from, so the constraint
    -- is an equivalence and not an implication.
    CONSTRAINT balance_stated_iff_available CHECK (
        (running_balance_filled IS NULL)
        = (running_balance_status = 'UNAVAILABLE')
    ),
    -- And the discrepancy exists exactly where two reconstructions did.
    CONSTRAINT discrepancy_iff_contradicted CHECK (
        (running_balance_discrepancy IS NOT NULL)
        = (running_balance_status = 'CONTRADICTED')
    )
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
