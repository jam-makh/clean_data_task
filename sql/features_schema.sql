-- The Stage 3 rule tables: the vocabularies the feature build reads.
-- Contents are seeded from src/rules/json/ and verified against it; the shape
-- is fixed here because a consumer branches on these columns.

-- What each processing code does to a balance, and whether it is spending.
--
-- `direction` is nullable on purpose. A code the source declares no direction
-- for is UNDECLARED, not DEBIT: its amount was never signed by Stage 2 and it
-- must contribute to neither credited nor debited totals. A sentinel string
-- would be one more value every consumer has to remember to exclude.
CREATE TABLE IF NOT EXISTS rule_processing_codes (
    code            TEXT PRIMARY KEY CHECK (code ~ '^[0-9]{2}$'),
    label           TEXT NOT NULL,
    direction       TEXT CHECK (direction IN ('CREDIT', 'DEBIT')),
    -- A DEBIT that is not spend-eligible still counts in total_debited and
    -- net_flow; it just enters no spending category. Transfer Out is the case.
    spend_eligible  BOOLEAN NOT NULL DEFAULT FALSE,
    -- Spending is debit-only, so a credit that claims to be spending is
    -- incoherent whichever half is the mistake.
    CONSTRAINT spend_eligible_implies_debit CHECK (
        NOT spend_eligible OR direction = 'DEBIT'
    )
);


-- The closed set of Stage 3 spending categories.
--
-- No hierarchy: an MCC maps to exactly one of these and there is no parent to
-- roll up to. Stage 3 does not need one and adding it later is easier than
-- unpicking a level nothing reads.
CREATE TABLE IF NOT EXISTS rule_spending_categories (
    category       TEXT PRIMARY KEY CHECK (category ~ '^[a-z][a-z0-9_]*$'),
    -- Fixes the column order of the feature table, so renaming a category
    -- cannot silently reorder the output.
    display_order  INT NOT NULL UNIQUE,
    -- Where an unmapped MCC lands. Exactly one row carries it; the partial
    -- unique index below is what makes "exactly one" a constraint rather than
    -- a convention.
    is_residual    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_spending_one_residual
    ON rule_spending_categories ((TRUE)) WHERE is_residual;


-- MCC to spending category, applied only to spend-eligible transactions.
--
-- The foreign key is the point of the table: a category can only be spelled
-- one way, and a typo here fails the seed instead of inventing an eighth
-- column in the feature table.
CREATE TABLE IF NOT EXISTS rule_mcc_categories (
    mcc       TEXT PRIMARY KEY CHECK (mcc ~ '^[0-9]{4}$'),
    category  TEXT NOT NULL REFERENCES rule_spending_categories(category)
);

CREATE INDEX IF NOT EXISTS idx_rule_mcc_category
    ON rule_mcc_categories (category);
