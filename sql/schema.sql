
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
    is_holiday_month_cleaned     BOOLEAN
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_cleaned_txn_ts 
    ON cleaned_transactions (txn_ts);
CREATE INDEX IF NOT EXISTS idx_cleaned_account_seq 
    ON cleaned_transactions (account_id, txn_seq);
CREATE INDEX IF NOT EXISTS idx_cleaned_merchant 
    ON cleaned_transactions (merchant_name_cleaned);
