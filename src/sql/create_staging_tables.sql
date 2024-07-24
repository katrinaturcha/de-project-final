CREATE SCHEMA IF NOT EXISTS STV2024021927__STAGING;

CREATE TABLE STV2024021927__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
);

CREATE TABLE STV2024021927__STAGING.currencies (
    date_update TIMESTAMP,
    currency_code INT,
    currency_code_with INT,
    currency_with_div NUMERIC(5, 3)
);
