CREATE SCHEMA IF NOT EXISTS STV2024021927__DWH;

CREATE TABLE IF NOT EXISTS STV2024021927__DWH.global_metrics (
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC,
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC,
    cnt_accounts_make_transactions INT
);
