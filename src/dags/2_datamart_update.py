import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from datetime import timedelta

def update_datamart(execution_date):
    vertica_hook = VerticaHook(vertica_conn_id='vertica_default')

    update_query = f"""
    INSERT INTO STV2024021927__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    SELECT
        CAST(transaction_dt AS DATE) AS date_update,
        CAST(currency_code AS INT) AS currency_from,
        SUM(CAST(amount AS NUMERIC(10, 2))) AS amount_total,
        COUNT(*) AS cnt_transactions,
        AVG(CAST(amount AS NUMERIC(10, 2))) AS avg_transactions_per_account,
        COUNT(DISTINCT CAST(account_number_from AS INT)) AS cnt_accounts_make_transactions
    FROM
        (
            SELECT
                transaction_dt,
                currency_code,
                amount,
                account_number_from
            FROM
                STV2024021927__STAGING.transactions_new
            WHERE
                status = 'done' AND
                transaction_dt IS NOT NULL AND
                currency_code IS NOT NULL AND
                amount IS NOT NULL AND
                account_number_from IS NOT NULL AND
                REGEXP_LIKE(currency_code::varchar, '^[0-9]+$') AND
                REGEXP_LIKE(amount::varchar, '^[0-9]+$') AND
                REGEXP_LIKE(account_number_from::varchar, '^[0-9]+$')
        ) AS filtered_data
    GROUP BY
        CAST(transaction_dt AS DATE),
        CAST(currency_code AS INT);
    """
    
    vertica_hook.run(update_query)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 10, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '2_datamart_update',
    default_args=default_args,
    description='Update datamart in Vertica',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    update_datamart_task = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_datamart,
        provide_context=True
    )
