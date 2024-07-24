import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from datetime import timedelta
import pandas as pd
import logging

def extract_transactions(execution_date):
    logging.info("Starting extract_transactions task")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        vertica_hook = VerticaHook(vertica_conn_id='vertica_default')

        # Извлечение данных за предыдущий день
        extract_query = f"""
        SELECT * FROM public.transactions
        WHERE transaction_dt >= '{execution_date.subtract(days=1).format('YYYY-MM-DD')}'
          AND transaction_dt < '{execution_date.format('YYYY-MM-DD')}'
        """
        transactions = pg_hook.get_pandas_df(extract_query)

        if not transactions.empty:
            transactions.to_csv('/tmp/transactions.csv', index=False)
            with open('/tmp/transactions.csv', 'r') as f:
                vertica_hook.run_copy("""
                    COPY STV2024021927__STAGING.transactions (
                        operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                    ) FROM LOCAL STDIN DELIMITER ',' ENCLOSED BY '"' ABORT ON ERROR;
                """, f)
        else:
            logging.info("No data found in public.transactions for the given date range")
    except Exception as e:
        logging.error(f"Error extracting transactions: {e}")

def extract_currencies(execution_date):
    logging.info("Starting extract_currencies task")
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        vertica_hook = VerticaHook(vertica_conn_id='vertica_default')

        # Извлечение данных за предыдущий день
        extract_query = f"""
        SELECT * FROM public.currencies
        WHERE date_update >= '{execution_date.subtract(days=1).format('YYYY-MM-DD')}'
          AND date_update < '{execution_date.format('YYYY-MM-DD')}'
        """
        currencies = pg_hook.get_pandas_df(extract_query)

        if not currencies.empty:
            currencies.to_csv('/tmp/currencies.csv', index=False)
            with open('/tmp/currencies.csv', 'r') as f:
                vertica_hook.run_copy("""
                    COPY STV2024021927__STAGING.currencies (
                        date_update,
                        currency_code,
                        currency_code_with,
                        currency_with_div
                    ) FROM LOCAL STDIN DELIMITER ',' ENCLOSED BY '"' ABORT ON ERROR;
                """, f)
        else:
            logging.info("No data found in public.currencies for the given date range")
    except Exception as e:
        logging.error(f"Error extracting currencies: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '1_data_import',
    default_args=default_args,
    description='Import data from PostgreSQL to Vertica',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    fetch_transactions_task = PythonOperator(
        task_id='fetch_transactions',
        python_callable=extract_transactions,
        provide_context=True
    )

    fetch_currencies_task = PythonOperator(
        task_id='fetch_currencies',
        python_callable=extract_currencies,
        provide_context=True
    )

    fetch_transactions_task >> fetch_currencies_task
