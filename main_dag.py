from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_dir)  # Add dags folder to sys.path

from extract_transactions import main as extract_main
from matrix_airflow import digital_sql_script  # Now this will work


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='digital_data_pipeline',
    default_args=default_args,
    description='Main extraction followed by SQL processing tasks',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['digital', 'etl']
) as dag:

    # Task 1: Run Python main() from extract_transactions.py
    run_main = PythonOperator(
        task_id='run_extract_transactions_main',
        python_callable=extract_main
    )

    # Task 2: Run SQL script via Python function from matrix_airflow.py
    run_matrix_script = PythonOperator(
        task_id='run_matrix_airflow_sql',
        python_callable=digital_sql_script
    )

    # Task 3: summary_static.sql
    run_summary_static = PostgresOperator(
        task_id='run_summary_static_sql',
        postgres_conn_id='postgres_target',
        sql='sql/summary_static.sql'
    )

    # Task 4: summary_year_month_based.sql
    run_summary_ym = PostgresOperator(
        task_id='run_summary_year_month_based_sql',
        postgres_conn_id='postgres_target',
        sql='sql/summary_year_month_based.sql'
    )

    # Task 5: summary_year_month_fund.sql
    run_summary_ym_fund = PostgresOperator(
        task_id='run_summary_year_month_fund_sql',
        postgres_conn_id='postgres_target',
        sql='sql/summary_year_month_fund.sql'
    )

    # Define dependencies
    run_main >> [run_matrix_script, run_summary_static, run_summary_ym, run_summary_ym_fund]
