from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.utils.dates import days_ago
import pendulum
from datetime import timedelta
import sys
import os

dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_dir)  # Add dags folder to sys.path

from extract_transactions import main as extract_main
from matrix_airflow import digital_sql_script, run_summary_static, run_summary_year_month_based, run_summary_year_month_fund, run_arketip  # Now this will work



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='digital_data_pipeline',
    default_args=default_args,
    description='Main extraction followed by SQL processing tasks',
    schedule='@daily',
    start_date=pendulum.now().subtract(days=1),
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
    run_summary_static = PythonOperator(
        task_id='run_summary_static_sql',
        python_callable=run_summary_static
    )
    run_summary_ym = PythonOperator(
        task_id='run_summary_year_month_based_sql',
        python_callable=run_summary_year_month_based
    )
    run_summary_ym_fund = PythonOperator(
        task_id='run_summary_year_month_fund_sql',
        python_callable=run_summary_year_month_fund
    )
    run_arketip = PythonOperator(
        task_id='run_arketip_sql',
        python_callable=run_arketip
    )   

    # Define dependencies
    run_main >> [run_matrix_script, run_summary_static, run_summary_ym, run_summary_ym_fund, run_arketip]

