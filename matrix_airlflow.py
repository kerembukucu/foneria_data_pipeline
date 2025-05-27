from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from utils.yas_araligi import yas_araligi
from utils.meslek import meslek
from utils.egitim import egitim
from utils.bit import bit
from utils.anket_risk import anket_risk
from utils.fund_balance_ratio import fund_balance_ratio
from utils.interested_fund_count import interested_fund_count
from utils.interested_fund_risk import interested_fund_risk
from utils.transaction_volume import transaction_volume
from utils.owned_fund_count import owned_fund_count
from utils.owned_fund_risk import owned_fund_risk
from utils.activity_score import activity_score
from utils.daily_click_seperator import daily_click_seperator
from utils.average_outstanding import average_outstanding
from utils.outstanding_trend import outstanding_trend
from utils.fund_clicks import fund_clicks
from utils.fund_outstanding import fund_outstanding
from utils.fund_transaction_volume import fund_transaction_volume
from utils.registration_date import registration_date
from utils.account_ready_date import account_ready_date
from datetime import datetime, date
import os
import json
import pandas as pd


dag_dir = os.path.dirname(os.path.abspath(__file__))
row_tracking_path = os.path.join(dag_dir, 'inserted_rows_tracking.json')

def get_source_connection():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_target")
        conn = hook.get_conn()
        return conn
    except Exception as e:
        print(f"SOURCE CONNECTION ERROR: {str(e)}")
        raise


def get_target_connection():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_target")
        conn = hook.get_conn()
        return conn
    except Exception as e:
        print(f"TARGET CONNECTION ERROR: {str(e)}")
        raise


def fetch_data():
    conn = get_source_connection()
    try:
        df1 = pd.read_sql("SELECT * FROM dws.member_details", con=conn)
        df2 = pd.read_sql("SELECT * FROM dws.bias_results", con=conn)
        df3 = pd.read_sql("SELECT * FROM dws.monthly_outstandings", con=conn)
        df4 = pd.read_sql("SELECT * FROM dws.fund_actions", con=conn)
        df5 = pd.read_sql("SELECT * FROM dws.fund_details", con=conn)
        df6 = pd.read_sql("SELECT * FROM dws.actions", con=conn)
        df7 = pd.read_sql("SELECT * FROM dws.monthly_outstanding_funds", con=conn)
        df8 = pd.read_sql("SELECT * FROM dws.daily_outstandings", con=conn)
        print(f"✅ Source DB'den çekilen veri ornek:\n{df1}")
        return df1, df2, df3, df4, df5, df6, df7, df8
    finally:
        conn.close()


# def log_upload(table_name, row_count, log_file=row_tracking_path, execution_date=None):
#     if execution_date:
#         execution_date_str = execution_date.isoformat()  # Convert to ISO string format
#     else:
#         execution_date_str = None

#     today = execution_date
    
#     # Eğer log dosyası varsa ve boş değilse yükle, yoksa boş dict başlat
#     if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
#         with open(log_file, "r") as f:
#             log_data = json.load(f)
#     else:
#         log_data = {}

#     # Bugünün girdisini güncelle
#     if today not in log_data:
#         log_data[today] = {}
    
#     log_data[today][table_name] = row_count

#     # Dosyaya tekrar yaz
#     with open(log_file, "w") as f:
#         json.dump(log_data, f, indent=4)

def log_upload_to_db(table_name, row_count, conn, execution_date=None):
    """
    Logs the upload info to the 'upload_log' table in PostgreSQL.

    Args:
        table_name (str): Name of the uploaded table.
        row_count (int): Number of rows uploaded.
        conn: psycopg2 connection object.
    """
    today = execution_date

    cur = conn.cursor()

    try:
        # Set a short statement timeout to avoid issues
        cur.execute("SET statement_timeout = 30000")  # 30 seconds

        # Ensure log table exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS dws.upload_log (
                log_date DATE NOT NULL,
                table_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                PRIMARY KEY (log_date, table_name)
            );
        """)

        # Insert or update the row (UPSERT)
        cur.execute("""
            INSERT INTO dws.upload_log (log_date, table_name, row_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (log_date, table_name)
            DO UPDATE SET row_count = EXCLUDED.row_count;
        """, (today, table_name, row_count))

        conn.commit()
        print(f"Logged upload: {table_name}, {row_count} rows")

    except Exception as e:
        conn.rollback()
        print(f"Error logging upload: {e}")
        raise

    finally:
        try:
            cur.execute("RESET statement_timeout")
        except:
            pass
        cur.close()



def upsert_monthly_dataframe(df, table_name, conn, schema="public", execution_date=None):
    cur = conn.cursor()
    today = execution_date
    current_year, current_month = today.year, today.month
    is_first_day = today.day == 1

    # Only filter by year/month if these columns exist in the dataframe
    if "YEAR" in df.columns and "MONTH" in df.columns:
        df = df[(df["YEAR"] == current_year) & (df["MONTH"] == current_month)]

    # Build column info
    columns = list(df.columns)
    column_names = ', '.join([f'"{col}"' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))

    # Create table if it doesn't exist
    column_definitions = []
    for col in columns:
        dtype = df[col].dtype
        if 'int' in str(dtype):
            pg_type = 'INTEGER'
        elif 'float' in str(dtype):
            pg_type = 'NUMERIC'
        elif 'datetime' in str(dtype):
            pg_type = 'TIMESTAMP'
        else:
            pg_type = 'TEXT'
        column_definitions.append(f'"{col}" {pg_type}')

    # Add primary key constraint
    if "YEAR" in df.columns and "MONTH" in df.columns:
        primary_key = 'PRIMARY KEY ("CUST_ID", "YEAR", "MONTH")'
        key_columns = ["CUST_ID", "YEAR", "MONTH"]
    else:
        primary_key = 'PRIMARY KEY ("CUST_ID")'
        key_columns = ["CUST_ID"]

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
            {', '.join(column_definitions)},
            {primary_key}
        )
    """

    try:
        cur.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to create table: {e}")
        raise

    # Build ON CONFLICT clause
    conflict_keys = ', '.join([f'"{col}"' for col in key_columns])
    update_clause = ', '.join(
        [f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in key_columns]
    )

    if len(key_columns) == 1:
        conflict_clause = f'ON CONFLICT ({conflict_keys}) DO UPDATE SET {update_clause}'
    else:
        conflict_clause = f'ON CONFLICT ({conflict_keys}) DO '
        conflict_clause += 'NOTHING' if is_first_day else f'UPDATE SET {update_clause}'

    insert_sql = f"""
        INSERT INTO "{schema}"."{table_name}" ({column_names})
        VALUES ({placeholders})
        {conflict_clause}
    """

    values = [
        tuple(None if pd.isna(x) else x for x in row)
        for _, row in df.iterrows()
    ]

    try:
        cur.executemany(insert_sql, values)
        conn.commit()
        print(f"{len(df)} rows {'inserted' if is_first_day else 'upserted'} into {schema}.{table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Failed to upsert: {e}")
        raise

    return len(df)


def hodri_meydan():
    df1, df2, df3, df4, df5, df6, df7, df8 = fetch_data()
    yas_araligi_df = yas_araligi(df1)
    meslek_df = meslek(df1)
    egitim_df = egitim(df1)
    bit_df = bit(df2)
    anket_risk_df = anket_risk(df2)
    registration_date_df = registration_date(df1)
    account_ready_date_df = account_ready_date(df1)

    #dataframes, represents columns of the table 2
    fund_balance_ratio_df = fund_balance_ratio(df3)
    interested_fund_count_df = interested_fund_count(df4)
    interested_fund_risk_df = interested_fund_risk(df4, df5)
    transaction_volume_df = transaction_volume(df6)
    owned_fund_count_df = owned_fund_count(df7)
    owned_fund_risk_df = owned_fund_risk(df7, df5)
    activity_score_df = activity_score(df6)
    daily_click_seperator_df = daily_click_seperator(df6)
    average_outstanding_df = average_outstanding(df8)
    outstanding_trend_df = outstanding_trend(df8)

    #dataframes, represents columns of the table 3
    fund_clicks_df = fund_clicks(df4)
    fund_outstanding_df = fund_outstanding(df7)
    fund_transaction_volume_df = fund_transaction_volume(df6)

    #merging dataframes for table 1
    merged_df1 = pd.merge(yas_araligi_df, meslek_df, on='CUST_ID', how='outer')
    merged_df1 = pd.merge(merged_df1, egitim_df, on='CUST_ID', how='outer')
    merged_df1 = pd.merge(merged_df1, bit_df, on='CUST_ID', how='outer')
    merged_df1 = pd.merge(merged_df1, anket_risk_df, on='CUST_ID', how='outer')
    merged_df1 = pd.merge(merged_df1, registration_date_df, on='CUST_ID', how='outer')
    merged_df1 = pd.merge(merged_df1, account_ready_date_df, on='CUST_ID', how='outer')

    # #merging dataframes for table 2
    merged_df2 = pd.merge(fund_balance_ratio_df, average_outstanding_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, outstanding_trend_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, interested_fund_count_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, interested_fund_risk_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, transaction_volume_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, owned_fund_count_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, owned_fund_risk_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, activity_score_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')
    merged_df2 = pd.merge(merged_df2, daily_click_seperator_df, on=['CUST_ID', 'YEAR', 'MONTH'], how='outer')

    #merging dataframes for table 3
    merged_df3 = pd.merge(fund_clicks_df, fund_outstanding_df, on=['CUST_ID', 'YEAR', 'MONTH','FUND_CODE'], how='outer')
    merged_df3 = pd.merge(merged_df3, fund_transaction_volume_df, on=['CUST_ID', 'YEAR', 'MONTH','FUND_CODE'], how='outer')

    return merged_df1, merged_df2, merged_df3


def write_to_target(**kwargs):

    execution_date_str = kwargs["ds"]  # e.g., '2025-03-11'
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()

    print(f"Running for execution_date: {execution_date}")


    static, year_month_based, year_month_fund_based = hodri_meydan()
    conn = get_target_connection()

    static_rows = upsert_monthly_dataframe(static, "static", conn, schema="dws", execution_date=execution_date)
    print(f"{static_rows} rows uploaded to static")
    # log_upload("static", static_rows, execution_date=execution_date)
    log_upload_to_db("static", static_rows, conn, execution_date=execution_date)

    year_month_based_rows = upsert_monthly_dataframe(year_month_based, "year_month_based", conn, schema="dws", execution_date=execution_date)
    print(f"{year_month_based_rows} rows uploaded to year_month_based")
    # log_upload("year_month_based", year_month_based_rows, execution_date=execution_date)
    log_upload_to_db("year_month_based", year_month_based_rows, conn, execution_date=execution_date)
    year_month_fund_based_rows = upsert_monthly_dataframe(year_month_fund_based, "year_month_fund_based", conn, schema="dws", execution_date=execution_date)
    print(f"{year_month_fund_based_rows} rows uploaded to year_month_fund_based")
    # log_upload("year_month_fund_based", year_month_fund_based_rows, execution_date=execution_date)
    log_upload_to_db("year_month_fund_based", year_month_fund_based_rows, conn, execution_date=execution_date)
    print("✅ Veri Target PostgreSQL'e başarıyla yazıldı.")



def run_sql_script():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'the_digital_script.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        cur.execute(sql)
        conn.commit()
        print("✅ SQL script executed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to execute SQL script: {e}")
        raise
    finally:
        conn.close()



default_args = {
    "owner": "kerem",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
}

dag = DAG(
    "data_transfer_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
)

transfer_task = PythonOperator(
    task_id="transfer_data",
    python_callable=write_to_target,
    provide_context= True,
    dag=dag,
)

run_sql_task = PythonOperator(
    task_id="run_sql_script",
    python_callable=run_sql_script,
    provide_context= True,
    dag=dag,
)

transfer_task >> run_sql_task