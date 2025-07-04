from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import os
import json
import pandas as pd


dag_dir = os.path.dirname(os.path.abspath(__file__))
row_tracking_path = os.path.join(dag_dir, 'inserted_rows_tracking.json')



def get_target_connection():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_target")
        conn = hook.get_conn()
        return conn
    except Exception as e:
        print(f"TARGET CONNECTION ERROR: {str(e)}")
        raise



def log_upload_to_db(table_name, row_count, conn):
    """
    Logs the upload info to the 'upload_log' table in PostgreSQL.

    Args:
        table_name (str): Name of the uploaded table.
        row_count (int): Number of rows uploaded.
        conn: psycopg2 connection object.
    """
    today = date.today()
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





def digital_sql_script():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'the_digital_script.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        # Execute the SQL script
        cur.execute(sql)
        
        # Get the number of rows affected
        rows_affected = cur.rowcount
        
        # Log the number of rows affected
        log_upload_to_db("out_path_4_events", rows_affected, conn)
        
        conn.commit()
        print(f"✅ SQL script executed successfully. {rows_affected} rows affected.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to execute SQL script: {e}")
        raise
    finally:
        conn.close()


def run_summary_static():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'summary_static.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM dws.summary_static;")
        row_count = cur.fetchone()[0]
        log_upload_to_db("summary_static", row_count, conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed: {e}")
        raise
    finally:
        conn.close()

def run_summary_year_month_based():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'summary_year_month_based.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        # Split SQL statements and run each
        for statement in sql.split(';'):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        # Tabloyu yeni isimle oluşturduysan, burada SELECT yapabilirsin
        cur.execute("SELECT COUNT(*) FROM dws.summary_year_month;")
        row_count = cur.fetchone()[0]
        log_upload_to_db("summary_year_month", row_count, conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed: {e}")
        raise
    finally:
        conn.close()


def run_summary_year_month_fund():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'summary_year_month_fund.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM dws.summary_year_month_fund_based;")
        row_count = cur.fetchone()[0]
        log_upload_to_db("summary_year_month_fund", row_count, conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed: {e}")
        raise
    finally:
        conn.close()


def run_arketip():
    conn = get_target_connection()
    cur = conn.cursor()
    script_path = os.path.join(dag_dir, 'sql', 'arketip.sql')
    with open(script_path, 'r') as f:
        sql = f.read()
    try:
        # Birden fazla statement varsa, split edip çalıştır
        for statement in sql.split(';'):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        # Son tabloyu say
        cur.execute("SELECT COUNT(*) FROM dws.arketip;")
        row_count = cur.fetchone()[0]
        log_upload_to_db("arketip", row_count, conn)
        conn.commit()
        print(f"✅ arketip.sql executed successfully. {row_count} rows in arketip.")
    except Exception as e:
        conn.rollback()
        print(f"Failed: {e}")
        raise
    finally:
        conn.close()