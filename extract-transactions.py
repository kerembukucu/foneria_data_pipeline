import os
import psycopg2
from datetime import datetime, date
import pandas as pd
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

TARGET_SCHEMA = 'dws'


dag_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(dag_dir, "latest_job_date.json")
row_tracking_path = os.path.join(dag_dir, 'inserted_rows_tracking.json')


latest_day_key = "latest"
date_format = "%Y-%m-%d"

# Date range constants for queries
ALL_MEMBER_QUERY = """
select tmp.serno from (
SELECT m.serno FROM customer.members m
WHERE  m.status in ('A')
AND m.account_type = 'named'
and m.registration_time is not null
and m.mobile_number_verified = '1'
and m.account_ready_time is null
union
SELECT m.serno FROM customer.members m
WHERE  m.status in ('A')
AND m.mobile_number_verified = '1'
AND m.account_ready_time IS NOT null) as tmp
    """

SINGLE_MEMBER_QUERY = """
select serno from customer.members where serno = 1002
    """

DIGITAL_ACTIONS_QUERY = """
SELECT
    a.member_serno AS customer_id,
    COALESCE(a.view_name, '') AS view_name,
    COALESCE(a.activity, '') AS action,
    COALESCE(a.details, '') AS details,
    a.client_time AS event_time
FROM
    customer.activity a
WHERE
    a.member_serno IN %s
    AND a.server_time >= %s
    AND a.server_time <= %s;
"""


MEMBER_QUERY = """
SELECT a.member_serno
FROM portfolio.accounts a
WHERE a.status = 'A'
and a.member_serno = 2029
ORDER BY a.member_serno
--limit 100
    """

MEMBER_HAVING_NO_ACCOUNT_QUERY = """
SELECT m.serno FROM customer.members m
WHERE  m.status in ('A')
AND m.account_type = 'named'
and m.registration_time is not null
and m.mobile_number_verified = '1'
and m.account_ready_time is null
ORDER BY RANDOM()
LIMIT 10000
    """

FUND_ACTIONS_QUERY = """
with potential_fund_events as (
select
right(a.details,
3) as fund_code,
a.*
from
customer.activity a
where
member_serno IN %s
and (a.view_name = 'fundsList'
    or a.view_name = 'fundMarket'
    or a.view_name = 'fundDetails')
and client_time >= %s
and client_time <= %s
)
select
member_serno,
case
    when SPLIT_PART(pfe.details,',',1) = 'back'
    then 'back'
    else 'open'
end as action_type,
pfe.fund_code as fund_code,
pfe.client_time as client_time
from
potential_fund_events pfe,
product.funds f
where
pfe.fund_code = f.code
order by
member_serno,
pfe.client_time
"""

MONTHLY_OUTSTANDING_FUND_QUERY = """
WITH last_day_sizes AS (
            SELECT
                CASE
                    WHEN DATE_TRUNC('month', CURRENT_DATE) = DATE_TRUNC('month', day)
                         AND eh.day = CURRENT_DATE THEN eh.day - INTERVAL '1 day'
                    ELSE DATE_TRUNC('month', eh.day) + INTERVAL '1 month' - INTERVAL '1 day'
                END AS month_end,
                a.member_serno,
                eh.fund_code,
                MAX(day) AS last_day_of_month
            FROM
                portfolio.accounts a, portfolio.portfolios p, portfolio.eod_holdings eh
            WHERE
            	p.account_serno = a.serno
            	AND eh.portfolio_serno = p.serno
                AND a.member_serno IN %s
                AND eh.currency = 'TRY'
                AND eh.day >= %s
                AND eh.day <= %s
            GROUP BY 1, 2, 3
        ),
        fund_sizes AS (
            SELECT
                TO_CHAR(lds.month_end, 'YYYY/MM') AS month_end,
                lds.member_serno,
                eh.fund_code,
                sum(eh.SIZE) AS size
            FROM
                last_day_sizes lds, portfolio.accounts a, portfolio.portfolios p, portfolio.eod_holdings eh
            WHERE
                lds.last_day_of_month = eh.DAY
                AND p.account_serno = a.serno
                AND lds.fund_code = eh.fund_code
                AND eh.portfolio_serno = p.serno
                AND a.member_serno = lds.member_serno
                AND eh.currency = 'TRY'
            GROUP BY 1, 2, 3
        )
        SELECT
            member_serno,
            month_end,
            fund_code,
            size
        FROM
            fund_sizes
        ORDER BY
            member_serno, month_end DESC, fund_code
    """

MONTHLY_OUTSTANDING_QUERY = """
        WITH last_day_sizes AS (
            SELECT
                CASE
                    WHEN DATE_TRUNC('month', CURRENT_DATE) = DATE_TRUNC('month', day)
                         AND ea.day = CURRENT_DATE THEN ea.day - INTERVAL '1 day'
                    ELSE DATE_TRUNC('month', ea.day) + INTERVAL '1 month' - INTERVAL '1 day'
                END AS month_end,
                a.member_serno,
                MAX(day) AS last_day_of_month
            FROM
                portfolio.accounts a, portfolio.eod_accounts ea
            WHERE
                ea.account_serno = a.serno
                AND a.member_serno IN %s
                AND ea.currency = 'TRY'
                AND ea.day >= %s
                AND ea.day <= %s
            GROUP BY 1, 2
        ),
        member_sizes AS (
            SELECT
                TO_CHAR(lds.month_end, 'YYYY/MM') AS month_end,
                lds.member_serno,
                ea.size,
                ea.movable_size
            FROM
                last_day_sizes lds, portfolio.accounts a, portfolio.eod_accounts ea
            WHERE
                lds.last_day_of_month = ea.day
                AND ea.account_serno = a.serno
                AND a.member_serno = lds.member_serno
                AND ea.currency = 'TRY'
        )
        SELECT
            member_serno,
            month_end,
            size,
            movable_size
        FROM
            member_sizes
        ORDER BY
            member_serno, month_end DESC
        """

DAILY_OUTSTANDING_QUERY = """
SELECT a.member_serno, ea."size", ea.movable_size, ea."day" FROM portfolio.accounts a, portfolio.eod_accounts ea
WHERE a.member_serno IN %s
AND ea.account_serno = a.serno
AND ea.currency = 'TRY'
AND ea.day >= %s
AND ea.day <= %s
ORDER BY a.member_serno, ea.day
    """

# Query to map actual_serno to random_serno
SERNO_MAPPING_QUERY = """
SELECT actual_serno, random_serno
FROM customer.serno_mapping
WHERE actual_serno IN %s
"""

MEMBER_DETAILS_QUERY = """
SELECT serno as member_serno, birth_year, occupation, education, interest_preference, registration_time, account_ready_time
FROM customer.members
WHERE serno IN %s
ORDER BY serno
"""

BIAS_RESULTS_QUERY = """
SELECT i.member_serno, r.result_key, r.result_value
FROM survey.results r, survey.instances i, survey.definitions d
WHERE i.member_serno IN %s
AND i.definition_serno = d.serno
AND d."name" = 'Investor-Profile-Survey'
AND i.completed is not null
AND r.instance_serno = i.serno
AND r.result_value <> 'false'
ORDER BY i.member_serno, r.result_key
"""

FUND_DETAILS_QUERY = """
SELECT 
    f.code,
    f.risk_value,
    f.category,
    f.asset_class
FROM 
    product.funds f
ORDER BY 
    f.code
"""


# Map of action types to their SQL queries with placeholders for WHERE conditions
ACTION_TYPE_QUERIES = {
    "İndirme": "SELECT serno as member_serno, creation_time as \"action_timestamp\" FROM customer.members WHERE serno IN %s ORDER BY serno",
    "Kaydolma": "SELECT serno as member_serno, registration_time as \"action_timestamp\" FROM customer.members WHERE serno IN %s ORDER BY serno",
    "Para Yatırma": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\" from portfolio.transactions t, portfolio.accounts a , customer.members m where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'CIN' and m.serno IN %s and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Para Çekme": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\" from portfolio.transactions t, portfolio.accounts a , customer.members m where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'COU' and m.serno IN %s and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Fon Alma": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\", t.fund_code from portfolio.transactions t, portfolio.accounts a , customer.members m where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'BUY' and m.serno IN %s and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Fon Satma": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\", t.fund_code from portfolio.transactions t, portfolio.accounts a , customer.members m where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'SEL' and m.serno IN %s and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Otonom Portföy Alma": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\", t.fund_code from portfolio.transactions t, portfolio.accounts a , customer.members m, product.funds f where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'BUY' and m.serno IN %s AND t.fund_code = f.code AND f.asset_class = 'Otonom Portföy' and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Otonom Portföy Satma": "select m.serno as member_serno, t.tx_time as \"action_timestamp\", t.amount as \"amount\", t.fund_code from portfolio.transactions t, portfolio.accounts a , customer.members m, product.funds f where t.account_serno = a.serno  and a.member_serno = m.serno AND t.tx_type = 'SEL' and m.serno IN %s AND t.fund_code = f.code AND f.asset_class = 'Otonom Portföy' and t.tx_time >= %s AND t.tx_time <= %s order by m.serno, t.serno",
    "Emir İptal Etme": "select m.serno as member_serno, fo.cancel_time as \"action_timestamp\", fo.amount as \"amount\", fo.fund_code from portfolio.portfolios p, portfolio.accounts a, portfolio.fund_orders fo, customer.members m where p.account_serno = a.serno and fo.portfolio_serno = p.serno and a.member_serno = m.serno and m.serno IN %s AND fo.status = 'C' AND fo.cancelled_by = 'U' ORDER BY m.serno, fo.serno",
    "Yeni Portföy Oluşturma": "WITH cte AS (SELECT p.*, a.member_serno, ROW_NUMBER() OVER (PARTITION BY a.member_serno ORDER BY p.serno) AS row_num FROM portfolio.portfolios p, portfolio.accounts a WHERE p.account_serno = a.serno AND a.member_serno IN %s AND p.TYPE = 'movable') SELECT member_serno, cte.create_date as \"action_timestamp\" FROM cte WHERE row_num > 1 ORDER BY member_serno",
    "Profil Resmi Ekli mi?": "SELECT serno as member_serno, current_timestamp as \"action_timestamp\" FROM customer.members WHERE serno IN %s and profile_picture is not null ORDER BY serno"
}

POPULAR_FUNDS_QUERY = """
SELECT fl.day, fl.fund_code
FROM product.fund_lists fl
WHERE fl.list_type = 'Popular'
AND fl.day >= %s
AND fl.day <= %s
ORDER BY fl.day, fl.fund_code
"""

RECORDED_IBANS_QUERY = """
SELECT member_serno, status, last_used_date
FROM portfolio.recorded_ibans
ORDER BY member_serno, last_used_date desc
"""

def format_timestamp(ts):
    """Format the timestamp to 'HH.mm-DD.MM.YY'.
    Handles None and pandas NaT values.
    """
    if ts is None or pd.isna(ts):
        return "N/A"
    return ts.strftime("%H.%M-%d.%m.%y")

def get_source_connection():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_source")
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
        print(f"SOURCE CONNECTION ERROR: {str(e)}")
        raise

def create_table_from_dataframe(df, table_name, conn, drop_table=True):
    """Create a table from a DataFrame using direct SQL instead of pandas to_sql.

    Args:
        df: DataFrame to upload
        table_name: Name of the table to create
        conn: Database connection
        drop_table: Whether to drop and recreate the table (default: True)
    """
    cur = conn.cursor()

    try:
        # Set a statement timeout to avoid hanging queries (3 minutes)
        cur.execute("SET statement_timeout = 180000")  # 3 minutes in milliseconds

        # Create table with schema if needed
        if drop_table:
            # Drop table if it exists with CASCADE to force drop even with dependencies
            drop_sql = f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{table_name}"
            print(f"Executing: {drop_sql}")

            # Try to drop the table with retries
            max_retries = 3
            retry_delay = 5  # seconds

            for attempt in range(max_retries):
                try:
                    cur.execute(drop_sql)
                    conn.commit()  # Commit the drop immediately to release locks
                    print(f"Successfully dropped table {TARGET_SCHEMA}.{table_name}")
                    break
                except Exception as e:
                    if "lock" in str(e).lower() and attempt < max_retries - 1:
                        print(f"Lock detected on {TARGET_SCHEMA}.{table_name}, retry {attempt+1}/{max_retries} in {retry_delay} seconds...")
                        conn.rollback()  # Rollback the failed transaction
                        import time
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to drop table {TARGET_SCHEMA}.{table_name}: {e}")
                        raise

            # Create table schema definitions
            column_defs = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                # Map pandas dtype to PostgreSQL type
                if 'int' in str(dtype):
                    pg_type = 'INTEGER'
                elif 'float' in str(dtype):
                    pg_type = 'NUMERIC'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                else:
                    pg_type = 'TEXT'

                # Properly quote column name if it contains spaces
                if ' ' in col_name:
                    quoted_name = f'"{col_name}"'
                else:
                    quoted_name = col_name

                column_defs.append(f"{quoted_name} {pg_type}")

            # Prepend the ID column definition
            all_column_defs = ["ID SERIAL PRIMARY KEY"] + column_defs

            # Create table
            create_sql = f"CREATE TABLE {TARGET_SCHEMA}.{table_name} ({', '.join(all_column_defs)})"
            print(f"Executing: {create_sql}")
            cur.execute(create_sql)
            conn.commit()  # Commit the create immediately

        # Insert data in bulk
        if not df.empty:
            # Prepare column names for INSERT
            columns = ', '.join(df.columns)

            # Create placeholders for the values
            placeholders = ', '.join(['%s'] * len(df.columns))

            # Prepare the SQL for insert
            insert_sql = f"INSERT INTO {TARGET_SCHEMA}.{table_name} ({columns}) VALUES ({placeholders})"

            # Process data in batches to avoid memory issues with very large datasets
            batch_size = 1000  # Reduced batch size for better performance
            total_rows = len(df)

            for i in range(0, total_rows, batch_size):
                # Get batch of rows
                batch_df = df.iloc[i:i+batch_size]

                # Convert batch to a list of tuples for executemany
                batch_values = []
                for _, row in batch_df.iterrows():
                    row_values = [None if pd.isna(x) else x for x in row]
                    batch_values.append(tuple(row_values))

                # Execute batch insert with retries
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        cur.executemany(insert_sql, batch_values)
                        # Commit after each batch to avoid holding locks for too long
                        conn.commit()
                        print(f"Inserted {min(i+batch_size, total_rows)}/{total_rows} rows into {TARGET_SCHEMA}.{table_name}")
                        break
                    except Exception as e:
                        if "lock" in str(e).lower() and attempt < max_retries - 1:
                            print(f"Lock detected during insert, retry {attempt+1}/{max_retries} in {retry_delay} seconds...")
                            conn.rollback()  # Rollback the failed transaction
                            import time
                            time.sleep(retry_delay)
                        else:
                            print(f"Failed to insert batch: {e}")
                            conn.rollback()
                            raise

        return len(df)

    except Exception as e:
        conn.rollback()
        print(f"Error in create_table_from_dataframe: {e}")
        raise
    finally:
        # Reset statement timeout
        try:
            cur.execute("RESET statement_timeout")
        except:
            pass
        cur.close()


def fetch_members(member_query):
    conn = get_source_connection()
    try:
        df1 = pd.read_sql(member_query, con=conn)  # Read query into DataFrame
        members = df1.iloc[:, 0].tolist()  # Convert the first column to a list
        return members
    finally:
        conn.close()

def map_actual_to_random_sernos(actual_sernos):
    """Map actual_sernos to random_sernos using the database."""
    serno_mapping = {}
    conn = None
    try:
        conn = get_source_connection()  # Use your connection function
        query = SERNO_MAPPING_QUERY  # SQL query
        df = pd.read_sql(query, con=conn, params=(tuple(actual_sernos),))  # Execute query and store in DataFrame

        # Convert DataFrame to dictionary
        serno_mapping = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))

    except Exception as e:
        print(f"Error mapping actual_sernos: {e}")
    finally:
        if conn:
            conn.close()

    return serno_mapping

def fetch_outstanding_fund_volumes(actual_sernos, start_date, end_date):
    """Fetch outstanding fund volumes for all members in a single query."""
    results = []
    conn = None
    try:
        conn = get_source_connection()  # Use your existing connection function

        print(f"Fetching outstanding fund volumes for {len(actual_sernos)} members...")

        # Read the query into a Pandas DataFrame
        df = pd.read_sql(
            MONTHLY_OUTSTANDING_FUND_QUERY,
            con=conn,
            params=(tuple(actual_sernos), start_date, end_date)
        )

        print(f"Retrieved {len(df)} outstanding fund volumes records")

        # Convert DataFrame to a list of tuples
        results = list(df.itertuples(index=False, name=None))

    except Exception as e:
        print(f"Error fetching outstanding fund volumes: {e}")
    finally:
        if conn:
            conn.close()

    return results

def fetch_outstanding_volumes(actual_sernos, start_date, end_date):
    """Fetch outstanding volumes for all members in a single query."""
    results = []
    conn = None
    try:
        conn = get_source_connection()  # Use your connection function

        print(f"Fetching outstanding volumes for {len(actual_sernos)} members...")

        # Execute the query and store results in a Pandas DataFrame
        df = pd.read_sql(
            MONTHLY_OUTSTANDING_QUERY,
            con=conn,
            params=(tuple(actual_sernos), start_date, end_date)
        )

        print(f"Retrieved {len(df)} outstanding volumes records")

        # Convert DataFrame to a list of tuples
        results = list(df.itertuples(index=False, name=None))

    except Exception as e:
        print(f"Error fetching outstanding volumes: {e}")
    finally:
        if conn:
            conn.close()

    return results


def fetch_digital_actions(actual_sernos, start_date, end_date):
    """Fetch digital actions for all members in a single query."""
    results = []
    conn = None
    try:
        conn = get_source_connection()  # Use your connection function

        print(f"Fetching digital actions for {len(actual_sernos)} members...")

        # Execute the query and store results in a Pandas DataFrame
        df = pd.read_sql(
            DIGITAL_ACTIONS_QUERY,
            con=conn,
            params=(tuple(actual_sernos), start_date, end_date)
        )

        print(f"Retrieved {len(df)} digital action records")

        # Convert DataFrame to a list of tuples
        results = list(df.itertuples(index=False, name=None))

    except Exception as e:
        print(f"Error fetching digital actions: {e}")
    finally:
        if conn:
            conn.close()

    return results


def fetch_fund_details():
    """Fetch fund details from the database."""
    conn = get_source_connection()
    try:
        df = pd.read_sql(FUND_DETAILS_QUERY, con=conn)
        print(f"Retrieved {len(df)} fund details records")
        return df
    except Exception as e:
        print(f"Error fetching fund details: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
    finally:
        conn.close()

def upload_fund_details(force_drop):
    """Fetch fund details and upload to target database."""
    print("Fetching fund details data...")
    df = fetch_fund_details()

    if not df.empty:
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (True)
        effective_drop_fund_details = force_drop or True
        rows_inserted = create_table_from_dataframe(df, 'fund_details', conn, drop_table=effective_drop_fund_details)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.fund_details'.")
    else:
        print("No fund details found.")

def fetch_daily_outstanding_volumes(actual_sernos, start_date, end_date):
    """Fetch daily outstanding volumes for all members in a single query."""
    results = []
    conn = None
    try:
        conn = get_source_connection()  # Use your connection function

        print(f"Fetching daily outstanding volumes for {len(actual_sernos)} members...")

        # Fetch data into a Pandas DataFrame
        df = pd.read_sql(
            DAILY_OUTSTANDING_QUERY,
            con=conn,
            params=(tuple(actual_sernos), start_date, end_date)
        )

        print(f"Retrieved {len(df)} daily outstanding volumes records")

        # Convert DataFrame to a list of tuples
        results = list(df.itertuples(index=False, name=None))

    except Exception as e:
        print(f"Error fetching daily outstanding volumes: {e}")
    finally:
        if conn:
            conn.close()

    return results

def fetch_action_data(member_sernos, start_date, end_date):
    """Fetch action data for all member serial numbers in a single query per action type."""
    results = []
    conn = None

    try:
        conn = get_source_connection()  # Use your connection function

        # Query data for each action type
        for action_type, query in ACTION_TYPE_QUERIES.items():
            print(f"Fetching data for action type: {action_type}")

            # Fetch data into a Pandas DataFrame
            if query.count("%s") > 1:  # Queries with date parameters
                df = pd.read_sql(query, con=conn, params=(tuple(member_sernos), start_date, end_date))
            else:
                df = pd.read_sql(query, con=conn, params=(tuple(member_sernos),))

            if df.empty:
                print(f"No rows found for action type: {action_type}")
                continue

            print(f"Retrieved {len(df)} records for action type: {action_type}")

            # Ensure required columns exist
            df = df.rename(columns=lambda x: x.lower())  # Normalize column names
            df["amount"] = df.get("amount", 0)  # Fill missing column with default
            df["fund_code"] = df.get("fund_code", "NA")  # Fill missing column

            # Convert DataFrame to a list of tuples
            results.extend(
                zip(
                    df["member_serno"],
                    [action_type] * len(df),  # Add action type for each row
                    df["action_timestamp"].apply(format_timestamp),
                    df["amount"],
                    df["fund_code"]
                )
            )

    except Exception as e:
        print(f"Error fetching action data: {e}")
    finally:
        if conn:
            conn.close()

    return results

def fetch_member_details(member_sernos):
    """Fetch member details for all members in a single query."""
    results = []
    conn = None

    try:
        conn = get_source_connection()  # Use your connection function

        print(f"Fetching member details for {len(member_sernos)} members...")

        # Fetch data into a Pandas DataFrame
        df = pd.read_sql(MEMBER_DETAILS_QUERY, con=conn, params=(tuple(member_sernos),))

        if df.empty:
            print("No records found.")
            return results

        print(f"Retrieved {len(df)} member details records")

        # Ensure required columns exist and process them
        df = df.rename(columns=lambda x: x.lower())  # Normalize column names
        # Add the results into a list of tuples
        results.extend(
            zip(
                df["member_serno"],
                df["birth_year"],
                df["occupation"],
                df["education"],
                df["interest_preference"],
                df["registration_time"].apply(format_timestamp),  # Format registration_time
                df["account_ready_time"].apply(format_timestamp)  # Format account_ready_time
            )
        )

    except Exception as e:
        print(f"Error fetching member details: {e}")
    finally:
        if conn:
            conn.close()

    return results

def fetch_bias_results(member_sernos):
    """Fetch bias results for all members in a single query."""
    results = []
    conn = None

    try:
        conn = get_source_connection()  # Use your connection function

        print(f"Fetching bias results for {len(member_sernos)} members...")

        # Fetch data into a Pandas DataFrame
        df = pd.read_sql(BIAS_RESULTS_QUERY, con=conn, params=(tuple(member_sernos),))

        if df.empty:
            print("No records found.")
            return results

        print(f"Retrieved {len(df)} bias results records")

        # Ensure required columns exist and process them
        df = df.rename(columns=lambda x: x.lower())  # Normalize column names

        # Add the results into a list of tuples
        results.extend(
            zip(
                df["member_serno"],
                df["result_key"],
                df["result_value"]
            )
        )

    except Exception as e:
        print(f"Error fetching bias results: {e}")
    finally:
        if conn:
            conn.close()

    return results

def fetch_fund_actions(member_sernos, start_date, end_date):
    """Fetch fund actions for all members in a single query."""
    fund_actions = []
    conn = None

    try:
        conn = get_source_connection()
        print(f"Executing FUND_ACTIONS_QUERY with parameters:")
        print(f"member_sernos: {member_sernos[:5]}... (total: {len(member_sernos)})")
        print(f"start_date: {start_date}")
        print(f"end_date: {end_date}")

        # Fetch data into a Pandas DataFrame
        df = pd.read_sql(FUND_ACTIONS_QUERY, con=conn, params=(tuple(member_sernos), start_date, end_date))

        print(f"Query returned {len(df)} rows")
        if df.empty:
            print("No records found in fund_actions query")
            return fund_actions

        # Normalize column names (optional but helps consistency)
        df = df.rename(columns=lambda x: x.lower())  # Convert column names to lowercase

        # Process the rows and append to the fund_actions list
        fund_actions.extend(
            zip(
                df["member_serno"],
                df["action_type"],
                df["fund_code"],
                df["client_time"].apply(format_timestamp)  # Format client_time
            )
        )

    except Exception as e:
        print(f"Error fetching fund actions: {e}")
    finally:
        if conn:
            conn.close()

    return fund_actions


def log_upload(table_name, row_count, log_file=row_tracking_path):
    today = str(date.today())
    
    # Eğer log dosyası varsa ve boş değilse yükle, yoksa boş dict başlat
    if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
        with open(log_file, "r") as f:
            log_data = json.load(f)
    else:
        log_data = {}

    # Bugünün girdisini güncelle
    if today not in log_data:
        log_data[today] = {}
    
    log_data[today][table_name] = row_count

    # Dosyaya tekrar yaz
    with open(log_file, "w") as f:
        json.dump(log_data, f, indent=4)


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

def get_end_date():
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            data = json.load(f)
            if "end_date" in data:
                try:
                    return datetime.strptime(data["end_date"], date_format).date()
                except ValueError:
                    print("Invalid end_date format in log file. Using current date.")
    return datetime.now().date()

def main():
    """Main script function."""

    # Read the start_date from the JSON
    log = {}
    try:
        with open(log_file, "r") as f:
            log = json.load(f)
        start_date_str = log.get(latest_day_key)
        if start_date_str is None:
            raise ValueError(f"No date found for {latest_day_key}")
        start_date = datetime.strptime(start_date_str, date_format)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error reading start/log file: {e}")
        # Fallback date if key not found or file error
        start_date = datetime(2000, 1, 1)

    # Determine if this is an initial run (force drop tables)
    force_drop = log.get("initial_run", True)
    if force_drop:
        print("Initial run detected (or log file missing/incomplete). Forcing table drops.")

    start_date_str = start_date.strftime(date_format)
    #end_date_str = datetime.now().strftime(date_format)  # 
    end_date_str = get_end_date().strftime(date_format)

    print("Start date:", start_date_str)
    print("End date:", end_date_str)

     # Load actual serial numbers from the file
    print("Fetching members...")
    member_sernos = fetch_members(ALL_MEMBER_QUERY)
    if not member_sernos:
        print("No members found. Exiting.")
        return

    print("Mapping actual_sernos to random_sernos...")
    serno_mapping = map_actual_to_random_sernos(member_sernos)
    if not serno_mapping:
        print("No mappings found. Exiting.")
        return

    print("Fetching fund actions data...")
    action_data = fetch_fund_actions(member_sernos, start_date_str, end_date_str)
    if action_data:
        # Create DataFrame from action_data with English column names
        df = pd.DataFrame(action_data, columns=["customer_id", "action_type", "fund_code", "timestamp"])

        # Apply the serno_mapping if needed (for example, mapping customer_id)
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])

        # Create a connection to the database
        conn = get_target_connection()  # Ensure to use your connection function

        # Upload DataFrame to the SQL table using direct SQL
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_fund_actions = force_drop or True
        rows_inserted = create_table_from_dataframe(df, 'fund_actions', conn, drop_table=effective_drop_fund_actions)

        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.fund_actions'.")
        
        # log the date and number of rows inserted to json file
        log_upload("fund_actions", rows_inserted)
        log_upload_to_db("fund_actions", rows_inserted, conn)

    else:
       print("No fund action data found.")

    # Fetch and write action data
    print("Fetching action data...")
    action_data = fetch_action_data(member_sernos, start_date_str, end_date_str)
    if action_data:
        df = pd.DataFrame(action_data, columns=["customer_id", "action_type", "timestamp", "amount", "fund_code"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_actions = force_drop or False
        rows_inserted = create_table_from_dataframe(df, 'actions', conn, drop_table=effective_drop_actions)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.actions'.")
        log_upload("actions", rows_inserted)
        log_upload_to_db("actions", rows_inserted, conn)
    else:
        print("No action data found.")

    # Fetch and write action data
    print("Fetching monthly outstanding data...")
    outstanding_data = fetch_outstanding_volumes(member_sernos, start_date_str, end_date_str)
    if outstanding_data:
        df = pd.DataFrame(outstanding_data, columns=["customer_id", "year_month", "account_volume", "fund_volume"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_monthly_outstandings = force_drop or False
        rows_inserted = create_table_from_dataframe(df, 'monthly_outstandings', conn, drop_table=effective_drop_monthly_outstandings)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.monthly_outstandings'.")
        log_upload("monthly_outstandings", rows_inserted)
        log_upload_to_db("monthly_outstandings", rows_inserted, conn)
    else:
        print("No outstanding data found.")

    # Fetch and write daily outstanding data
    print("Fetching daily outstanding data...")
    daily_outstanding_data = fetch_daily_outstanding_volumes(member_sernos, start_date_str, end_date_str)
    if daily_outstanding_data:
        df = pd.DataFrame(daily_outstanding_data, columns=["customer_id", "date", "account_volume", "fund_volume"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_daily_outstandings = force_drop or False
        rows_inserted = create_table_from_dataframe(df, 'daily_outstandings', conn, drop_table=effective_drop_daily_outstandings)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.daily_outstandings'.")
        log_upload("daily_outstandings", rows_inserted)
        log_upload_to_db("daily_outstandings", rows_inserted, conn)
    else:
        print("No outstanding data found.")


    # Fetch and write monthly outstanding fund data
    print("Fetching monthly outstanding fund data...")
    outstanding_fund_data = fetch_outstanding_fund_volumes(member_sernos, start_date_str, end_date_str)
    if outstanding_fund_data:
        df = pd.DataFrame(outstanding_fund_data, columns=["customer_id", "year_month", "fund_code", "size"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_monthly_outstanding_funds = force_drop or False
        rows_inserted = create_table_from_dataframe(df, 'monthly_outstanding_funds', conn, drop_table=effective_drop_monthly_outstanding_funds)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.monthly_outstanding_funds'.")
        log_upload("monthly_outstanding_funds", rows_inserted)
        log_upload_to_db("monthly_outstanding_funds", rows_inserted, conn)
    else:
        print("No outstanding data found.")

    # Fetch and write member details
    print("Fetching member details...")
    member_details = fetch_member_details(member_sernos)
    if member_details:
        df = pd.DataFrame(member_details, columns=["customer_id", "birth_year", "occupation", "education", "interest_preference", "registration_time", "account_ready_time"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (True)
        effective_drop_member_details = force_drop or True
        rows_inserted = create_table_from_dataframe(df, 'member_details', conn, drop_table=effective_drop_member_details)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.member_details'.")
        log_upload("member_details", rows_inserted)
        log_upload_to_db("member_details", rows_inserted, conn)
    else:
        print("No member details found.")

    # Fetch and write bias results
    print("Fetching bias results...")
    bias_results = fetch_bias_results(member_sernos)
    if bias_results:
        df = pd.DataFrame(bias_results, columns=["customer_id", "result_key", "result_value"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (True)
        effective_drop_bias_results = force_drop or True
        rows_inserted = create_table_from_dataframe(df, 'bias_results', conn, drop_table=effective_drop_bias_results)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.bias_results'.")
        log_upload("bias_results", rows_inserted)
        log_upload_to_db("bias_results", rows_inserted, conn)
    else:
        print("No bias results found.")


    print("Fetching and uploading fund details data...")
    # Pass force_drop to the function
    upload_fund_details(force_drop)


    # Fetch and write digital actions
    print("Fetching digital actions...")
    digital_actions = fetch_digital_actions(member_sernos, start_date_str, end_date_str)
    if digital_actions:
        df = pd.DataFrame(digital_actions, columns=["customer_id", "view_name", "action", "details", "event_time"])
        df["customer_id"] = df["customer_id"].map(serno_mapping).fillna(df["customer_id"])
        conn = get_target_connection()
        # Calculate effective drop based on force_drop and specific setting (False)
        effective_drop_digital_actions = force_drop or False
        rows_inserted = create_table_from_dataframe(df, 'digital_actions', conn, drop_table=effective_drop_digital_actions)
        print(f"Successfully uploaded {rows_inserted} records to SQL table 'dws.digital_actions'.")
        log_upload("digital_actions", rows_inserted)
        log_upload_to_db("digital_actions", rows_inserted, conn)
    else:
        print("No member details found.")

    # At the end, update the file with today's date in "YYYY-MM-DD" format
    log[latest_day_key] = datetime.now().strftime(date_format)
    # Set initial_run to False after successful run
    log["initial_run"] = False

    with open(log_file, "w") as f:
        json.dump(log, f, indent=2)


default_args = {
    "owner": "kerem",
    "start_date": datetime(2024, 3, 19),
    "retries": 1,
}

dag = DAG(
    "data_setup_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

transfer_task = PythonOperator(
    task_id="setup_data",
    python_callable=main,
    dag=dag,
)

# Trigger the second DAG after the first one finishes
trigger_dag2 = TriggerDagRunOperator(
    task_id="trigger_dag2",
    trigger_dag_id="data_transfer_pipeline",  # The second DAG's ID
    conf={},
    dag=dag,
)

transfer_task >> trigger_dag2
