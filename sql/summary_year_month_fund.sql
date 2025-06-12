DROP TABLE IF EXISTS dws.summary_year_month_fund_based;

CREATE TABLE dws.summary_year_month_fund_based AS
WITH fund_clicks AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS month,
        fund_code,
        COUNT(*) AS fund_clicks
    FROM dws.fund_actions
    WHERE timestamp ~ '^[0-9]{2}.[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}$'
    GROUP BY customer_id, fund_code, year, month
),
fund_transaction_volume AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS month,
        fund_code,
        SUM(amount) AS fund_transaction_volume
    FROM dws.actions
    WHERE fund_code IS NOT NULL
      AND timestamp ~ '^[0-9]{2}.[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}$'
    GROUP BY customer_id, fund_code, year, month
),
fund_outstanding AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_DATE(year_month, 'YYYY/MM')) AS year,
        EXTRACT(MONTH FROM TO_DATE(year_month, 'YYYY/MM')) AS month,
        fund_code,
        SUM(size) AS fund_outstanding
    FROM dws.monthly_outstanding_funds
    WHERE year_month ~ '^[0-9]{4}/[0-9]{2}$'
    GROUP BY customer_id, fund_code, year, month
)
SELECT
    COALESCE(fc.cust_id, ftv.cust_id, fo.cust_id) AS cust_id,
    COALESCE(fc.year, ftv.year, fo.year) AS year,
    COALESCE(fc.month, ftv.month, fo.month) AS month,
    COALESCE(fc.fund_code, ftv.fund_code, fo.fund_code) AS fund_id,
    fc.fund_clicks,
    fo.fund_outstanding,
    ftv.fund_transaction_volume
FROM fund_clicks fc
FULL OUTER JOIN fund_transaction_volume ftv
    ON fc.cust_id = ftv.cust_id
    AND fc.year = ftv.year
    AND fc.month = ftv.month
    AND fc.fund_code = ftv.fund_code
FULL OUTER JOIN fund_outstanding fo
    ON COALESCE(fc.cust_id, ftv.cust_id) = fo.cust_id
    AND COALESCE(fc.year, ftv.year) = fo.year
    AND COALESCE(fc.month, ftv.month) = fo.month
    AND COALESCE(fc.fund_code, ftv.fund_code) = fo.fund_code;
