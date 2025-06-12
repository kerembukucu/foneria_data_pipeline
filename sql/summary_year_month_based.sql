DROP TABLE IF EXISTS dws.summary_year_month;

CREATE TABLE dws.summary_year_month AS
WITH monthly AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_DATE(year_month, 'YYYY/MM')) AS year,
        EXTRACT(MONTH FROM TO_DATE(year_month, 'YYYY/MM')) AS month,
        SUM(fund_volume) AS fund_volume,
        SUM(account_volume) AS account_volume
    FROM dws.monthly_outstandings
    WHERE year_month ~ '^[0-9]{4}/[0-9]{2}$'
    GROUP BY customer_id, year_month
),
actions AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS month,
        SUM(amount) AS transaction_volume,
        COUNT(*) AS activity_score
    FROM dws.actions
    WHERE timestamp ~ '^[0-9]{2}.[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}$'
    GROUP BY customer_id, year, month
),
fund_actions AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(timestamp, 'HH24.MI-DD.MM.YY')) AS month,
        COUNT(DISTINCT fund_code) AS interested_fund_count
    FROM dws.fund_actions
    WHERE timestamp ~ '^[0-9]{2}.[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}$'
    GROUP BY customer_id, year, month
),
daily_clicks AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        COUNT(*) AS daily_click_count
    FROM dws.daily_outstandings
    WHERE date::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    GROUP BY customer_id, year, month
),
avg_outstanding AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        AVG(account_volume) AS average_outstanding
    FROM dws.daily_outstandings
    WHERE date::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    GROUP BY customer_id, year, month
),
outstanding_trend_calc AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        CASE 
            WHEN (MAX(account_volume) - MIN(account_volume)) > 0 THEN 1
            WHEN (MAX(account_volume) - MIN(account_volume)) < 0 THEN -1
            ELSE 0
        END AS outstanding_trend
    FROM dws.daily_outstandings
    WHERE date::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    GROUP BY customer_id, year, month
),
fund_action_risk AS (
    SELECT
        fa.customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_TIMESTAMP(fa.timestamp, 'HH24.MI-DD.MM.YY')) AS year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(fa.timestamp, 'HH24.MI-DD.MM.YY')) AS month,
        AVG(CAST(fd.risk_value AS NUMERIC)) AS interested_fund_risk
    FROM dws.fund_actions fa
    JOIN dws.fund_details fd ON fa.fund_code = fd.code
    WHERE fa.timestamp ~ '^[0-9]{2}.[0-9]{2}-[0-9]{2}.[0-9]{2}.[0-9]{2}$'
      AND fa.action_type = 'open'
    GROUP BY fa.customer_id, year, month
),
owned_funds AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_DATE(year_month, 'YYYY/MM')) AS year,
        EXTRACT(MONTH FROM TO_DATE(year_month, 'YYYY/MM')) AS month,
        COUNT(DISTINCT fund_code) AS owned_fund_count
    FROM dws.monthly_outstanding_funds
    GROUP BY customer_id, year, month
),
owned_fund_risk_calc AS (
    SELECT
        mof.customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_DATE(mof.year_month, 'YYYY/MM')) AS year,
        EXTRACT(MONTH FROM TO_DATE(mof.year_month, 'YYYY/MM')) AS month,
        SUM(CAST(fd.risk_value AS NUMERIC) * mof.size)::NUMERIC / NULLIF(SUM(mof.size), 0) AS owned_fund_risk
    FROM dws.monthly_outstanding_funds mof
    JOIN dws.fund_details fd ON mof.fund_code = fd.code
    GROUP BY mof.customer_id, year, month
)
SELECT
    m.cust_id,
    m.year,
    m.month,
    m.fund_volume / NULLIF(m.account_volume, 0) AS fund_balance_ratio,
    ao.average_outstanding,
    ot.outstanding_trend,
    f.interested_fund_count,
    far.interested_fund_risk,
    a.transaction_volume,
    ofc.owned_fund_count,
    ofr.owned_fund_risk,
    a.activity_score,
    d.daily_click_count
FROM monthly m
LEFT JOIN fund_actions f ON m.cust_id = f.cust_id AND m.year = f.year AND m.month = f.month
LEFT JOIN actions a ON m.cust_id = a.cust_id AND m.year = a.year AND m.month = a.month
LEFT JOIN daily_clicks d ON m.cust_id = d.cust_id AND m.year = d.year AND m.month = d.month
LEFT JOIN avg_outstanding ao ON m.cust_id = ao.cust_id AND m.year = ao.year AND m.month = ao.month
LEFT JOIN outstanding_trend_calc ot ON m.cust_id = ot.cust_id AND m.year = ot.year AND m.month = ot.month
LEFT JOIN fund_action_risk far ON m.cust_id = far.cust_id AND m.year = far.year AND m.month = far.month
LEFT JOIN owned_funds ofc ON m.cust_id = ofc.cust_id AND m.year = ofc.year AND m.month = ofc.month
LEFT JOIN owned_fund_risk_calc ofr ON m.cust_id = ofr.cust_id AND m.year = ofr.year AND m.month = ofr.month;
