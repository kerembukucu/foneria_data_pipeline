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
        EXTRACT(YEAR FROM timestamp::timestamp) AS year,
        EXTRACT(MONTH FROM timestamp::timestamp) AS month,
        SUM(CASE 
                WHEN action_type IN ('Fon Alma', 'Fon Satma') THEN amount 
                ELSE 0 
            END) AS transaction_volume,
        COUNT(*) AS activity_score
    FROM dws.actions
    WHERE timestamp IS NOT NULL 
      AND timestamp != 'N/A'
      AND timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
    GROUP BY customer_id, year, month
),
fund_actions AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM timestamp::timestamp) AS year,
        EXTRACT(MONTH FROM timestamp::timestamp) AS month,
        COUNT(DISTINCT fund_code) AS interested_fund_count
    FROM dws.fund_actions
    WHERE timestamp IS NOT NULL 
      AND timestamp != 'N/A'
      AND timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
    GROUP BY customer_id, year, month
),
avg_outstanding AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM date::date) AS year,
        EXTRACT(MONTH FROM date::date) AS month,
        AVG(account_volume) AS average_outstanding
    FROM dws.daily_outstandings
    WHERE date IS NOT NULL
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
    WHERE date IS NOT NULL
    GROUP BY customer_id, year, month
),
fund_action_risk AS (
    SELECT
        fa.customer_id AS cust_id,
        EXTRACT(YEAR FROM fa.timestamp::timestamp) AS year,
        EXTRACT(MONTH FROM fa.timestamp::timestamp) AS month,
        AVG(CAST(fd.risk_value AS NUMERIC)) AS interested_fund_risk
    FROM dws.fund_actions fa
    JOIN dws.fund_details fd ON fa.fund_code = fd.code
    WHERE fa.action_type = 'open'
      AND fa.timestamp IS NOT NULL 
      AND fa.timestamp != 'N/A'
      AND fa.timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
    GROUP BY fa.customer_id, year, month
),
owned_funds AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM TO_DATE(year_month, 'YYYY/MM')) AS year,
        EXTRACT(MONTH FROM TO_DATE(year_month, 'YYYY/MM')) AS month,
        COUNT(DISTINCT fund_code) AS owned_fund_count
    FROM dws.monthly_outstanding_funds
    WHERE year_month IS NOT NULL
      AND year_month != 'N/A'
      AND year_month ~ '^[0-9]{4}/[0-9]{2}$'
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
    WHERE mof.year_month IS NOT NULL
      AND mof.year_month != 'N/A'
      AND mof.year_month ~ '^[0-9]{4}/[0-9]{2}$'
    GROUP BY mof.customer_id, year, month
),
hourly_activity AS (
    SELECT
        customer_id AS cust_id,
        EXTRACT(YEAR FROM timestamp::timestamp) AS year,
        EXTRACT(MONTH FROM timestamp::timestamp) AS month,
        COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp::timestamp) BETWEEN 5 AND 10) AS early_morning,
        COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp::timestamp) BETWEEN 11 AND 16) AS work_time,
        COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp::timestamp) BETWEEN 17 AND 22) AS evening,
        COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM timestamp::timestamp) BETWEEN 0 AND 4 OR EXTRACT(HOUR FROM timestamp::timestamp) > 22) AS night
    FROM dws.actions
    WHERE timestamp IS NOT NULL 
      AND timestamp != 'N/A'
      AND timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
    GROUP BY customer_id, year, month
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
    ROUND(100.0 * ha.early_morning / NULLIF((ha.early_morning + ha.work_time + ha.evening + ha.night), 0), 2) AS early_morning_activity_percentage,
    ROUND(100.0 * ha.work_time / NULLIF((ha.early_morning + ha.work_time + ha.evening + ha.night), 0), 2) AS work_time_activity_percentage,
    ROUND(100.0 * ha.evening / NULLIF((ha.early_morning + ha.work_time + ha.evening + ha.night), 0), 2) AS evening_activity_percentage,
    ROUND(100.0 * ha.night / NULLIF((ha.early_morning + ha.work_time + ha.evening + ha.night), 0), 2) AS night_activity_percentage
FROM monthly m
LEFT JOIN fund_actions f 
  ON m.cust_id::text = f.cust_id::text 
 AND m.year::int = f.year::int 
 AND m.month::int = f.month::int

LEFT JOIN actions a 
  ON m.cust_id::text = a.cust_id::text 
 AND m.year::int = a.year::int 
 AND m.month::int = a.month::int

LEFT JOIN avg_outstanding ao 
  ON m.cust_id::text = ao.cust_id::text 
 AND m.year::int = ao.year::int 
 AND m.month::int = ao.month::int

LEFT JOIN outstanding_trend_calc ot 
  ON m.cust_id::text = ot.cust_id::text 
 AND m.year::int = ot.year::int 
 AND m.month::int = ot.month::int

LEFT JOIN fund_action_risk far 
  ON m.cust_id::text = far.cust_id::text 
 AND m.year::int = far.year::int 
 AND m.month::int = far.month::int

LEFT JOIN owned_funds ofc 
  ON m.cust_id::text = ofc.cust_id::text 
 AND m.year::int = ofc.year::int 
 AND m.month::int = ofc.month::int

LEFT JOIN owned_fund_risk_calc ofr 
  ON m.cust_id::text = ofr.cust_id::text 
 AND m.year::int = ofr.year::int 
 AND m.month::int = ofr.month::int

LEFT JOIN hourly_activity ha 
  ON m.cust_id::text = ha.cust_id::text 
 AND m.year::int = ha.year::int 
 AND m.month::int = ha.month::int;
