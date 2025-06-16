
-- addBankAccount

ALTER TABLE dws.year_month_based
ADD COLUMN addBankAccount INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET addBankAccount = 1
FROM (
    SELECT DISTINCT customer_id,
                    EXTRACT(YEAR FROM event_time)::INT AS year,
                    EXTRACT(MONTH FROM event_time)::INT AS month
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%addBankAccount%'
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- settings

ALTER TABLE dws.year_month_based
ADD COLUMN settings INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET settings = da.settings_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS settings_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%settings%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- bes

ALTER TABLE dws.year_month_based
ADD COLUMN bes INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET bes = 1
FROM (
    SELECT DISTINCT customer_id,
                    EXTRACT(YEAR FROM event_time)::INT AS year,
                    EXTRACT(MONTH FROM event_time)::INT AS month
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%besSuggestion%'
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- otonom

ALTER TABLE dws.year_month_based
ADD COLUMN otonom INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET otonom = 1
FROM (
    SELECT DISTINCT customer_id,
                    EXTRACT(YEAR FROM event_time)::INT AS year,
                    EXTRACT(MONTH FROM event_time)::INT AS month
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%investmentStrategies%'
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- pushNotification

ALTER TABLE dws.year_month_based
ADD COLUMN pushNotification INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET pushNotification = da.pushNotification_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS pushNotification_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%pushNotification%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- compare

ALTER TABLE dws.year_month_based
ADD COLUMN compare INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET compare = da.compare_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS compare_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%manageComparisonItems%'
    and details ILIKE '%apply%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;

-- filterselection

ALTER TABLE dws.year_month_based
ADD COLUMN filterselection INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET filterselection = da.filterselection_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS filterselection_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%filterSelection%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- sorting

ALTER TABLE dws.year_month_based
ADD COLUMN sorting INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET sorting = da.sorting_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS sorting_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%fundsList%'
    and details ILIKE '%ChangeSortDirection%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- otonom portfoyden sonra 1 hafta icinde 2 den fazla fon alanlar

ALTER TABLE dws.year_month_based
ADD COLUMN otonombuy INTEGER DEFAULT 0;

WITH all_strategy_events AS (
    SELECT customer_id,
           event_time AS strategy_time
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%investmentStrategies%'
),
fon_alma_counts AS (
    SELECT s.customer_id,
           s.strategy_time,
           COUNT(*) AS fon_alma_count
    FROM all_strategy_events s
    JOIN dws.actions a 
      ON a.customer_id = s.customer_id
     AND to_timestamp(a."timestamp", 'HH24.MI-DD.MM.YY') BETWEEN s.strategy_time AND s.strategy_time + INTERVAL '7 days'
    WHERE a.action_type::TEXT ILIKE '%Fon Alma%'
    GROUP BY s.customer_id, s.strategy_time
),
qualified_events AS (
    SELECT customer_id,
           EXTRACT(YEAR FROM strategy_time)::INT AS year,
           EXTRACT(MONTH FROM strategy_time)::INT AS month
    FROM fon_alma_counts
    WHERE fon_alma_count > 1
)
UPDATE dws.year_month_based ymb
SET otonombuy = 1
FROM qualified_events q
WHERE ymb."CUST_ID" = q.customer_id
  AND ymb."YEAR" = q.year
  AND ymb."MONTH" = q.month;


-- transactionsHistory

ALTER TABLE dws.year_month_based
ADD COLUMN transactionsHistory INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET transactionsHistory = da.transactionsHistory_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS transactionsHistory_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%transactionsHistory%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- generalSummary

ALTER TABLE dws.year_month_based
ADD COLUMN generalsummary INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET generalsummary = da.generalsummary_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS generalsummary_count
    FROM dws.digital_actions
    WHERE details ILIKE '%generalSummary%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- background

ALTER TABLE dws.year_month_based
ADD COLUMN background INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET background = da.background_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS background_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%background%'
    and action ilike '%open%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- foreground

ALTER TABLE dws.year_month_based
ADD COLUMN foreground INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET foreground = da.foreground_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS foreground_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%foreground%'
    and action ilike '%open%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- canceltransaction

ALTER TABLE dws.year_month_based
ADD COLUMN canceltransaction INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET canceltransaction = da.canceltransaction_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS canceltransaction_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%account%'
    and details ilike '%canceltransaction%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- grihelpcenter

ALTER TABLE dws.year_month_based
ADD COLUMN grihelpcenter INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET grihelpcenter = da.grihelpcenter_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS grihelpcenter_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%account%'
    and details ilike '%grihelpcenter%'
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- signup_night

ALTER TABLE dws.year_month_based
ADD COLUMN signup_night INTEGER DEFAULT 0;


UPDATE dws.year_month_based ymb
SET signup_night = da.signup_night_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS signup_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%bottomPopup%'
      AND details ILIKE '%signUp%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- signin_night

ALTER TABLE dws.year_month_based
ADD COLUMN signin_night INTEGER DEFAULT 0;


UPDATE dws.year_month_based ymb
SET signin_night = da.signin_night_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS signin_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%bottomPopup%'
      AND details ILIKE '%signin%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;


-- order_night

ALTER TABLE dws.year_month_based
ADD COLUMN order_night INTEGER DEFAULT 0;


UPDATE dws.year_month_based ymb
SET order_night = da.order_night_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS order_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%orderDetail%'
      AND action ILIKE '%close%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- addFavourite 

ALTER TABLE dws.year_month_based
ADD COLUMN addfavourite INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET addfavourite = da.addfavourite_count
FROM (
    SELECT customer_id,
           EXTRACT(YEAR FROM event_time)::INT AS year,
           EXTRACT(MONTH FROM event_time)::INT AS month,
           COUNT(*) AS addfavourite_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%fundDetails%'
      AND details ILIKE '%addfavourite%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id, year, month
) da
WHERE ymb."CUST_ID" = da.customer_id
  AND ymb."YEAR" = da.year
  AND ymb."MONTH" = da.month;



-- age
ALTER TABLE dws.year_month_based
ADD COLUMN age_28_48 INTEGER DEFAULT 0;

UPDATE dws.year_month_based ymb
SET age_28_48 = 1
FROM dws.member_details md
WHERE ymb."CUST_ID" = md.customer_id
  AND (EXTRACT(YEAR FROM CURRENT_DATE) - md.birth_year) BETWEEN 28 AND 48;














