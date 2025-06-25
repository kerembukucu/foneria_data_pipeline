-- dya_score
UPDATE dws.arketip a
SET dya_score = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.bias_results
    WHERE result_key = 'BIT'
) b
WHERE a.customer_id = b.customer_id;

-- has_account_ready
UPDATE dws.arketip a
SET has_account_ready = 1
FROM (
    SELECT customer_id::text AS customer_id
    FROM dws.member_details
    WHERE account_ready_time IS NOT NULL
) md
WHERE a.customer_id = md.customer_id;

-- addBankAccount
UPDATE dws.arketip a
SET addBankAccount = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%addBankAccount%'
) da
WHERE a.customer_id = da.customer_id;

-- settings
UPDATE dws.arketip a
SET settings = da.settings_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS settings_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%settings%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- bes
UPDATE dws.arketip a
SET bes = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%besSuggestion%'
) da
WHERE a.customer_id = da.customer_id;

-- otonom
UPDATE dws.arketip a
SET otonom = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.digital_actions
    WHERE details::TEXT ILIKE '%investmentStrategies%'
) da
WHERE a.customer_id = da.customer_id;

-- pushNotification
UPDATE dws.arketip a
SET pushNotification = da.pushNotification_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS pushNotification_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%pushNotification%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- compare
UPDATE dws.arketip a
SET compare = da.compare_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS compare_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%manageComparisonItems%'
      AND details ILIKE '%apply%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- filterselection
UPDATE dws.arketip a
SET filterselection = da.filterselection_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS filterselection_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%filterSelection%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- sorting
UPDATE dws.arketip a
SET sorting = da.sorting_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS sorting_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%fundsList%'
      AND details ILIKE '%ChangeSortDirection%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- otonombuy (customers with >1 "Fon Alma" in 1 week after investmentStrategies)
UPDATE dws.arketip a
SET otonombuy = 1
FROM (
    WITH all_strategy_events AS (
        SELECT customer_id::text AS customer_id, event_time AS strategy_time
        FROM dws.digital_actions
        WHERE details::TEXT ILIKE '%investmentStrategies%'
    ),
    fon_alma_counts AS (
        SELECT s.customer_id, COUNT(*) AS fon_alma_count
        FROM all_strategy_events s
        JOIN dws.actions act
          ON act.customer_id::text = s.customer_id
         AND to_timestamp(act."timestamp", 'HH24.MI-DD.MM.YY') BETWEEN s.strategy_time AND s.strategy_time + INTERVAL '7 days'
        WHERE act.action_type::TEXT ILIKE '%Fon Alma%'
        GROUP BY s.customer_id
    )
    SELECT customer_id
    FROM fon_alma_counts
    WHERE fon_alma_count > 1
) da
WHERE a.customer_id = da.customer_id;

-- transactionsHistory
UPDATE dws.arketip a
SET transactionsHistory = da.transactionsHistory_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS transactionsHistory_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%transactionsHistory%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- generalsummary
UPDATE dws.arketip a
SET generalsummary = da.generalsummary_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS generalsummary_count
    FROM dws.digital_actions
    WHERE details ILIKE '%generalSummary%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- background
UPDATE dws.arketip a
SET background = da.background_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS background_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%background%'
      AND action ILIKE '%open%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- foreground
UPDATE dws.arketip a
SET foreground = da.foreground_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS foreground_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%foreground%'
      AND action ILIKE '%open%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- canceltransaction
UPDATE dws.arketip a
SET canceltransaction = da.canceltransaction_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS canceltransaction_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%account%'
      AND details ILIKE '%canceltransaction%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- grihelpcenter
UPDATE dws.arketip a
SET grihelpcenter = da.grihelpcenter_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS grihelpcenter_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%account%'
      AND details ILIKE '%grihelpcenter%'
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- signup_night
UPDATE dws.arketip a
SET signup_night = da.signup_night_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS signup_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%bottomPopup%'
      AND details ILIKE '%signUp%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- signin_night
UPDATE dws.arketip a
SET signin_night = da.signin_night_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS signin_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%bottomPopup%'
      AND details ILIKE '%signin%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- order_night
UPDATE dws.arketip a
SET order_night = da.order_night_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS order_night_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%orderDetail%'
      AND action ILIKE '%close%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- addfavourite
UPDATE dws.arketip a
SET addfavourite = da.addfavourite_count
FROM (
    SELECT customer_id::text AS customer_id, COUNT(*) AS addfavourite_count
    FROM dws.digital_actions
    WHERE view_name ILIKE '%fundDetails%'
      AND details ILIKE '%addfavourite%'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
    GROUP BY customer_id
) da
WHERE a.customer_id = da.customer_id;

-- age_28_48
UPDATE dws.arketip a
SET age_28_48 = 1
FROM dws.member_details md
WHERE a.customer_id = md.customer_id::text
  AND (EXTRACT(YEAR FROM CURRENT_DATE) - md.birth_year) BETWEEN 28 AND 48;


-- katilim_yatirim
UPDATE dws.arketip a
SET katilim_yatirim = 1
FROM (
    SELECT
        m.customer_id::text AS customer_id,
        -- Katılım fonlarının toplamı
        SUM(CASE WHEN f.code IS NOT NULL THEN m.size ELSE 0 END) AS katilim_fon_toplam,
        -- Tüm fonların toplamı
        SUM(m.size) AS toplam_fon,
        -- Oran
        CASE
            WHEN SUM(m.size) > 0
            THEN SUM(CASE WHEN f.code IS NOT NULL THEN m.size ELSE 0 END) / SUM(m.size)::float
            ELSE 0
        END AS oran
    FROM dws.monthly_outstanding_funds m
    LEFT JOIN (
        SELECT DISTINCT code
        FROM dws.fund_details
        WHERE asset_class = 'Katılım'
    ) f ON m.fund_code = f.code
    GROUP BY m.customer_id
) t
WHERE a.customer_id = t.customer_id
  AND t.oran > 0.8;

--faiz_okey
UPDATE dws.arketip a
SET faiz_okey = 1
FROM (
    SELECT customer_id::text AS customer_id
    FROM dws.member_details
    WHERE interest_preference = 'Y'
) md
WHERE a.customer_id = md.customer_id;

--video_izleyen
UPDATE dws.arketip a
SET video_izleyen = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.digital_actions
    WHERE view_name = 'feeds'
      AND details = 'close'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
) d
WHERE a.customer_id = d.customer_id;

--dya_gece
UPDATE dws.arketip a
SET dya_gece = 1
FROM (
    SELECT DISTINCT customer_id::text AS customer_id
    FROM dws.digital_actions
    WHERE view_name = 'profiling'
      AND details = 'startAnalysis'
      AND (EXTRACT(HOUR FROM event_time) >= 22 OR EXTRACT(HOUR FROM event_time) < 6)
) d
WHERE a.customer_id = d.customer_id;


-- katilim_ilgi
UPDATE dws.arketip a
SET katilim_ilgi = 1
FROM (
    SELECT
        fa.customer_id::text AS customer_id,
        SUM(CASE WHEN fd.code IS NOT NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS oran
    FROM dws.actions fa
    LEFT JOIN (
        SELECT DISTINCT code
        FROM dws.fund_details
        WHERE asset_class = 'Katılım'
    ) fd ON fa.fund_code = fd.code
    WHERE fa.action_type = 'open'
    GROUP BY fa.customer_id
    HAVING SUM(CASE WHEN fd.code IS NOT NULL THEN 1 ELSE 0 END)::float / COUNT(*) > 0.8
) t
WHERE a.customer_id = t.customer_id;

-- popular_fund_ratio
UPDATE dws.arketip a
SET popular_fund_ratio = ratios.ratio
FROM (
    SELECT
        customer_id,
        COALESCE(
            CAST(
                COUNT(*) FILTER (
                    WHERE action_type = 'Fon Alma'
                      AND fund_code IS NOT NULL
                      AND fund_code IN (SELECT fund_code FROM dws.popular_funds)
                ) AS FLOAT
            ) / NULLIF(
                COUNT(*) FILTER (
                    WHERE action_type = 'Fon Alma'
                      AND fund_code IS NOT NULL
                ), 0),
            0
        ) AS ratio
    FROM dws.actions
    GROUP BY customer_id
) ratios
WHERE a.customer_id = ratios.customer_id;


-- yeni_portfoy
UPDATE dws.arketip a
SET yeni_portfoy = CASE 
    WHEN ap.customer_id IS NOT NULL THEN 1 
    ELSE 0 
END
FROM (
    SELECT DISTINCT customer_id
    FROM dws.actions
    WHERE action_type = 'Yeni Portföy Oluşturma'
) ap
WHERE a.customer_id = ap.customer_id;
-- Afterwards, set the rest to 0 just in case:
UPDATE dws.arketip
SET yeni_portfoy = 0
WHERE yeni_portfoy IS NULL;


-- birden_fazla_banka_hesabi
UPDATE dws.arketip a
SET birden_fazla_banka_hesabi = 1
FROM (
    SELECT DISTINCT customer_id
    FROM dws.actions
    WHERE action_type = 'Birden Fazla Banka Hesabı Var mı?'
) b
WHERE a.customer_id = b.customer_id;

-- Set 0 for users who don't (and still have NULL)
UPDATE dws.arketip
SET birden_fazla_banka_hesabi = 0
WHERE birden_fazla_banka_hesabi IS NULL;
