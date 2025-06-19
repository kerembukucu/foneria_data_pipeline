-- 0 - Create index on digital_actions
DROP INDEX IF EXISTS dws.IDX_digital_actions_CUST_ID;
CREATE INDEX IDX_digital_actions_CUST_ID ON dws.digital_actions (customer_id);

-- 1 - Create digital_actions_formatted
DROP TABLE IF EXISTS dws.digital_actions_formatted;

CREATE TABLE dws.digital_actions_formatted AS 
SELECT 
    da.customer_id,
    da.view_name,
    da.action,
    da.details, 
    CONCAT(da.view_name, ' - ', da.action, ' - ', SPLIT_PART(da.details, ',', 1)) AS event_field,
    da.event_time::timestamp AS converted_timestamp,
    EXTRACT(YEAR FROM da.event_time::timestamp) AS year,
    EXTRACT(MONTH FROM da.event_time::timestamp) AS month,
    SPLIT_PART(da.details, ',', 1) AS details_bef_comma,
    SPLIT_PART(da.details, ',', 2) AS details_aft_comma,
    '' event_data
FROM dws.digital_actions da
WHERE 
    -- customer_id < 1000000 AND 
    event_time >= (CURRENT_DATE - INTERVAL '6 months');

DROP INDEX IF EXISTS dws.IDX_digital_actions_formatted_event_field;
CREATE INDEX IDX_digital_actions_formatted_event_field ON dws.digital_actions_formatted (event_field);
ANALYZE dws.digital_actions_formatted;

-- 2 - Create lookup tables
DROP TABLE IF EXISTS dws.lookup_START_END_EVENTS;
CREATE TABLE dws.lookup_START_END_EVENTS (
    seq SERIAL PRIMARY KEY,  
    direction TEXT NOT NULL, 
    view_name TEXT NOT NULL,
    action TEXT NOT NULL,
    event_details TEXT NOT NULL,
    event_field TEXT NOT NULL
);

INSERT INTO dws.lookup_START_END_EVENTS (direction, view_name, action, event_details, event_field) 
    VALUES 
       ('last', 'account', 'click', 'deposit', 'account - click - deposit'),
       ('last', 'account', 'click', 'openAccount', 'account - click - openAccount'),
       ('last', 'moneyTransfer', 'click', 'addBankAccount', 'moneyTransfer - click - addBankAccount');

DROP INDEX IF EXISTS dws.idx_end_events_table;
CREATE INDEX idx_end_events_table ON dws.lookup_START_END_EVENTS (event_field);

DROP INDEX IF EXISTS dws.idx_lookup_start_end_event;
CREATE INDEX idx_lookup_start_end_event ON dws.lookup_START_END_EVENTS (event_field, direction);

DROP TABLE IF EXISTS dws.lookup_IGNORED_EVENTS;
CREATE TABLE dws.lookup_IGNORED_EVENTS (
    seq SERIAL PRIMARY KEY,  
    view_name TEXT NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    ignored_event_field TEXT NOT NULL
);

-- Insert sabit veri seti
INSERT INTO dws.lookup_IGNORED_EVENTS (view_name, action, details, ignored_event_field) 
VALUES 
    ('signIn', 'click', 'auto login', 'signIn - click - auto login'),
    ('foreground', 'open', 'app is on foreground', 'foreground - open - app is on foreground'),
    ('foreground', 'open', 'app is on foreground ios', 'foreground - open - app is on foreground ios'),
    ('x', 'x', 'x', 'background - open - app is on background ios'),
    ('x', 'x', 'x', 'background - open - app is on background'),
    ('x', 'x', 'x', 'settings - click - Çıkış Yap'),
    ('x', 'x', 'x', 'bottomTabBar - click - Hesabım'),
    ('x', 'x', 'x', 'bottomTabBar - click - Portföyüm'),
    ('x', 'x', 'x', 'account - click - settings'),
    ('x', 'x', 'x', 'account -  withdraw'),
    ('x', 'x', 'x', 'fundDetails - click - buy'),
    ('x', 'x', 'x', 'fundDetails - click - back'),
    ('x', 'x', 'x', 'settings - click - k Yap'),
    ('x', 'x', 'x', 'signn - click - auto login'),
    ('x', 'x', 'x', 'bottomTabBar - click - Hesabm'),
    ('x', 'x', 'x', 'bottomTabBar - click - Portfym');

-- Insert dinamik olarak oluşturulan veri seti
INSERT INTO dws.lookup_IGNORED_EVENTS (view_name, action, details, ignored_event_field) 
SELECT DISTINCT 
    'auto' AS view_name,
    'cancelbackclose' AS action,
    'cancelbackclose' AS details,
    event_field AS ignored_event_field
FROM dws.digital_actions_formatted  
WHERE 
    event_field ILIKE '%cancel%' OR 
    event_field ILIKE '%close%' OR 
    event_field ILIKE '%back%' OR  
    event_field ILIKE '%chart%' OR 
    event_field ILIKE '%zebra%';

-- Index
DROP INDEX IF EXISTS dws.idx_lookup_ignored_events;
CREATE INDEX idx_lookup_ignored_events ON dws.lookup_IGNORED_EVENTS (ignored_event_field);

-- 3 - Create digital_actions_process
DROP TABLE IF EXISTS dws.digital_actions_process;

CREATE TABLE dws.digital_actions_process AS 
SELECT 
    customer_id,
    year,
    month,
    'digital' source,
    view_name,
    action,
    details,
    event_field,
    event_data,
    details_bef_comma,
    details_aft_comma,
    '' event_data_text,
    0  event_data_num,
    converted_timestamp 
FROM dws.digital_actions_formatted da 
WHERE 
    da.event_field NOT IN (SELECT ignored_event_field FROM dws.lookup_IGNORED_EVENTS);

INSERT INTO dws.digital_actions_process
(
    customer_id,
    year,
    month,
    source,
    view_name,
    action,
    details,
    event_field,
    event_data,
    details_bef_comma,
    details_aft_comma,
    event_data_text,
    event_data_num,
    converted_timestamp 
)
SELECT 
    customer_id,
    EXTRACT(YEAR FROM TO_TIMESTAMP(SPLIT_PART(timestamp, '-', 2), 'DD.MM.YY')) AS year,
    EXTRACT(MONTH FROM TO_TIMESTAMP(SPLIT_PART(timestamp, '-', 2), 'DD.MM.YY')) AS month,
    'transaction' AS source,
    '' AS view_name, 
    '' AS action,
    action_type AS details,
    action_type AS event_field,
    '' event_data,
    '' details_bef_comma,
    '' details_aft_comma,
    fund_code AS event_data_text,
    amount AS event_data_num,
    TO_TIMESTAMP(SPLIT_PART(timestamp, '-', 2), 'DD.MM.YY') + 
    (SPLIT_PART(SPLIT_PART(timestamp, '-', 1), '.', 1) || ' days ' || 
     SPLIT_PART(SPLIT_PART(timestamp, '-', 1), '.', 2) || ' hours')::interval AS converted_timestamp
FROM dws.actions
WHERE 
    -- customer_id < 1000000 AND 
    timestamp ~ '^[0-9]{2}\.[0-9]{2}-[0-9]{2}\.[0-9]{2}\.[0-9]{2}$' AND
    TO_TIMESTAMP(SPLIT_PART(timestamp, '-', 2), 'DD.MM.YY') >= (CURRENT_DATE - INTERVAL '6 months');

DROP INDEX IF EXISTS dws.IDX_digital_actions_process_par_month;
CREATE INDEX IDX_digital_actions_process_par_month ON dws.digital_actions_process (month);

CREATE INDEX idx_digital_actions_customer_id_partition ON dws.digital_actions_process (customer_id);
CREATE INDEX idx_digital_actions_event_field ON dws.digital_actions_process (event_field);
CREATE INDEX idx_digital_actions_time ON dws.digital_actions_process (converted_timestamp);

ANALYZE dws.digital_actions_process;

-- 4 - Create temp_START_END_EVENTS
DROP TABLE IF EXISTS dws.temp_START_END_EVENTS;

CREATE TABLE dws.temp_START_END_EVENTS (
    seq INT NOT NULL,  
    direction TEXT NOT NULL,
    customer_id INT NOT NULL, 
    converted_timestamp TIMESTAMP NOT NULL,
    event_field TEXT NOT NULL,
    event_data_text TEXT,
    event_data_num INT
);

INSERT INTO dws.temp_START_END_EVENTS  
SELECT 
    et.seq, et.direction, CAST(da.customer_id AS INT), da.converted_timestamp, da.event_field, da.event_data_text, da.event_data_num
FROM dws.digital_actions_process da
JOIN dws.lookup_START_END_EVENTS et 
    ON da.event_field = et.event_field AND et.direction = 'last';

DROP INDEX IF EXISTS dws.temp_START_END_EVENTS_ind1;
CREATE INDEX temp_START_END_EVENTS_ind1 ON dws.temp_START_END_EVENTS (customer_id, converted_timestamp);

DROP INDEX IF EXISTS dws.temp_START_END_EVENTS_ind2;
CREATE INDEX temp_START_END_EVENTS_ind2 ON dws.temp_START_END_EVENTS (converted_timestamp);

DROP INDEX IF EXISTS dws.temp_START_END_EVENTS_ind3;
CREATE INDEX temp_START_END_EVENTS_ind3 ON dws.temp_START_END_EVENTS (event_field);

-- Drop and create the output table
DROP TABLE IF EXISTS dws.out_path_4_events;

CREATE TABLE dws.out_path_4_events (
    seq INT, 
    direction TEXT NOT NULL, 
    cust_id BIGINT, 
    last_event_timestamp TIMESTAMP,
    event_1 TEXT, 
    event_2 TEXT, 
    event_3 TEXT, 
    event_4 TEXT,
    event_4_data_text TEXT,
    event_4_data_num INT
);

-- Insert processed event data
INSERT INTO dws.out_path_4_events 
(seq, direction, cust_id, last_event_timestamp, event_1, event_2, event_3, event_4, event_4_data_text, event_4_data_num)
WITH end_event AS (
    SELECT 
        *, 
        converted_timestamp AS last_event_timestamp
    FROM dws.temp_START_END_EVENTS
    ORDER BY converted_timestamp
),
lagged_events AS (
    SELECT 
        CAST(da.customer_id AS INT) AS customer_id, 
        da.converted_timestamp AS event_time, 
        da.event_field AS event_type, 
        LAG(da.event_field) OVER (PARTITION BY CAST(da.customer_id AS INT) ORDER BY da.converted_timestamp) AS prev_event,
        ee.seq, 
        ee.last_event_timestamp
    FROM dws.digital_actions_process da
    JOIN end_event ee 
        ON CAST(da.customer_id AS INT) = ee.customer_id AND da.converted_timestamp < ee.converted_timestamp
),
event_group_step1 AS (
    SELECT 
        customer_id, 
        event_time, 
        event_type, 
        seq, 
        last_event_timestamp,
        CASE 
            WHEN event_type != prev_event OR prev_event IS NULL THEN 1 
            ELSE 0 
        END AS is_new_group
    FROM lagged_events
),
event_with_groups AS (
    SELECT 
        customer_id, 
        event_time, 
        event_type, 
        seq, 
        last_event_timestamp,
        SUM(is_new_group) OVER (PARTITION BY customer_id, seq ORDER BY event_time) AS event_group
    FROM event_group_step1
),
distinct_events AS (
    SELECT 
        customer_id, 
        event_time, 
        event_type, 
        seq, 
        last_event_timestamp
    FROM (
        SELECT 
            customer_id, 
            event_time, 
            event_type, 
            seq, 
            last_event_timestamp,
            ROW_NUMBER() OVER (PARTITION BY customer_id, seq, event_type ORDER BY event_time DESC) AS rn
        FROM event_with_groups
    ) sub 
    WHERE rn = 1
),
last_3_events AS (
    SELECT 
        customer_id, 
        event_time, 
        event_type, 
        seq, 
        last_event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY customer_id, seq ORDER BY event_time DESC) AS event_rank
    FROM distinct_events
)
SELECT 
    ee.seq, 
    'last', 
    ee.customer_id,  -- already INT from temp_START_END_EVENTS
    ee.last_event_timestamp,
    l3_1.event_type AS event_1,
    l3_2.event_type AS event_2,
    l3_3.event_type AS event_3,
    ee.event_field AS event_4,
    ee.event_data_text AS event_4_data_text,
    ee.event_data_num AS event_4_data_num
FROM last_3_events l3_1
LEFT JOIN last_3_events l3_2 
    ON l3_1.customer_id = l3_2.customer_id AND l3_1.seq = l3_2.seq AND l3_2.event_rank = 2
LEFT JOIN last_3_events l3_3 
    ON l3_1.customer_id = l3_3.customer_id AND l3_1.seq = l3_3.seq AND l3_3.event_rank = 3
JOIN end_event ee 
    ON l3_1.customer_id = ee.customer_id AND l3_1.seq = ee.seq
WHERE l3_1.event_rank = 1
ORDER BY ee.seq, l3_1.customer_id;
