DROP TABLE IF EXISTS dws.summary_static;

CREATE TABLE dws.summary_static AS
SELECT
    md.customer_id AS cust_id,
    CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - md.birth_year <= 25 THEN '0-25'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - md.birth_year <= 40 THEN '26-40'
        ELSE '40+'
    END AS age_range,
    md.occupation AS job,
    md.education AS education,
    md.interest_preference AS interest_preference,
    survey_risk.result_value AS survey_risk,
    bit_score.result_value AS bit_score,
    md.registration_time AS reg_date,
    md.account_ready_time AS account_ready_date
FROM dws.member_details md
LEFT JOIN dws.bias_results survey_risk 
    ON md.customer_id = survey_risk.customer_id 
   AND survey_risk.result_key = 'Risk-Score'
LEFT JOIN dws.bias_results bit_score 
    ON md.customer_id = bit_score.customer_id 
   AND bit_score.result_key = 'BIT';
