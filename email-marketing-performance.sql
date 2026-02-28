/*
PROJECT: Email Marketing Performance & User Retention Analysis
OBJECTIVE: Build a consolidated analytical view to track user registration 
           and email engagement metrics across top-performing countries.
AUTHOR: [Yehor Samoilov]
*/

WITH Account_Attributes AS (
-- CTE 1: Collecting dimension attributes for accounts and sessions.
-- This serves as the base table for joining metrics across different datasets.

    SELECT DISTINCT
        SES.date AS date,                
        ACC.id AS account_id,            
        SEP.country AS country,          
        ACC.send_interval AS acc_interval, -- Email frequency setting
        
        -- Converting numeric flags (1/0) to readable TRUE/FALSE strings
        CASE
            WHEN ACC.is_verified = 1 THEN "TRUE"
            ELSE "FALSE"
        END AS is_verified,
        CASE
            WHEN ACC.is_unsubscribed = 1 THEN "TRUE"
            ELSE "FALSE"
        END AS is_unsubscribed
    FROM
        `DA.account` AS ACC
    JOIN
        `DA.account_session` AS ACS ON ACC.id = ACS.account_id
    JOIN
        `DA.session_params` AS SEP ON ACS.ga_session_id = SEP.ga_session_id
    JOIN
        `DA.session` AS SES ON ACS.ga_session_id = SES.ga_session_id
),

Account_Metrics AS (
-- CTE 2: Calculating account creation metrics by dimensions.

    SELECT
        date,
        COUNT(DISTINCT account_id) AS account,
        country,
        acc_interval,
        is_verified,
        is_unsubscribed
    FROM
        Account_Attributes
    GROUP BY 1, 3, 4, 5, 6
),

Email_Metrics AS (
-- CTE 3: Calculating email engagement metrics (sent, opened, visited).

    SELECT
        -- Calculating delivery date: Account creation date + days elapsed
        DATE_ADD(AAT.date, INTERVAL ES.sent_date DAY) AS date,
        country,
        acc_interval,
        is_verified,
        is_unsubscribed,
        COUNT(DISTINCT ES.id_message) AS sent_cnt,  
        COUNT(DISTINCT EO.id_message) AS open_cnt,  
        COUNT(DISTINCT EV.id_message) AS visit_cnt  
    FROM
        `DA.email_sent` AS ES
    LEFT JOIN
        `DA.email_open` AS EO ON ES.id_message = EO.id_message  
    LEFT JOIN
        `DA.email_visit` AS EV ON ES.id_message = EV.id_message  
    JOIN
        Account_Attributes AS AAT ON ES.id_account = AAT.account_id
    GROUP BY 1, 2, 3, 4, 5
),

FINAL AS (
-- CTE 4: Merging account and email metrics into a single harmonized dataset.

    SELECT
        date,
        account AS account,
        country,
        acc_interval,
        is_verified,
        is_unsubscribed,
        0 AS sent_cnt,    
        0 AS open_cnt,
        0 AS visit_cnt
    FROM Account_Metrics

    UNION ALL

    SELECT
        date,
        0 AS account,    
        country,
        acc_interval,
        is_verified,
        is_unsubscribed,
        sent_cnt,
        open_cnt,
        visit_cnt
    FROM Email_Metrics
),

Aggregated AS (
-- CTE 5: Final aggregation to sum up metrics across all dimensions.

    SELECT
        date,
        SUM(account) AS account_cnt,  
        country,
        acc_interval,
        is_verified,
        is_unsubscribed,
        SUM(sent_cnt) AS sent_msg,    
        SUM(open_cnt) AS open_msg,    
        SUM(visit_cnt) AS visit_msg  
    FROM
        FINAL
    GROUP BY 1, 3, 4, 5, 6
),

with_country_totals AS (
-- CTE 6: Calculating country-level totals using Window Functions.

    SELECT
        *,
        SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM
        Aggregated
),

final_with_ranks AS (
-- CTE 7: Ranking countries based on total volume using DENSE_RANK.

    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM
        with_country_totals
)

-- Final SELECT: Filtering for Top 10 countries based on either acquisition or engagement metrics.
SELECT
    date,
    account_cnt,
    country,
    acc_interval,
    is_verified,
    is_unsubscribed,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    rank_total_country_account_cnt,
    rank_total_country_sent_cnt
FROM
    final_with_ranks
WHERE
    rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10;
