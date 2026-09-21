

WITH product_events AS (
    SELECT 
        laptop_id,
        user_pseudo_id,
        event_timestamp,

        is_valid_product_view,
        is_valid_product_comparison_event,
        is_select_event,
        is_add_to_comparison_event
    
    FROM `laplap-analytics`.`laplap_analytics`.`int_events_parsed`

    WHERE laptop_id is NOT NULL
        AND (is_valid_product_view OR is_valid_product_comparison_event)
),

product_metrics AS (
    SELECT 
        laptop_id,
        COUNTIF(is_valid_product_view) AS view_count,
        COUNTIF(is_valid_product_comparison_event AND is_select_event) AS select_count,
        COUNTIF(is_valid_product_comparison_event AND is_add_to_comparison_event) AS add_compare_count,
        COUNT(DISTINCT user_pseudo_id) AS unique_engaged_users,
        MIN(event_timestamp) AS first_interaction_at,
        MAX(event_timestamp) AS last_interaction_at
    FROM product_events
    GROUP BY laptop_id
),

product_metrics_with_totals AS (
    SELECT 
        laptop_id,
        view_count,
        select_count,
        add_compare_count,
        (view_count + select_count + add_compare_count) AS interaction_count,
        unique_engaged_users,
        first_interaction_at,
        last_interaction_at
    FROM product_metrics
),

product_with_all_laptops AS (

    SELECT
        l.laptop_id,

        COALESCE(p.view_count, 0) AS view_count,
        COALESCE(p.select_count, 0) AS select_count,
        COALESCE(p.add_compare_count, 0) AS add_compare_count,
        COALESCE(p.interaction_count, 0) AS interaction_count,
        COALESCE(p.unique_engaged_users, 0) AS unique_engaged_users,

        p.first_interaction_at,
        p.last_interaction_at
    FROM `laplap-analytics`.`laplap_analytics`.`dim_laptop` AS l
    LEFT JOIN product_metrics_with_totals AS p ON l.laptop_id = p.laptop_id
),

engagement_rates AS (
    SELECT 
        *,
        ROUND(COALESCE(SAFE_DIVIDE(select_count + add_compare_count, interaction_count) * 100, 0), 2) AS intent_rate
    FROM product_with_all_laptops
),

ranked_products AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY view_count) * 100 AS view_percentile,
        PERCENT_RANK() OVER ( ORDER BY select_count) * 100 AS select_percentile,
        PERCENT_RANK() OVER (ORDER BY add_compare_count) * 100 AS add_compare_percentile
    FROM engagement_rates
)

SELECT
    laptop_id,

    view_count,
    select_count,
    add_compare_count,
    interaction_count,

    unique_engaged_users,

    ROUND(
        (
            view_percentile
            + select_percentile
            + add_compare_percentile
        ) / 3,
        2
    ) AS popularity_score,

    intent_rate,

    first_interaction_at,
    last_interaction_at

FROM ranked_products