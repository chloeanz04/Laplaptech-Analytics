
  
    

    create or replace table `laplap-analytics`.`laplap_analytics`.`fct_comparison_behavior`
      
    
    

    
    OPTIONS()
    as (
      

WITH comparison_events AS (

    SELECT
        id AS event_id,

        user_pseudo_id,
        session_id,

        event_timestamp,
        elton_created_at,

        event_name,

        comparison_device_ids,
        comparison_device_count,

        sort_by,
        sort_direction,
        geekbench_version

    FROM `laplap-analytics`.`laplap_analytics`.`int_events_parsed`

    WHERE has_comparison_set

),

comparison_behavior AS (

    SELECT
        event_id,

        user_pseudo_id,
        session_id,

        event_timestamp,
        elton_created_at,

        event_name,

        comparison_device_count,

        CASE
            WHEN comparison_device_count = 1 THEN '1_product'
            WHEN comparison_device_count = 2 THEN '2_products'
            WHEN comparison_device_count = 3 THEN '3_products'
            WHEN comparison_device_count >= 4 THEN '4_plus_products'
            ELSE 'unknown'
        END AS comparison_size_group,

        CASE
            WHEN comparison_device_count = 1 THEN 'single_product'
            WHEN comparison_device_count = 2 THEN 'two_products'
            WHEN comparison_device_count >= 3 THEN 'multi_product'
            ELSE 'unknown'
        END AS comparison_type,

        sort_by,
        sort_direction,
        geekbench_version,

        ARRAY_TO_STRING(comparison_device_ids, ',') AS comparison_device_ids
    FROM comparison_events

)

SELECT
    event_id,

    user_pseudo_id,
    session_id,

    event_timestamp,
    elton_created_at,

    event_name,

    comparison_device_count,
    comparison_size_group,
    comparison_type,

    sort_by,
    sort_direction,
    geekbench_version,

    comparison_device_ids

FROM comparison_behavior
    );
  