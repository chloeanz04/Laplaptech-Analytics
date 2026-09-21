{{ config(materialized='view') }}

WITH parsed AS (
    SELECT 
        id,
        event_name,

        user_id,
        user_pseudo_id,
        session_id,

        event_timestamp,
        server_received_timestamp,
        elton_created_at,

        app_version,
        device,

        page_name,
        referrer,
        url,
        search_keyword,

        device_id_raw,
        device_id_normalized,

        /*
        Single-product device ID.

        Valid example:
        "157" -> 157

        Malformed / multi-device example:
        "114%2C104" -> "114,104" -> NULL
        */

        SAFE_CAST(device_id_normalized AS INT64) AS laptop_id,

        device_ids_json,
        sort_by,
        sort_direction,
        geekbench_version,
        home_page 
    FROM {{ ref('stg_user_event_tracking') }}
),

comparison_parsed AS (
    SELECT 
        *,
        JSON_VALUE_ARRAY(device_ids_json) AS comparison_device_ids
    FROM parsed
)

SELECT
    id,
    event_name,

    user_id,
    user_pseudo_id,
    session_id,

    event_timestamp,
    server_received_timestamp,
    elton_created_at,

    app_version,
    device,

    page_name,
    referrer,
    url,
    search_keyword,

    device_id_raw,
    device_id_normalized,
    laptop_id,

    comparison_device_ids,

    ARRAY_LENGTH(comparison_device_ids) AS comparison_device_count,

    sort_by,
    sort_direction,
    geekbench_version,
    home_page,

    event_name = 'search_for_device' AS is_search_event,
    event_name = 'pageview' AND page_name = 'DeviceDetail' AS is_product_view_event,
    event_name = 'select_device_for_comparison' AS is_select_event,
    event_name = 'add_to_comparison' AS is_add_to_comparison_event,
    event_name = 'comparison_chart_sort_selection' AS is_comparison_sort_event,
    event_name = 'comparison_chart_geekbench_version_selection' AS is_geekbench_selection_event,

    (
        event_name = 'pageview'
        AND page_name = 'DeviceDetail'
        AND laptop_id IS NOT NULL
    ) AS is_valid_product_view,

    (
        event_name IN (
            'select_device_for_comparison',
            'add_to_comparison'
        )
        AND laptop_id IS NOT NULL
    ) AS is_valid_product_comparison_event,

    (
        event_name IN (
            'comparison_chart_sort_selection',
            'comparison_chart_geekbench_version_selection'
        )
        AND ARRAY_LENGTH(comparison_device_ids) > 0
    ) AS has_comparison_set

FROM comparison_parsed