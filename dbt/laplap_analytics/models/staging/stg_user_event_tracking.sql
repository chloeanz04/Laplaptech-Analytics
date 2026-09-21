{{ config(materialized='view') }}

SELECT
    id,
    event_name,
    user_id,
    user_psuedo_id AS user_pseudo_id,
    session_id,

    event_local_timestamp AS event_timestamp,
    event_received_on_server_timestamp AS server_received_timestamp,
    elton_created_at,

    app_version,
    device,
    event_data,

    JSON_VALUE(event_data, '$.page_name') AS page_name,
    JSON_VALUE(event_data, '$.referrer') AS referrer,
    JSON_VALUE(event_data, '$.url') AS url,
    JSON_VALUE(event_data, '$.keyword') AS search_keyword,

    JSON_VALUE(event_data, '$.device_id') AS device_id_raw,

    REGEXP_REPLACE(
        JSON_VALUE(event_data, '$.device_id'),
        r'%2[Cc]',
        ','
    ) AS device_id_normalized,

    JSON_QUERY(event_data, '$.device_ids') AS device_ids_json,
    JSON_VALUE(event_data, '$.sort_by') AS sort_by,
    JSON_VALUE(event_data, '$.sort_direction') AS sort_direction,
    JSON_VALUE(event_data, '$.geekbench_version') AS geekbench_version,

    SAFE_CAST(JSON_VALUE(event_data, '$.page') AS INT64) AS home_page

FROM {{ source('laplap', 'bronze_user_event_tracking') }}