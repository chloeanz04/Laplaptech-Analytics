{{ config(materialized='view') }}

SELECT
    id AS benchmark_result_id, 
    laptop_model_id, 
    office_battery_result_minutes, 
    gaming_battery_result_minutes, 
    foldable_opening_battery_result_minutes,

    geekbench_6_compute_gpu_plugged_in, 
    geekbench_6_compute_gpu_battery, 
    geekbench_6_cpu_single_core_plugged_in, 
    geekbench_6_cpu_single_core_battery,

    geekbench_6_cpu_multi_core_plugged_in, 
    geekbench_6_cpu_multi_core_battery, 
    geekbench_7_compute_gpu_plugged_in, 
    geekbench_7_compute_gpu_battery,

    geekbench_7_cpu_single_core_plugged_in, 
    geekbench_7_cpu_single_core_battery, 
    geekbench_7_cpu_multi_core_plugged_in, 
    geekbench_7_cpu_multi_core_battery,

    note, 
    review_video_url, 
    is_active, 
    created_on, 
    changed_on, 
    elton_created_at, 
    COALESCE(changed_on, created_on, elton_created_at) AS record_updated_at
FROM {{ source('laplap', 'bronze_laptop_benchmark_result') }}