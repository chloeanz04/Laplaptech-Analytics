
  
    

    create or replace table `laplap-analytics`.`laplap_analytics`.`dim_laptop_benchmark`
      
    
    

    
    OPTIONS()
    as (
      

WITH ranked AS (

    SELECT
        benchmark_result_id,
        laptop_model_id AS laptop_id,

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

        record_updated_at,
        elton_created_at,

        ROW_NUMBER() OVER (
            PARTITION BY laptop_model_id
            ORDER BY
                record_updated_at DESC,
                elton_created_at DESC
        ) AS rn

    FROM `laplap-analytics`.`laplap_analytics`.`stg_laptop_benchmark_result`

)

SELECT
    benchmark_result_id,
    laptop_id,

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
    is_active

FROM ranked

WHERE rn = 1
    );
  