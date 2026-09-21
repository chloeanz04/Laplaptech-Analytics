

SELECT
    l.laptop_id,
    l.laptop_name,

    -- Brand
    b.brand_id,
    b.brand_name,

    -- CPU
    l.cpu_model_id,
    c.cpu_model_name,

    -- GPU
    l.gpu_model_id,
    g.gpu_model_name,

    -- Product classification
    l.is_gaming_laptop,
    l.is_workstation,
    l.is_mobile_device,

    -- Product specifications
    l.year_introduce,
    l.screen_size,
    l.screen_dimension_width,
    l.screen_dimension_height,
    l.screen_ppi,
    l.laptop_weight,
    l.charger_weight,
    l.battery_capacity_whr,

    -- CPU / GPU specs from laptop model
    l.cpu_note,
    l.cpu_tdp,
    l.gpu_note,
    l.gpu_tdp,

    -- Benchmark
    bm.office_battery_result_minutes,
    bm.gaming_battery_result_minutes,

    bm.geekbench_7_cpu_single_core_plugged_in,
    bm.geekbench_7_cpu_multi_core_plugged_in,
    bm.geekbench_7_compute_gpu_plugged_in,

    bm.geekbench_7_cpu_single_core_battery,
    bm.geekbench_7_cpu_multi_core_battery,
    bm.geekbench_7_compute_gpu_battery,

    -- Metadata useful for filtering
    l.is_visible,
    l.is_active

FROM `laplap-analytics`.`laplap_analytics`.`int_laptop_model_latest` AS l
LEFT JOIN `laplap-analytics`.`laplap_analytics`.`dim_brand` AS b ON l.brand_id = b.brand_id
LEFT JOIN `laplap-analytics`.`laplap_analytics`.`dim_cpu` AS c ON l.cpu_model_id = c.cpu_model_id
LEFT JOIN `laplap-analytics`.`laplap_analytics`.`dim_gpu` AS g ON l.gpu_model_id = g.gpu_model_id
LEFT JOIN `laplap-analytics`.`laplap_analytics`.`dim_laptop_benchmark` AS bm ON l.laptop_id = bm.laptop_id