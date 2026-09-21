

SELECT
    id AS laptop_id,
    name AS laptop_name,

    is_gaming_laptop,
    is_workstation,
    is_mobile_device,
    is_visible,
    is_active,

    brand_id,
    cpu_model_id,
    gpu_model_id,

    year_introduce,
    screen_size,
    screen_dimension_width,
    screen_dimension_height,
    screen_ppi,
    laptop_weight,
    charger_weight,
    battery_capacity_whr,

    cpu_note,
    cpu_tdp,
    gpu_note,
    gpu_tdp,

    brand_model_codename,
    thumbnail_image_url,

    created_on,
    changed_on,
    elton_created_at,

    COALESCE(
        changed_on,
        created_on,
        elton_created_at
    ) AS record_updated_at

FROM `laplap-analytics`.`laplap_analytics`.`bronze_laptop_model_raw`