SELECT
    id,
    (
        COALESCE(otc_колво_подписчиков_канала, 0) +
        COALESCE(ots_all_18, 0) +
        COALESCE(ots_all_25_45_inc_3_5, 0) +
        COALESCE(ots_all_25_54_bc, 0) +
        COALESCE(ots_all_25_55, 0) +
        COALESCE(ots_all_25_55_bc, 0) +
        COALESCE(ots_all_25_55_inc_3_5, 0) +
        COALESCE(ots_all_25_58_inc_3_5, 0) +
        COALESCE(ots_all_50_65, 0)
    ) AS ots_all
FROM econometrics