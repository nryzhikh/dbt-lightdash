SELECT
    id,
    (
        COALESCE(CAST(otc_колво_подписчиков_канала AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_18 AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_45_inc_3_5 AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_54_bc AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_55 AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_55_bc AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_55_inc_3_5 AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_25_58_inc_3_5 AS NUMERIC), 0) +
        COALESCE(CAST(ots_all_50_65 AS NUMERIC), 0) +
        COALESCE(CAST(отs AS NUMERIC), 0)
    ) AS ots_all
FROM {{ ref('эконометрика_общая') }}