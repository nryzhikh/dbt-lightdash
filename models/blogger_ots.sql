SELECT
    b.id,
    start_date,
     ROUND((
      SUM(CASE
        WHEN channel ILIKE '%блогер%'
        THEN o.ots_all / 1000.0
        ELSE 0
      END)
    )::NUMERIC, 1)
FROM
    econometrics b
LEFT JOIN
    {{ ref('ots_all') }} o ON b.id = o.id
GROUP BY
    b.id,
    start_date