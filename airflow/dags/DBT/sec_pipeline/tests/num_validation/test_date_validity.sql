SELECT ddate
FROM {{ ref('raw_num_table') }}
WHERE TRY_CAST(TO_DATE(CAST(ddate AS STRING), 'YYYYMMDD') AS DATE) IS NULL
