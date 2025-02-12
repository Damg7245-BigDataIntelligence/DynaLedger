SELECT value
FROM {{ ref('raw_num_table') }}
WHERE value < -100000000000 OR value > 100000000000
