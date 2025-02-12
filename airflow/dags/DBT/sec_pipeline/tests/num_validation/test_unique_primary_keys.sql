SELECT adsh, COUNT(*)
FROM {{ ref('raw_num_table') }}
GROUP BY adsh
HAVING COUNT(*) > 1
