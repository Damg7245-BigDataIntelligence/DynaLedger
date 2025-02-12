

SELECT adsh, report, line, COUNT(*) 
FROM {{ ref('raw_pre_table') }}
GROUP BY adsh, report, line
HAVING COUNT(*) > 1
