SELECT 
    COUNT(*) AS row_count,
    {{ var('min_bronze_rows') }} AS expected_minimum
FROM {{ ref('stg_bookings') }}
HAVING COUNT(*) < {{ var('min_bronze_rows') }}