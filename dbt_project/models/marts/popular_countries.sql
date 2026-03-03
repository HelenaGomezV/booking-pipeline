{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH booking_stats AS (
    SELECT
        country,
        COUNT(*) AS total_bookings,
        SUM(revenue) AS total_revenue,
        ROUND(AVG(adr), 2) AS avg_daily_rate,
        ROUND(AVG(total_nights), 1) AS avg_stay_nights,
        ROUND(100.0 * SUM(CASE WHEN is_repeated_guest = 1 THEN 1 ELSE 0 END) 
              / COUNT(*), 1) AS repeat_guest_pct
    FROM {{ ref('stg_bookings') }}
    WHERE is_canceled = 0
      AND country IS NOT NULL
    GROUP BY country
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_bookings DESC) AS popularity_rank,
        ROUND(100.0 * total_bookings 
              / SUM(total_bookings) OVER (), 1) AS booking_share_pct
    FROM booking_stats
)

SELECT * FROM ranked
ORDER BY popularity_rank