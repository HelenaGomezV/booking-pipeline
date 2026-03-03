{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH monthly_revenue AS (
    SELECT
        hotel_type,
        year_month,
        COUNT(*) AS total_bookings,
        SUM(revenue) AS total_booking_amount,
        ROUND(AVG(adr), 2) AS avg_daily_rate,
        SUM(CASE WHEN is_canceled = 0 THEN 1 ELSE 0 END) AS completed_bookings,
        SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) AS canceled_bookings
    FROM {{ ref('stg_bookings') }}
    WHERE year_month IS NOT NULL
    GROUP BY hotel_type, year_month
),

with_mom AS (
    SELECT
        *,
        LAG(total_booking_amount) OVER (
            PARTITION BY hotel_type
            ORDER BY year_month
        ) AS prev_month_amount,
        CASE
            WHEN LAG(total_booking_amount) OVER (
                PARTITION BY hotel_type
                ORDER BY year_month
            ) IS NOT NULL
            AND LAG(total_booking_amount) OVER (
                PARTITION BY hotel_type
                ORDER BY year_month
            ) > 0
            THEN ROUND(
                100.0 * (total_booking_amount - LAG(total_booking_amount) OVER (
                    PARTITION BY hotel_type
                    ORDER BY year_month
                )) / LAG(total_booking_amount) OVER (
                    PARTITION BY hotel_type
                    ORDER BY year_month
                ), 1
            )
            ELSE NULL
        END AS mom_pct_change
    FROM monthly_revenue
)

SELECT * FROM with_mom
ORDER BY hotel_type, year_month