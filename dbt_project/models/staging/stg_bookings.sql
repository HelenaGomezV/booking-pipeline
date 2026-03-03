{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_bookings') }}
),

-- Mapa de meses: texto → número
month_map AS (
    SELECT * FROM (VALUES
        ('January', '01'), ('February', '02'), ('March', '03'),
        ('April', '04'), ('May', '05'), ('June', '06'),
        ('July', '07'), ('August', '08'), ('September', '09'),
        ('October', '10'), ('November', '11'), ('December', '12')
    ) AS m(month_name, month_number)
),

-- Deduplicación: si hay filas con el mismo hash, quédate con la primera
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY _row_hash
            ORDER BY _loaded_at
        ) AS _rn
    FROM source
),

cleaned AS (
    SELECT
        -- === Columnas originales con tipos correctos ===
        d.hotel                                             AS hotel_type,
        CAST(NULLIF(NULLIF(d.is_canceled, 'NA'), 'NULL') AS INTEGER)  AS is_canceled,
        CAST(NULLIF(NULLIF(d.booking_to_arrival_time, 'NA'), 'NULL') AS INTEGER)  AS lead_time,
        CAST(NULLIF(NULLIF(d.arrival_date_year, 'NA'), 'NULL') AS INTEGER)  AS arrival_date_year,
        d.arrival_date_month,
        CAST(NULLIF(NULLIF(d.arrival_date_week_number, 'NA'), 'NULL') AS INTEGER)  AS arrival_date_week_number,
        CAST(NULLIF(NULLIF(d.arrival_date_day_of_month, 'NA'), 'NULL') AS INTEGER)  AS arrival_date_day_of_month,
        CAST(NULLIF(NULLIF(d.stays_in_weekend_nights, 'NA'), 'NULL') AS INTEGER)  AS stays_in_weekend_nights,
        CAST(NULLIF(NULLIF(d.stays_in_week_nights, 'NA'), 'NULL') AS INTEGER)  AS stays_in_week_nights,
        CAST(NULLIF(NULLIF(d.adults, 'NA'), 'NULL') AS INTEGER)  AS adults,
        CAST(NULLIF(NULLIF(d.children, 'NA'), 'NULL') AS INTEGER)  AS children,
        CAST(NULLIF(NULLIF(d.babies, 'NA'), 'NULL') AS INTEGER)  AS babies,
        d.board,
        NULLIF(NULLIF(d.country, 'NULL'), 'NA')             AS country,
        d.market_segment,
        d.acquisition_channel,
        CAST(NULLIF(NULLIF(d.is_repeated_guest, 'NA'), 'NULL') AS INTEGER)  AS is_repeated_guest,
        CAST(NULLIF(NULLIF(d.previous_cancellations, 'NA'), 'NULL') AS INTEGER)  AS previous_cancellations,
        CAST(NULLIF(NULLIF(d.previous_bookings_not_canceled, 'NA'), 'NULL') AS INTEGER)  AS previous_bookings_not_canceled,
        d.reserved_room_type,
        d.assigned_room_type,
        CAST(NULLIF(NULLIF(d.booking_changes, 'NA'), 'NULL') AS INTEGER)  AS booking_changes,
        d.deposit_type,
        NULLIF(NULLIF(d.agent, 'NULL'), 'NA')               AS agent,
        NULLIF(NULLIF(d.company, 'NULL'), 'NA')             AS company,
        CAST(NULLIF(NULLIF(d.days_in_waiting_list, 'NA'), 'NULL') AS INTEGER)  AS days_in_waiting_list,
        d.customer_type,
        CAST(NULLIF(NULLIF(d.adr, 'NA'), 'NULL') AS NUMERIC(10,2))  AS adr,
        CAST(NULLIF(NULLIF(d.parking_lot, 'NA'), 'NULL') AS INTEGER)  AS parking_lot,
        CAST(NULLIF(NULLIF(d.total_of_special_requests, 'NA'), 'NULL') AS INTEGER)  AS total_of_special_requests,
        d.reservation_status,
        CAST(NULLIF(NULLIF(d.reservation_status_date, 'NA'), 'NULL') AS DATE)  AS reservation_status_date,

        -- === Columnas DERIVADAS (no existían en el CSV) ===

        -- Total de noches
        COALESCE(CAST(NULLIF(NULLIF(d.stays_in_weekend_nights, 'NA'), 'NULL') AS INTEGER), 0)
            + COALESCE(CAST(NULLIF(NULLIF(d.stays_in_week_nights, 'NA'), 'NULL') AS INTEGER), 0)
            AS total_nights,

        -- Revenue: ADR × noches (0 si cancelada o datos faltantes)
        CASE
            WHEN NULLIF(NULLIF(d.is_canceled, 'NA'), 'NULL') = '0'
            THEN COALESCE(CAST(NULLIF(NULLIF(d.adr, 'NA'), 'NULL') AS NUMERIC(10,2)), 0)
                 * (COALESCE(CAST(NULLIF(NULLIF(d.stays_in_weekend_nights, 'NA'), 'NULL') AS INTEGER), 0)
                    + COALESCE(CAST(NULLIF(NULLIF(d.stays_in_week_nights, 'NA'), 'NULL') AS INTEGER), 0))
            ELSE 0
        END                                                 AS revenue,

        -- Fecha de llegada reconstruida
        CASE
            WHEN d.arrival_date_year IS NOT NULL
                 AND m.month_number IS NOT NULL
                 AND d.arrival_date_day_of_month IS NOT NULL
                 AND d.arrival_date_year != 'NA'
                 AND d.arrival_date_day_of_month != 'NA'
            THEN TO_DATE(
                d.arrival_date_year || '-' || m.month_number || '-'
                || LPAD(d.arrival_date_day_of_month, 2, '0'),
                'YYYY-MM-DD'
            )
            ELSE NULL
        END                                                 AS arrival_date,

        -- Año-mes para agrupar
        CASE
            WHEN d.arrival_date_year != 'NA' AND m.month_number IS NOT NULL
            THEN d.arrival_date_year || '-' || m.month_number
            ELSE NULL
        END                                                 AS year_month,

        -- ¿Cambió de habitación?
        CASE
            WHEN d.reserved_room_type != d.assigned_room_type THEN TRUE
            ELSE FALSE
        END                                                 AS was_room_changed,

        -- Metadatos de lineage
        d._loaded_at,
        d._source_file,
        d._row_hash

    FROM deduplicated d
    LEFT JOIN month_map m ON d.arrival_date_month = m.month_name
    WHERE d._rn = 1
)

SELECT * FROM cleaned