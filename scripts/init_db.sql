-- ============================================
-- Medallion Architecture: Database Initialization
-- ============================================

-- Crear schemas 
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================
-- BRONZE
-- ============================================
CREATE TABLE IF NOT EXISTS bronze.raw_bookings (
    -- Las 32 columnas originales del CSV (todas TEXT)
    hotel                          TEXT,
    is_canceled                    TEXT,
    booking_to_arrival_time        TEXT,
    arrival_date_year              TEXT,
    arrival_date_month             TEXT,
    arrival_date_week_number       TEXT,
    arrival_date_day_of_month      TEXT,
    stays_in_weekend_nights        TEXT,
    stays_in_week_nights           TEXT,
    adults                         TEXT,
    children                       TEXT,
    babies                         TEXT,
    board                          TEXT,
    country                        TEXT,
    market_segment                 TEXT,
    acquisition_channel            TEXT,
    is_repeated_guest              TEXT,
    previous_cancellations         TEXT,
    previous_bookings_not_canceled TEXT,
    reserved_room_type             TEXT,
    assigned_room_type             TEXT,
    booking_changes                TEXT,
    deposit_type                   TEXT,
    agent                          TEXT,
    company                        TEXT,
    days_in_waiting_list           TEXT,
    customer_type                  TEXT,
    adr                            TEXT,
    parking_lot                    TEXT,
    total_of_special_requests      TEXT,
    reservation_status             TEXT,
    reservation_status_date        TEXT,

    -- 3 columnas de METADATOS (las añadimos nosotros)
    _loaded_at                     TIMESTAMP DEFAULT NOW(),
    _source_file                   TEXT,
    _row_hash                      TEXT
);

-- Índice único para idempotencia
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_bookings_hash
    ON bronze.raw_bookings (_row_hash);