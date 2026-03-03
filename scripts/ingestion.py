"""
Bronze Ingestion: CSV → PostgreSQL (raw_bookings)
Loads raw booking data without transformations.
"""
import csv
import hashlib
import logging
import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    'hotel', 'is_canceled', 'booking_to_arrival_time', 'arrival_date_year',
    'arrival_date_month', 'arrival_date_week_number', 'arrival_date_day_of_month',
    'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children',
    'babies', 'board', 'country', 'market_segment', 'acquisition_channel',
    'is_repeated_guest', 'previous_cancellations', 'previous_bookings_not_canceled',
    'reserved_room_type', 'assigned_room_type', 'booking_changes', 'deposit_type',
    'agent', 'company', 'days_in_waiting_list', 'customer_type', 'adr',
    'parking_lot', 'total_of_special_requests', 'reservation_status',
    'reservation_status_date'
]

BATCH_SIZE = 5000


def get_db_connection():
    """Connects to PostgreSQL using environment variables."""
    return psycopg2.connect(
        host=os.getenv('BOOKING_DB_HOST', 'localhost'),
        port=os.getenv('BOOKING_DB_PORT', '5434'),
        user=os.getenv('BOOKING_DB_USER', 'booking_user'),
        password=os.getenv('BOOKING_DB_PASSWORD', 'booking_pass'),
        dbname=os.getenv('BOOKING_DB_NAME', 'booking_db')
    )


def compute_row_hash(row):
    """Creates SHA-256 fingerprint of a row for deduplication."""
    row_string = '|'.join(str(row.get(col, '')) for col in EXPECTED_COLUMNS)
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()


def validate_schema(csv_columns):
    """Checks that the CSV has all expected columns."""
    missing = set(EXPECTED_COLUMNS) - set(csv_columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")
    extra = set(csv_columns) - set(EXPECTED_COLUMNS)
    if extra:
        logger.warning(f"CSV has extra columns (will be ignored): {extra}")
    logger.info(f"Schema validation passed: {len(EXPECTED_COLUMNS)} expected columns found")


def insert_batch(cursor, batch):
    """Inserts a batch of rows with ON CONFLICT DO NOTHING."""
    columns = EXPECTED_COLUMNS + ['_loaded_at', '_source_file', '_row_hash']
    col_names = ','.join(columns)

    query = f"""
        INSERT INTO bronze.raw_bookings ({col_names})
        VALUES %s
        ON CONFLICT (_row_hash) DO NOTHING
    """
    cursor.execute("SAVEPOINT batch_sp")
    try:
        execute_values(cursor, query, batch, page_size=len(batch))
        cursor.execute("RELEASE SAVEPOINT batch_sp")
        return len(batch), 0
    except Exception as e:
        cursor.execute("ROLLBACK TO SAVEPOINT batch_sp")
        logger.error(f"Batch insert failed: {e}")
        return 0, len(batch)


def ingest_csv_to_bronze(csv_path):
    """Main ingestion: CSV → bronze.raw_bookings"""
    source_file = os.path.basename(csv_path)
    logger.info(f"Starting Bronze ingestion from {source_file}")

    conn = get_db_connection()
    cur = conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        validate_schema(reader.fieldnames)

        batch = []
        total_inserted = 0
        total_duplicates = 0

        for row in reader:
            row_hash = compute_row_hash(row)
            values = [row.get(col, '') for col in EXPECTED_COLUMNS]
            values.extend([datetime.now(timezone.utc), source_file, row_hash])
            batch.append(tuple(values))

            if len(batch) >= BATCH_SIZE:
                inserted, dupes = insert_batch(cur, batch)
                total_inserted += inserted
                total_duplicates += dupes
                conn.commit()
                batch = []

        # Insert remaining rows
        if batch:
            inserted, dupes = insert_batch(cur, batch)
            total_inserted += inserted
            total_duplicates += dupes
            conn.commit()

    # Post-load verification
    cur.execute("SELECT COUNT(*) FROM bronze.raw_bookings")
    row_count = cur.fetchone()[0]
    logger.info(f"Bronze ingestion complete: {total_inserted} inserted, {total_duplicates} duplicates skipped")
    logger.info(f"Total rows in bronze.raw_bookings: {row_count}")

    if row_count < 80000:
        raise ValueError(f"Post-load check failed: only {row_count} rows (expected >= 80,000)")

    cur.close()
    conn.close()
    return {'inserted': total_inserted, 'duplicates': total_duplicates, 'total': row_count}


if __name__ == '__main__':
    csv_path = sys.argv[1] if len(sys.argv) > 1 else '/opt/airflow/data/raw/bookings.csv'
    result = ingest_csv_to_bronze(csv_path)
    logger.info(f"Result: {result}")
