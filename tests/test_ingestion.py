"""Tests for the Bronze ingestion logic."""
import hashlib
import pytest
import csv
import os


# === Replicate the hash function from ingestion.py ===
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


def compute_row_hash(row):
    row_string = '|'.join(str(row.get(col, '')) for col in EXPECTED_COLUMNS)
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()


class TestRowHash:
    def test_hash_is_deterministic(self):
        """Same row always produces same hash."""
        row = {'hotel': 'Hotel', 'is_canceled': '0', 'adr': '100.50'}
        assert compute_row_hash(row) == compute_row_hash(row)

    def test_different_rows_different_hash(self):
        """Different rows produce different hashes."""
        row1 = {'hotel': 'Hotel', 'adr': '100'}
        row2 = {'hotel': 'Apartment', 'adr': '200'}
        assert compute_row_hash(row1) != compute_row_hash(row2)

    def test_hash_is_sha256_format(self):
        """Hash is 64 hex characters (SHA-256)."""
        row = {'hotel': 'Hotel'}
        h = compute_row_hash(row)
        assert len(h) == 64
        assert all(c in '0123456789abcdef' for c in h)


class TestSchemaValidation:
    def test_csv_has_expected_columns(self):
        """The raw CSV has all 32 expected columns."""
        csv_path = 'data/raw/bookings.csv'
        if not os.path.exists(csv_path):
            pytest.skip("CSV file not found")
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            csv_columns = set(reader.fieldnames)
        missing = set(EXPECTED_COLUMNS) - csv_columns
        assert len(missing) == 0, f"Missing columns: {missing}"

    def test_expected_columns_count(self):
        """We expect exactly 32 columns."""
        assert len(EXPECTED_COLUMNS) == 32

    def test_no_duplicate_column_names(self):
        """Column names are unique."""
        assert len(EXPECTED_COLUMNS) == len(set(EXPECTED_COLUMNS))