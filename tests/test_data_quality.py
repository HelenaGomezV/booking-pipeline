"""Tests for raw data quality checks."""

import csv
import os
import pytest

CSV_PATH = "data/raw/bookings.csv"


def load_rows():
    if not os.path.exists(CSV_PATH):
        pytest.skip("CSV file not found")
    with open(CSV_PATH) as f:
        return list(csv.DictReader(f))


class TestVolumeChecks:
    def test_minimum_row_count(self):
        """CSV has at least 80K rows."""
        rows = load_rows()
        assert len(rows) >= 80000, f"Only {len(rows)} rows"

    def test_not_empty(self):
        """CSV is not empty."""
        rows = load_rows()
        assert len(rows) > 0


class TestAcceptedValues:
    def test_hotel_types(self):
        """Only Hotel and Apartment."""
        rows = load_rows()
        types = set(r["hotel"] for r in rows)
        assert types == {"Hotel", "Apartment"}, f"Unexpected types: {types}"

    def test_is_canceled_binary(self):
        """is_canceled is only 0 or 1."""
        rows = load_rows()
        values = set(r["is_canceled"] for r in rows)
        assert values == {"0", "1"}, f"Unexpected values: {values}"


class TestDataDistribution:
    def test_cancellation_rate_reasonable(self):
        """Cancellation rate between 10% and 50%."""
        rows = load_rows()
        canceled = sum(1 for r in rows if r["is_canceled"] == "1")
        rate = canceled / len(rows) * 100
        assert 10 <= rate <= 50, f"Cancellation rate {rate:.1f}% outside expected range"

    def test_country_diversity(self):
        """At least 50 different countries."""
        rows = load_rows()
        countries = set(r["country"] for r in rows if r["country"] != "NULL")
        assert len(countries) >= 50, f"Only {len(countries)} countries"

    def test_adr_mostly_positive(self):
        """Less than 1% of ADR values are negative."""
        rows = load_rows()
        negative = sum(1 for r in rows if float(r["adr"]) < 0)
        pct = negative / len(rows) * 100
        assert pct < 1, f"{pct:.2f}% negative ADR values"
