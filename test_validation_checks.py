"""
test_validation_checks.py

Local tests for validation_checks.py, run with a local SparkSession
against synthetic data shaped like the real bronze/silver/gold/
derivative tables. This does not call ai_extract/ai_classify (those
only exist inside a Databricks SQL warehouse) — instead it tests
that the validation logic itself correctly catches bad data, by
constructing DataFrames as if those functions had already run.

Run with: pytest test_validation_checks.py -v
"""

import pytest
from pyspark.sql import SparkSession

from validation_checks import (
    validate_bronze,
    validate_silver,
    validate_gold,
    validate_derivative_totals,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("validation-checks-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------

def test_bronze_passes_with_clean_data(spark):
    df = spark.createDataFrame(
        [("hash1", "2026-09-08 00:00:00"), ("hash2", "2026-09-08 00:01:00")],
        ["uuid", "insertion_time"],
    )
    report = validate_bronze(df)
    assert report.passed, report.failures


def test_bronze_fails_on_duplicate_uuid(spark):
    df = spark.createDataFrame(
        [("hash1", "2026-09-08 00:00:00"), ("hash1", "2026-09-08 00:01:00")],
        ["uuid", "insertion_time"],
    )
    report = validate_bronze(df)
    assert not report.passed
    assert any("uuid" in f and "not unique" in f for f in report.failures)


def test_bronze_fails_on_null_uuid(spark):
    df = spark.createDataFrame(
        [(None, "2026-09-08 00:00:00")],
        "uuid string, insertion_time string",
    )
    report = validate_bronze(df)
    assert not report.passed
    assert any("uuid" in f and "null" in f for f in report.failures)


def test_bronze_fails_on_empty_dataframe(spark):
    df = spark.createDataFrame([], "uuid string, insertion_time string")
    report = validate_bronze(df)
    assert not report.passed
    assert any("0 rows" in f for f in report.failures)


# ---------------------------------------------------------------------
# Silver — the ai_classify() category-drift check
# ---------------------------------------------------------------------

def test_silver_passes_with_expected_categories(spark):
    df = spark.createDataFrame(
        [
            ("u1", "complaint", "angry"),
            ("u2", "order placement", "neutral"),
        ],
        ["uuid_silver", "call_type", "sentiment"],
    )
    report = validate_silver(df)
    assert report.passed, report.failures


def test_silver_fails_when_ai_classify_returns_unexpected_call_type(spark):
    # Simulates the exact risk this check exists for: ai_classify()
    # returning something outside the requested label set.
    df = spark.createDataFrame(
        [("u1", "refund request", "angry")],  # "refund request" was never a valid label
        ["uuid_silver", "call_type", "sentiment"],
    )
    report = validate_silver(df)
    assert not report.passed
    assert any("call_type" in f and "refund request" in f for f in report.failures)


def test_silver_fails_when_sentiment_outside_allowed_set(spark):
    df = spark.createDataFrame(
        [("u1", "complaint", "frustrated")],  # not one of the 5 allowed sentiments
        ["uuid_silver", "call_type", "sentiment"],
    )
    report = validate_silver(df)
    assert not report.passed
    assert any("sentiment" in f for f in report.failures)


# ---------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------

def test_gold_passes_with_clean_data(spark):
    df = spark.createDataFrame(
        [
            ("g1", "2026-09-08", "kitchen"),
            ("g2", "2026-09-08", "tech electronic"),
        ],
        ["uuid_gold", "date", "appliances"],
    )
    report = validate_gold(df, expected_row_count=2)
    assert report.passed, report.failures


def test_gold_fails_on_unexpected_appliance_category(spark):
    df = spark.createDataFrame(
        [("g1", "2026-09-08", "garden")],  # not in the 6 allowed categories
        ["uuid_gold", "date", "appliances"],
    )
    report = validate_gold(df)
    assert not report.passed
    assert any("appliances" in f and "garden" in f for f in report.failures)


def test_gold_fails_when_row_count_does_not_match_upstream(spark):
    df = spark.createDataFrame(
        [("g1", "2026-09-08", "kitchen")],
        ["uuid_gold", "date", "appliances"],
    )
    # simulate silver having 200 rows but gold only got 1 through
    report = validate_gold(df, expected_row_count=200)
    assert not report.passed
    assert any("row count" in f for f in report.failures)


# ---------------------------------------------------------------------
# Derivative — aggregate completeness check
# ---------------------------------------------------------------------

def test_derivative_passes_when_totals_match(spark):
    appliances = spark.createDataFrame([("kitchen", 3), ("home", 2)], ["appliances", "count"])
    sentiment = spark.createDataFrame([("angry", 5)], ["sentiment", "count"])
    calltype = spark.createDataFrame([("complaint", 5)], ["call_type", "count"])

    report = validate_derivative_totals(
        gold_slice_count=5,
        count_appliances_df=appliances,
        count_sentiment_df=sentiment,
        count_calltype_df=calltype,
    )
    assert report.passed, report.failures


def test_derivative_fails_when_groupby_silently_dropped_rows(spark):
    # gold slice has 5 rows, but appliances aggregate only sums to 4 —
    # simulates a null "appliances" value being silently dropped by groupBy
    appliances = spark.createDataFrame([("kitchen", 3), ("home", 1)], ["appliances", "count"])
    sentiment = spark.createDataFrame([("angry", 5)], ["sentiment", "count"])
    calltype = spark.createDataFrame([("complaint", 5)], ["call_type", "count"])

    report = validate_derivative_totals(
        gold_slice_count=5,
        count_appliances_df=appliances,
        count_sentiment_df=sentiment,
        count_calltype_df=calltype,
    )
    assert not report.passed
    assert any("count_appliances" in f for f in report.failures)
