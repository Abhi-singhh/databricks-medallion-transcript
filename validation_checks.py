"""
validation_checks.py

Schema and data-quality checks for the bronze -> silver -> gold ->
derivative medallion pipeline.

These are PySpark assertions, not a generic library — each check
targets a real, specific risk in THIS pipeline:

  - bronze:     did ingestion actually produce rows, and is the
                traceability hash (uuid) unique and non-null?
  - silver:     did ai_classify() return one of the categories we
                actually asked it for? An LLM-backed classifier can
                drift or return something unexpected, and nothing in
                the current pipeline would catch that.
  - gold:       same category-drift risk for the appliance
                classification, plus: does every gold row have a
                valid partition date, and is uuid_gold unique?
  - derivative: do the three aggregate tables (by appliance, by
                sentiment, by call type) each sum back to the same
                row count as the gold slice they were built from?
                A groupBy silently drops rows with a null grouping
                key, which this check would catch.

Each function returns a ValidationReport. Call `.raise_if_failed()`
to turn failures into a real exception (what you'd want at the end
of a notebook cell, right before a `.write`), or inspect
`.failures` yourself for logging.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ValidationReport:
    layer: str
    checks_run: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0

    def raise_if_failed(self) -> None:
        if not self.passed:
            failure_text = "\n  - " + "\n  - ".join(self.failures)
            raise AssertionError(
                f"Validation failed for '{self.layer}' layer:{failure_text}"
            )


def _check_row_count_positive(df: DataFrame, report: ValidationReport) -> None:
    report.checks_run.append("row_count_positive")
    n = df.count()
    if n == 0:
        report.failures.append("DataFrame has 0 rows — ingestion or upstream read produced nothing")


def _check_column_not_null(df: DataFrame, col: str, report: ValidationReport) -> None:
    report.checks_run.append(f"{col}_not_null")
    null_count = df.filter(F.col(col).isNull()).count()
    if null_count > 0:
        report.failures.append(f"'{col}' has {null_count} null value(s)")


def _check_column_unique(df: DataFrame, col: str, report: ValidationReport) -> None:
    report.checks_run.append(f"{col}_unique")
    total = df.count()
    distinct = df.select(col).distinct().count()
    if distinct != total:
        report.failures.append(
            f"'{col}' is not unique: {total} rows but only {distinct} distinct values "
            f"({total - distinct} duplicate(s))"
        )


def _check_column_in_allowed_set(
    df: DataFrame, col: str, allowed: set[str], report: ValidationReport
) -> None:
    """
    The core check for ai_classify() output drift: confirms every
    non-null value in `col` is one of the categories we actually
    passed to ai_classify(). An unexpected value means the model
    returned something outside the requested label set.
    """
    report.checks_run.append(f"{col}_in_allowed_categories")
    unexpected = (
        df.filter(F.col(col).isNotNull() & ~F.col(col).isin(list(allowed)))
        .select(col)
        .distinct()
        .rdd.flatMap(lambda row: row)
        .collect()
    )
    if unexpected:
        report.failures.append(
            f"'{col}' contains value(s) outside the expected set {sorted(allowed)}: {unexpected}"
        )


def validate_bronze(df: DataFrame) -> ValidationReport:
    report = ValidationReport(layer="bronze")
    _check_row_count_positive(df, report)
    _check_column_not_null(df, "uuid", report)
    _check_column_unique(df, "uuid", report)
    _check_column_not_null(df, "insertion_time", report)
    return report


def validate_silver(df: DataFrame) -> ValidationReport:
    report = ValidationReport(layer="silver")
    _check_row_count_positive(df, report)
    _check_column_not_null(df, "uuid_silver", report)
    _check_column_in_allowed_set(
        df, "call_type", {"complaint", "order placement", "customer query"}, report
    )
    _check_column_in_allowed_set(
        df, "sentiment", {"positive", "neutral", "negative", "angry", "happy"}, report
    )
    return report


def validate_gold(df: DataFrame, expected_row_count: int | None = None) -> ValidationReport:
    report = ValidationReport(layer="gold")
    _check_row_count_positive(df, report)
    _check_column_not_null(df, "uuid_gold", report)
    _check_column_unique(df, "uuid_gold", report)
    _check_column_not_null(df, "date", report)
    _check_column_in_allowed_set(
        df,
        "appliances",
        {"kitchen", "home", "tech electronic", "software", "bathroom", "sports"},
        report,
    )
    if expected_row_count is not None:
        report.checks_run.append("row_count_matches_upstream")
        actual = df.count()
        if actual != expected_row_count:
            report.failures.append(
                f"gold row count ({actual}) does not match expected upstream "
                f"count ({expected_row_count}) — rows may have been silently "
                f"dropped or duplicated"
            )
    return report


def validate_derivative_totals(
    gold_slice_count: int,
    count_appliances_df: DataFrame,
    count_sentiment_df: DataFrame,
    count_calltype_df: DataFrame,
) -> ValidationReport:
    """
    Each derivative table is a groupBy(...).count() over the same
    gold slice. If any grouping key is null, that row is silently
    dropped from the aggregate — this check confirms nothing was
    lost by comparing each aggregate's summed count back to the
    original slice size.
    """
    report = ValidationReport(layer="derivative")

    for name, agg_df in [
        ("count_appliances", count_appliances_df),
        ("count_sentiment", count_sentiment_df),
        ("count_calltype", count_calltype_df),
    ]:
        report.checks_run.append(f"{name}_sums_to_gold_slice")
        summed = agg_df.agg(F.sum("count").alias("total")).collect()[0]["total"] or 0
        if summed != gold_slice_count:
            report.failures.append(
                f"{name} sums to {summed}, but the gold slice it was built from "
                f"has {gold_slice_count} rows — {gold_slice_count - summed} row(s) "
                f"were likely dropped due to a null grouping key"
            )

    return report
