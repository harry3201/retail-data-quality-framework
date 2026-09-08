"""
Retail Data Quality Framework
------------------------------
Validates the UCI Online Retail dataset (541,909 real transactions) with
PySpark, quarantines failing rows with a rule-level reason code instead of
dropping them, and produces a pass/fail quality report from an actual run.

Design decision: rules are split into HARD (row is quarantined) and SOFT
(row passes but is flagged) because not every anomaly is "bad data" -- a
missing CustomerID is a legitimate guest checkout, and a negative Quantity
is a legitimate return. Silently dropping those would throw away real
business events. Corrupted rows (duplicates, non-positive prices, unparseable
dates, fully-blank product identifiers) get quarantined because no
downstream aggregate should trust them.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

RAW_PATH = "data/online_retail_raw.csv"
CLEAN_OUT = "output/silver_valid_transactions"
QUARANTINE_OUT = "output/quarantined_records"
REPORT_OUT = "output/quality_report.json"

EXPECTED_SCHEMA = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", DoubleType(), True),
    StructField("Country", StringType(), True),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("RetailDataQualityFramework")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_raw(spark, path):
    return spark.read.csv(path, header=True, schema=EXPECTED_SCHEMA)


def apply_rules(df):
    """
    Tags every row with the quality rules it fails.
    HARD rules -> row is quarantined.
    SOFT rules -> row passes but is flagged for downstream awareness.
    """
    df = df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *df.columns), 256))

    # ---- HARD rules (quarantine) ----
    df = df.withColumn(
        "fail_non_positive_price",
        F.when(F.col("UnitPrice").isNull() | (F.col("UnitPrice") <= 0), F.lit(True)).otherwise(F.lit(False)),
    )
    df = df.withColumn(
        "fail_missing_identifiers",
        F.when(F.col("InvoiceNo").isNull() | F.col("StockCode").isNull(), F.lit(True)).otherwise(F.lit(False)),
    )
    df = df.withColumn(
        "fail_invalid_date",
        F.when(F.col("InvoiceDate").isNull(), F.lit(True)).otherwise(F.lit(False)),
    )

    # exact duplicate rows -> keep first occurrence, quarantine the rest
    from pyspark.sql import Window
    w = Window.partitionBy("_row_hash").orderBy(F.lit(1))
    df = df.withColumn("_dup_rank", F.row_number().over(w))
    df = df.withColumn("fail_duplicate_row", F.col("_dup_rank") > 1)

    # ---- SOFT rules (flag, don't quarantine) ----
    df = df.withColumn(
        "flag_missing_customer_id",
        F.when(F.col("CustomerID").isNull(), F.lit(True)).otherwise(F.lit(False)),
    )
    df = df.withColumn(
        "flag_non_positive_quantity",
        F.when(F.col("Quantity") <= 0, F.lit(True)).otherwise(F.lit(False)),
    )
    df = df.withColumn(
        "flag_non_product_stockcode",
        F.when(F.col("StockCode").rlike("^(POST|D|M|BANK CHARGES|DOT|CRUK|C2|PADS|AMAZONFEE)$"), F.lit(True)).otherwise(F.lit(False)),
    )

    df = df.withColumn(
        "is_quarantined",
        F.col("fail_non_positive_price") | F.col("fail_missing_identifiers")
        | F.col("fail_invalid_date") | F.col("fail_duplicate_row"),
    )

    hard_reason_cols = ["fail_non_positive_price", "fail_missing_identifiers", "fail_invalid_date", "fail_duplicate_row"]
    df = df.withColumn(
        "quarantine_reasons",
        F.array_join(
            F.array(*[F.when(F.col(c), F.lit(c.replace("fail_", ""))) for c in hard_reason_cols]),
            ",",
        ),
    )
    soft_reason_cols = ["flag_missing_customer_id", "flag_non_positive_quantity", "flag_non_product_stockcode"]
    df = df.withColumn(
        "quality_flags",
        F.array_join(
            F.array(*[F.when(F.col(c), F.lit(c.replace("flag_", ""))) for c in soft_reason_cols]),
            ",",
        ),
    )
    return df


def run():
    spark = build_spark()
    raw = load_raw(spark, RAW_PATH)
    total = raw.count()

    tagged = apply_rules(raw)
    tagged.cache()

    quarantined = tagged.filter(F.col("is_quarantined"))
    valid = tagged.filter(~F.col("is_quarantined"))

    quarantined_count = quarantined.count()
    valid_count = valid.count()

    # per-rule failure counts, computed from the actual run
    hard_rules = ["fail_non_positive_price", "fail_missing_identifiers", "fail_invalid_date", "fail_duplicate_row"]
    soft_rules = ["flag_missing_customer_id", "flag_non_positive_quantity", "flag_non_product_stockcode"]

    rule_counts = {}
    for r in hard_rules + soft_rules:
        rule_counts[r] = tagged.filter(F.col(r)).count()

    report = {
        "run_type": "batch",
        "source_file": RAW_PATH,
        "total_records": total,
        "valid_records": valid_count,
        "quarantined_records": quarantined_count,
        "quarantine_rate_pct": round(100 * quarantined_count / total, 3),
        "hard_rule_failures": {r: rule_counts[r] for r in hard_rules},
        "soft_rule_flags": {r: rule_counts[r] for r in soft_rules},
    }

    keep_cols = [c for c in EXPECTED_SCHEMA.fieldNames()] + ["quality_flags"]
    valid.select(*keep_cols).coalesce(1).write.mode("overwrite").option("header", True).csv(CLEAN_OUT)

    quarantine_cols = [c for c in EXPECTED_SCHEMA.fieldNames()] + ["quarantine_reasons"]
    quarantined.select(*quarantine_cols).coalesce(1).write.mode("overwrite").option("header", True).csv(QUARANTINE_OUT)

    import json
    with open(REPORT_OUT, "w") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    spark.stop()
    return report


if __name__ == "__main__":
    run()
