"""A realistic PySpark data-engineering job.

Generates a batch of synthetic card transactions, then aggregates them two ways:
a per-category summary (volume, total amount, approval rate) and a daily
settlement total. Results are printed so they show up in the Pipekit run logs.

The data generation and the aggregation math live in plain Python functions
(`make_transactions`, `summarize`, `daily_settlement`) so a data engineer can
unit-test the logic in milliseconds without a Spark cluster (see
`test_txn_aggregate.py`). `main()` runs the same aggregation in Spark at
scale on the cluster.
"""

import random

CATEGORIES = ["groceries", "fuel", "travel", "dining", "online", "utilities"]
DAYS = ["2026-06-08", "2026-06-09", "2026-06-10"]


def make_transactions(n, seed=42):
    """Return n synthetic transactions as a list of dicts. Deterministic for a seed."""
    rng = random.Random(seed)
    transactions = []
    for _ in range(n):
        category = rng.choice(CATEGORIES)
        transactions.append(
            {
                "category": category,
                "amount": round(rng.uniform(5.0, 500.0), 2),
                "approved": rng.random() > 0.08,
                "day": rng.choice(DAYS),
            }
        )
    return transactions


def summarize(transactions):
    """Per-category volume, total amount, and approval rate.

    Returns a list of dicts sorted by category. This is the reference the unit
    test asserts against and the same shape `main()` computes in Spark.
    """
    by_category = {}
    for txn in transactions:
        bucket = by_category.setdefault(
            txn["category"], {"count": 0, "approved": 0, "total_amount": 0.0}
        )
        bucket["count"] += 1
        bucket["approved"] += 1 if txn["approved"] else 0
        bucket["total_amount"] += txn["amount"]

    summary = []
    for category in sorted(by_category):
        bucket = by_category[category]
        summary.append(
            {
                "category": category,
                "count": bucket["count"],
                "total_amount": round(bucket["total_amount"], 2),
                "approval_rate": round(bucket["approved"] / bucket["count"], 4),
            }
        )
    return summary


def daily_settlement(transactions):
    """Total approved amount per day, sorted by day."""
    by_day = {}
    for txn in transactions:
        if txn["approved"]:
            by_day[txn["day"]] = round(by_day.get(txn["day"], 0.0) + txn["amount"], 2)
    return [{"day": day, "settled_amount": by_day[day]} for day in sorted(by_day)]


def main():
    # Imported here so the pure functions above (and their tests) do not require
    # pyspark or a JVM.
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    # Default shuffle partitions (200) spawns 200 tasks for a tiny aggregation, which
    # is pure scheduling overhead on a small cluster. Match it to the data instead.
    spark = (
        SparkSession.builder.appName("txn-aggregate")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    transactions = make_transactions(n=1_000)
    df = spark.createDataFrame(transactions)

    category_summary = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg(F.col("approved").cast("int")), 4).alias("approval_rate"),
        )
        .orderBy("category")
    )

    settlement = (
        df.filter(F.col("approved"))
        .groupBy("day")
        .agg(F.round(F.sum("amount"), 2).alias("settled_amount"))
        .orderBy("day")
    )

    print("=== Transactions by category ===")
    category_summary.show(truncate=False)
    print("=== Daily settlement (approved only) ===")
    settlement.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
