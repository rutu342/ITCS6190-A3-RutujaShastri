
"""Assignment 3: Trip Analytics with Spark.

Usage (on the Docker cluster):
    spark-submit main.py <input directory> <output directory>

Reads trips.csv and zones.csv from the input directory. Writes the cleaned trips as Parquet
to <output>/clean_trips and one CSV result per part to <output>/part1, part2a, part2b, part3.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as ssum, avg, round as sround, to_date,
    desc, asc, row_number, expr, when, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType
)
from pyspark.sql.window import Window

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)

in_dir, out_dir = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")

spark = SparkSession.builder.appName("TripAnalytics").getOrCreate()


def save(df, name, n=20):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return

    print(f"\n=== {name} ===")
    df.show(n, truncate=False)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{out_dir}/{name}"
    )


# ---------------------------------------------------------------- Load

# Keep fare as a string so invalid values remain visible and countable.
trips_schema = StructType([
    StructField("trip_id", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("pickup_zone", IntegerType(), False),
    StructField("dropoff_zone", IntegerType(), False),
    StructField("pickup_ts", TimestampType(), False),
    StructField("dropoff_ts", TimestampType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("fare", StringType(), True),
    StructField("payment_type", StringType(), False),
])

zones_schema = "zone_id INT, zone_name STRING, borough STRING"

raw = spark.read.csv(
    f"{in_dir}/trips.csv", header=True, schema=trips_schema
)
zones = spark.read.csv(
    f"{in_dir}/zones.csv", header=True, schema=zones_schema
)

raw.printSchema()


# ---------------------------------------------------------------- Part 1: clean

def part1_clean(raw):
    """Remove invalid trips and write the clean ones to Parquet.

    Rule 1: dropoff_ts must be after pickup_ts.
    Rule 2: fare must be present and numeric.
    Returns clean trips and a one-row DataFrame containing the five required counts.
    """

    # Safely convert fare. Invalid strings become null under try_cast.
    checked = raw.withColumn(
        "_fare_double",
        expr("try_cast(fare AS DOUBLE)")
    )

    # Mark each invalidity rule independently.
    checked = (
        checked
        .withColumn(
            "_bad_time",
            col("dropoff_ts") <= col("pickup_ts")
        )
        .withColumn(
            "_bad_fare",
            col("_fare_double").isNull()
        )
    )

    # Count both kinds of invalidity separately.
    # Rows violating both rules count only once in "removed".
    counts = checked.agg(
        count("*").alias("rows_total"),
        ssum(
            when(col("_bad_time"), 1).otherwise(0)
        ).alias("bad_time"),
        ssum(
            when(col("_bad_fare"), 1).otherwise(0)
        ).alias("bad_fare"),
        ssum(
            when(
                col("_bad_time") | col("_bad_fare"), 1
            ).otherwise(0)
        ).alias("removed"),
        ssum(
            when(
                ~(col("_bad_time") | col("_bad_fare")), 1
            ).otherwise(0)
        ).alias("rows_kept")
    )

    # Keep only valid rows and replace the original string fare with DOUBLE.
    clean = (
        checked
        .filter(
            ~(col("_bad_time") | col("_bad_fare"))
        )
        .withColumn("fare", col("_fare_double"))
        .drop("_fare_double", "_bad_time", "_bad_fare")
        .withColumn("day", to_date("pickup_ts"))
        .withColumn(
            "duration_min",
            (
                unix_timestamp("dropoff_ts")
                - unix_timestamp("pickup_ts")
            ) / 60.0
        )
    )

    # Save clean trips as Parquet.
    clean.write.mode("overwrite").parquet(
        f"{out_dir}/clean_trips"
    )

    return clean, counts


# ---------------------------------------------------------------- Part 2a: zone revenue per day

def part2a_zone_revenue(clean, zones):
    """Trips and revenue per pickup zone per day.

    Columns: day, zone_id, zone_name, trips, revenue.
    Ordered by day, then zone_id.
    """

    daily_zone = (
        clean
        .groupBy(
            col("day"),
            col("pickup_zone").alias("zone_id")
        )
        .agg(
            count("*").alias("trips"),
            sround(ssum("fare"), 2).alias("revenue")
        )
    )

    result = (
        daily_zone
        .join(
            zones,
            daily_zone["zone_id"] == zones["zone_id"],
            "inner"
        )
        .select(
            daily_zone["day"],
            daily_zone["zone_id"],
            zones["zone_name"],
            daily_zone["trips"],
            daily_zone["revenue"]
        )
        .orderBy(
            asc("day"),
            asc("zone_id")
        )
    )

    return result


# ---------------------------------------------------------------- Part 2b: driver busiest day

def part2b_driver_busiest_day(clean):
    """Each driver's busiest day.

    Columns: driver_id, day, trips.
    Ties are resolved by choosing the earliest day.
    Ordered by driver_id.
    """

    daily_driver = (
        clean
        .groupBy("driver_id", "day")
        .agg(count("*").alias("trips"))
    )

    window_spec = Window.partitionBy("driver_id").orderBy(
        desc("trips"),
        asc("day")
    )

    result = (
        daily_driver
        .withColumn(
            "_row_number",
            row_number().over(window_spec)
        )
        .filter(col("_row_number") == 1)
        .select("driver_id", "day", "trips")
        .orderBy("driver_id")
    )

    return result


# ---------------------------------------------------------------- Part 3: Spark SQL

def part3_borough_sql(clean, zones):
    """Activity per pickup borough using one Spark SQL query."""

    # Register temporary views for this Spark session.
    clean.createOrReplaceTempView("trips")
    zones.createOrReplaceTempView("zones")

    # One SQL query: join, aggregate, round, and order.
    result = spark.sql("""
        SELECT
            z.borough AS borough,
            COUNT(*) AS trips,
            ROUND(SUM(t.fare), 2) AS revenue,
            ROUND(AVG(t.distance_km), 2) AS avg_distance_km
        FROM trips AS t
        INNER JOIN zones AS z
            ON t.pickup_zone = z.zone_id
        GROUP BY z.borough
        ORDER BY revenue DESC
    """)

    return result


# ---------------------------------------------------------------- Run all parts

clean, counts = part1_clean(raw)
save(counts, "part1")

zone_rev = part2a_zone_revenue(clean, zones) if clean is not None else None
save(zone_rev, "part2a")

driver_day = (
    part2b_driver_busiest_day(clean)
    if clean is not None else None
)
save(driver_day, "part2b")

borough = (
    part3_borough_sql(clean, zones)
    if clean is not None else None
)
save(borough, "part3")

# Print physical plans for the required report comparison.
if borough is not None and zone_rev is not None:
    print("\n=== Physical plan of part 2a (DataFrame API) ===")
    zone_rev.explain()

    print("\n=== Physical plan of part 3 (Spark SQL) ===")
    borough.explain()

spark.stop()
