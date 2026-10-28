# Assignment #3: Report

**Name:** Rutuja Hemant Shastri\
**Student ID:** 801484366\
**Email:** rshastri@charlotte.edu

------------------------------------------------------------------------

## Seed and commands

Seed used for `datagen.py`: **801484366**

The commands I ran, in order:

``` bash
python3 datagen.py 801484366
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
```

The Spark application completed successfully and generated the output
for all four parts.

------------------------------------------------------------------------

## Part 1: load and clean

### Implementation

Rule 1 removes trips where `dropoff_ts` is not later than `pickup_ts`,
using the condition `dropoff_ts <= pickup_ts`.

Rule 2 removes trips whose fare is missing or cannot be converted to a
number. I used `try_cast(fare AS DOUBLE)` and removed rows where the
converted fare was null. I counted the two invalidity rules
independently, while counting a row only once in `removed` if it
violated either rule.

I read `fare` as a string so invalid input values remain visible and
countable. If it were read directly as a `DOUBLE`, empty or nonnumeric
values could be converted to null during CSV loading, making it harder
to distinguish and report the invalid input values. With Spark 4 ANSI
behavior, a regular cast of invalid numeric text can also raise an
error; `try_cast` returns null instead.

### The five counts

    `rows_total`   `bad_time`   `bad_fare`   `removed`   `rows_kept`
  -------------- ------------ ------------ ----------- -------------
          100000         1460         1523        2983         97017

### Examples of bad rows from `trips.csv`

Empty fare --- `T000020`:

``` text
T000020,D048,7,14,2026-09-23 07:13:03,2026-09-23 07:38:54,4.89,,cash
```

Empty fare --- `T000028`:

``` text
T000028,D032,5,4,2026-09-13 08:19:30,2026-09-13 08:30:52,1.98,,card
```

Drop-off timestamp before pickup --- `T000071`:

``` text
T000071,D160,12,3,2026-09-14 08:52:23,2026-09-14 08:43:13,2.54,10.67,card
```

------------------------------------------------------------------------

## Part 2a: trips and revenue per zone per day

I grouped cleaned trips by pickup date and pickup zone, calculated the
trip count and total fare, joined the zone names, rounded revenue to two
decimal places, and ordered the results by day and zone ID.

First ten rows:

``` text
day,zone_id,zone_name,trips,revenue
2026-09-01,1,Uptown Center,273,5519.45
2026-09-01,2,First Ward,284,5507.13
2026-09-01,3,Fourth Ward,172,3282.23
2026-09-01,4,South End,287,5458.42
2026-09-01,5,Dilworth,147,2879.23
2026-09-01,6,Plaza Midwood,126,2482.69
2026-09-01,7,NoDa,203,3902.79
2026-09-01,8,University City,225,4344.61
2026-09-01,9,Ballantyne,129,2458.51
2026-09-01,10,SouthPark,147,2964.49
```

These rows show that trip counts and revenue vary by pickup zone. On
September 1, South End had 287 trips and First Ward had 284 trips.

------------------------------------------------------------------------

## Part 2b: each driver's busiest day

I first counted each driver's trips per day. I used `row_number()` with
a window partitioned by driver and ordered by trip count descending,
then day ascending. This returns one busiest day per driver and chooses
the earliest day in the event of a tie.

First ten rows:

``` text
driver_id,day,trips
D001,2026-09-01,21
D002,2026-09-27,19
D003,2026-09-30,29
D004,2026-09-22,19
D005,2026-09-09,21
D006,2026-09-04,21
D007,2026-09-02,17
D008,2026-09-25,20
D009,2026-09-15,20
D010,2026-09-04,21
```

The result contains one busiest day for each driver. For example, driver
D003 had 29 trips on September 30.

------------------------------------------------------------------------

## Part 3: activity per borough (Spark SQL)

I registered the cleaned trips and zones as temporary views named
`trips` and `zones`. I used one SQL query to join trips to their pickup
zones, aggregate by borough, calculate trip count, revenue and average
distance, and order by revenue descending.

### SQL query

``` sql
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
```

### Result

``` text
borough,trips,revenue,avg_distance_km
Central,38679,764657.52,5.36
South,17952,356860.7,5.39
North,15837,314390.28,5.38
West,12411,243107.68,5.30
East,12138,240099.06,5.37
```

Central had the highest number of trips and the highest revenue among
the five boroughs. The trip counts across the five boroughs sum to
97,017, matching the number of cleaned trips.

------------------------------------------------------------------------

## The two plans

**Note:** The blocks below are concise transcriptions of the physical
plans from the Spark run, not verbatim full `explain()` output. They
show the principal operators and their relationships.

### `explain()` output of Part 2a (DataFrame API)

``` text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day ASC NULLS FIRST, zone_id ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day ASC NULLS FIRST, zone_id ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS
      +- Project [day, zone_id, zone_name, trips, revenue]
         +- BroadcastHashJoin [zone_id], [zone_id], Inner, BuildRight
            :- HashAggregate(keys=[day, pickup_zone], functions=[count(1), sum(fare)])
            :  +- Exchange hashpartitioning(day, pickup_zone, 200), ENSURE_REQUIREMENTS
            :     +- HashAggregate(keys=[day, pickup_zone], functions=[partial_count(1), partial_sum(fare)])
            :        +- Project [pickup_zone, try_cast(fare as double) AS fare, cast(pickup_ts as date) AS day]
            :           +- Filter [valid timestamps and numeric fare]
            :              +- FileScan csv [pickup_zone, pickup_ts, dropoff_ts, fare] from trips.csv
            +- BroadcastExchange
               +- Filter isnotnull(zone_id)
                  +- FileScan csv [zone_id, zone_name] from zones.csv
```

### `explain()` output of Part 3 (Spark SQL)

``` text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue DESC NULLS LAST, 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[borough], functions=[count(1), sum(fare), avg(distance_km)])
         +- Exchange hashpartitioning(borough, 200), ENSURE_REQUIREMENTS
            +- HashAggregate(keys=[borough], functions=[partial_count(1), partial_sum(fare), partial_avg(distance_km)])
               +- Project [distance_km, fare, borough]
                  +- BroadcastHashJoin [pickup_zone], [zone_id], Inner, BuildRight
                     :- Project [pickup_zone, distance_km, try_cast(fare as double) AS fare]
                     :  +- Filter [valid timestamps and numeric fare]
                     :     +- FileScan csv [pickup_zone, pickup_ts, dropoff_ts, distance_km, fare] from trips.csv
                     +- BroadcastExchange
                        +- Filter isnotnull(zone_id)
                           +- FileScan csv [zone_id, borough] from zones.csv
```

### Comparison

Both plans scan the trips and zones CSV files. In each plan, the trips
scan applies filters for valid timestamps and numeric fares. Both use an
inner `BroadcastHashJoin`, with the zones data on the build-right side.
Each plan uses partial and final hash aggregation, with a
hash-partitioning `Exchange` between the partial and final aggregation
stages to redistribute records by the grouping keys. Each plan also uses
a range-partitioning `Exchange` before the final `Sort` to meet its
ordering requirement.

The main differences are the grouping keys and sort columns. Part 2a
groups by pickup date and pickup zone, then sorts by date and zone ID.
Part 3 groups by borough and calculates trip count, revenue, and average
distance, then sorts by revenue descending. Although the queries were
written using different interfaces, Spark's Catalyst optimizer produces
similar physical operators for the joins and aggregations.

------------------------------------------------------------------------

## At scale

For 200 million trips, I would keep Spark DataFrame transformations,
`try_cast`-based validation, distributed aggregations, the window
function, and the SQL query. I would first avoid forcing large results
into one output file with `coalesce(1)`, because this can create a
single-task bottleneck. I would store cleaned data as Parquet and
consider partitioning it by a commonly filtered column such as `day` so
queries can skip irrelevant partitions. I would evaluate caching or
reusing the cleaned dataset because multiple analyses use it, while
checking memory pressure and Spark UI metrics. I would also inspect the
physical plans and Spark UI for expensive shuffles and data skew before
making further tuning changes.

------------------------------------------------------------------------

## Problems and fixes

When starting the cluster, existing containers named `spark-master`,
`spark-worker-1`, and `spark-worker-2` caused a container-name conflict.
I removed the old containers and restarted the cluster using Docker
Compose. The Spark application then completed successfully and generated
all four result outputs. The exact Docker error text was not retained in
my notes.
