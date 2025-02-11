# F1 Lap Time Analysis using BigQuery

This guide explains how to process F1 drivers' lap time data, calculate the average lap time for each driver, and determine the top three drivers in ascending order using Google BigQuery. This is a batch process, and it assumes you have a CSV file with lap time data.

## Getting Started

### Prerequisites

Before you begin, make sure you have the following:

- A Google Cloud Project with BigQuery enabled.
- The BigQuery command-line tool (`bq`) installed and authenticated.
- A CSV file with F1 lap time data.

### Steps

**Step 1: Create a BigQuery Table**

Run the following SQL command to create a BigQuery table for your lap time data. Replace `your_project_id` and `your_dataset_id` with your actual project and dataset information.

```sql
CREATE OR REPLACE TABLE your_project_id.your_dataset_id.f1_lap_times (
  Driver STRING,
  Time FLOAT64
);
```

**Step 2: Upload CSV Data to the BigQuery Table**
Use the `bq` command-line tool to load your CSV data into the BigQuery table:

```sh
bq load --autodetect --source_format=CSV your_project_id:your_dataset_id.f1_lap_times f1_lap_times.csv
```

**Step 3: Calculate Average Lap Time**
Run the following SQL query to calculate the average and fastest lap time for each driver:

```sql
SELECT
  Driver,
  AVG(Time) AS AvgLapTime,
  MIN(Time) AS FastestLapTime
FROM
  your_project_id.your_dataset_id.f1_lap_times
GROUP BY
  Driver;
```

**Step 4: Sort Drivers by Average Lap Time**

Execute this SQL query to sort drivers by their average lap time:

```sql
SELECT
  Driver,
  AvgLapTime,
  FastestLapTime
FROM (
  SELECT
    Driver,
    AVG(Time) AS AvgLapTime,
    MIN(Time) AS FastestLapTime
  FROM
    your_project_id.your_dataset_id.f1_lap_times
  GROUP BY
    Driver
)
ORDER BY
  AvgLapTime;
```

**Step 5: Select the Top 3 Drivers**

```sql
WITH DriverAverages AS (
  SELECT
    Driver,
    AVG(Time) AS AvgLapTime,
    MIN(Time) AS FastestLapTime
  FROM
    your_project_id.your_dataset_id.f1_lap_times
  GROUP BY
    Driver
)
SELECT
  Driver,
  AvgLapTime,
  FastestLapTime
FROM (
  SELECT
    Driver,
    AvgLapTime,
    FastestLapTime,
    RANK() OVER (ORDER BY AvgLapTime ASC, FastestLapTime ASC) AS LapTimeRank
  FROM DriverAverages
)
WHERE LapTimeRank <= 3;
```

**Step 6: Generate Output**
You can export the results in your desired format. For example, to export as CSV:

```sh
bq query --use_legacy_sql=false --format=csv --nouse_cache --nouse_legacy_sql "Query from step 5" > output.csv

```

These steps can be manually run or they can be orchestrated using airflow dag and the operators. 
