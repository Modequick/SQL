--Dialect : Google Big Query
---Gaps and Islands is a concept in SQL used to identify sequential patterns in ordered data. It refers to the presence of gaps (missing values) and islands (contiguous groups of values) within a sequential data set ---

WITH
  selected_bike_id AS (
  SELECT
    '23277' AS bike_id ),
  base AS (
  SELECT
    DISTINCT bikeid,
    DATE(DATE_TRUNC(start_time, day)) AS date
  FROM
    `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE
    bikeid IN (
    SELECT
      *
    FROM
      selected_bike_id))


  -- We will use leads method (function to retrieve the value from a next row) and we will name it next_order_date and lags method (function to retrieve the value from a next row).
  -- By having previous_order_date we can create day_difference between previous order_date and date.
  -- If the difference between date and previous_order_date is >1 than there is a gap between that, and an island if otherwise
  
 , final_preparation AS (
  SELECT
    *,
    LEAD(date) OVER (PARTITION BY bikeid ORDER BY date ASC) AS next_order_date,
    LAG(date) OVER (PARTITION BY bikeid ORDER BY date ASC) AS previous_order_date
  FROM
    base
  ORDER BY
    bikeid,
    date ASC)


  #finding island
  ,island AS (
  SELECT
    bikeid,
    date,
    -- DATE_DIFF(next_order_date,date,day) AS lead_day_difference,
    -- DATE_DIFF(previous_order_date,date,day) as lag_day_difference
  FROM
    final_preparation
  WHERE
    DATE_DIFF(next_order_date,date,day) = 1
    OR DATE_DIFF(previous_order_date,date,day) = -1)

    
  #finding gaps
,gaps AS (
  SELECT
    bikeid,
    date,
  FROM
    final_preparation
  WHERE
    DATE_DIFF(next_order_date,date,day) > 1)
SELECT
  *
FROM
  island