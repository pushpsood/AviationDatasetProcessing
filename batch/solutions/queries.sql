--  Optimizations

SET hive.cli.print.header=true;

SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
ANALYZE TABLE aviation PARTITION (Yeard, Monthd) COMPUTE STATISTICS;
ANALYZE TABLE aviation PARTITION (Yeard, Monthd) COMPUTE STATISTICS FOR COLUMNS;

SET dynamodb.throughput.read.percent=1.0;
SET dynamodb.throughput.write.percent=1.0;
SET dynamodb.max.map.tasks=50;
SET mapred.reduce.tasks=50;
SET hive.exec.reducers.max=50;


SELECT
  o.origin AS airport,
  o.total_departing_flights + d.total_arriving_flights AS total_flights
FROM
  (SELECT origin, count(flightnum) AS total_departing_flights FROM aviation GROUP BY origin) o
  JOIN
  (SELECT dest, count(flightnum) AS total_arriving_flights FROM aviation GROUP BY dest) d
  ON o.origin = d.dest
ORDER BY total_flights DESC
LIMIT 10;


SELECT
  uniquecarrier AS airline,
  round(avg(ArrDelay),2) as avg_delay
FROM aviation
WHERE cancelled = 0
GROUP BY uniquecarrier
ORDER BY avg_delay
LIMIT 10;


SELECT
  DayOfWeek AS Weekday,
  round(avg(ArrDelay),2) as avg_delay
FROM aviation
WHERE cancelled = 0
GROUP BY DayOfWeek
ORDER BY avg_delay;




DROP TABLE aviation_dataset_processing_2_1;
CREATE EXTERNAL TABLE aviation_dataset_processing_2_1
    (airport   STRING,
    airline    STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "aviation_dataset_processing_2_1",
    "dynamodb.column.mapping"="airport:Airport,airline:Airline,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE aviation_dataset_processing_2_1
SELECT *
FROM (
  SELECT
    *,
    rank() over (PARTITION BY airport ORDER BY avg_delay, airline) rnk
  FROM (
    SELECT
      origin AS airport,
      uniquecarrier AS airline,
      round(avg(DepDelay),2) as avg_delay
    FROM aviation
    WHERE cancelled = 0
    GROUP BY origin, uniquecarrier
  ) unranked
) ranked
WHERE ranked.rnk <= 10;


SELECT * FROM aviation_dataset_processing_2_1 WHERE airport IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO');



DROP TABLE aviation_dataset_processing_2_2;
CREATE EXTERNAL TABLE aviation_dataset_processing_2_2
    (origin    STRING,
    dest       STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "aviation_dataset_processing_2_2",
    "dynamodb.column.mapping"="origin:Airport,dest:Destination,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE aviation_dataset_processing_2_2
SELECT *
FROM (
  SELECT
    *,
    rank() over (PARTITION BY origin ORDER BY avg_delay, dest) rnk
  FROM (
    SELECT
      origin AS origin,
      dest AS dest,
      round(avg(DepDelay),2) as avg_delay
    FROM aviation
    WHERE cancelled = 0
    GROUP BY origin, dest
  ) unranked
) ranked
WHERE ranked.rnk <= 10;


SELECT * FROM aviation_dataset_processing_2_2 WHERE origin IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO');



DROP TABLE aviation_dataset_processing_2_3;
CREATE EXTERNAL TABLE aviation_dataset_processing_2_3
    (flight    STRING,
    airline    STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "aviation_dataset_processing_2_3",
    "dynamodb.column.mapping"="flight:Flight,airline:Airline,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE aviation_dataset_processing_2_3
SELECT
  concat(ranked.x, "-", ranked.y) AS flight,
  ranked.airline,
  ranked.avg_delay,
  ranked.rnk
FROM (
  SELECT
    *,
    rank() over (PARTITION BY x, y ORDER BY avg_delay, airline) rnk
  FROM (
    SELECT
      origin AS x,
      dest AS y,
      uniquecarrier AS airline,
      round(avg(ArrDelay), 2) AS avg_delay
    FROM aviation
    WHERE cancelled = 0 AND ArrDelay IS NOT NULL
    GROUP BY origin, dest, uniquecarrier
  ) unranked
) ranked
WHERE ranked.rnk <= 10;


SELECT * FROM aviation_dataset_processing_2_3 WHERE flight IN ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX');



DROP TABLE aviation_dataset_processing_2_4;
CREATE EXTERNAL TABLE aviation_dataset_processing_2_4
    (flight    STRING,
    avg_delay  DOUBLE)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "aviation_dataset_processing_2_4",
    "dynamodb.column.mapping"="flight:Flight,avg_delay:AvgDelay"
);

INSERT OVERWRITE TABLE aviation_dataset_processing_2_4
SELECT
  concat(origin, "-", dest) AS flight,
  round(avg(ArrDelay), 2) AS avg_delay
FROM aviation
WHERE cancelled = 0 AND ArrDelay IS NOT NULL
GROUP BY origin, dest;


SELECT * FROM aviation_dataset_processing_2_4 WHERE flight IN ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX');




SELECT
  o.origin AS airport,
  o.total_departing_flights + d.total_arriving_flights AS total_flights,
  rank() OVER (ORDER BY o.total_departing_flights + d.total_arriving_flights DESC) rnk
FROM
  (SELECT origin, count(flightnum) AS total_departing_flights FROM aviation WHERE cancelled = 0 GROUP BY origin) o
  JOIN
  (SELECT dest, count(flightnum) AS total_arriving_flights FROM aviation WHERE cancelled = 0  GROUP BY dest) d
  ON o.origin = d.dest
ORDER BY rnk;



DROP TABLE aviation_dataset_processing_3_2;
CREATE TABLE aviation_dataset_processing_3_2 (
  x STRING,
  y STRING,
  airline1 STRING,
  flightnum1 STRING,
  flightdate1 STRING,
  departure1 STRING,
  delay1 DOUBLE,
  z STRING,
  airline2 STRING,
  flightnum2 STRING,
  flightdate2 STRING,
  departure2 STRING,
  delay2 DOUBLE,
  total_delay DOUBLE,
  itinerary STRING);

INSERT OVERWRITE TABLE aviation_dataset_processing_3_2
SELECT
  leg1.origin AS x,
  leg1.dest AS y,
  leg1.uniquecarrier AS airline1,
  leg1.flightnum AS flight_num1,
  leg1.flightdate AS flight_date1,
  leg1.DepTime AS departure1,
  leg1.ArrDelay AS delay1,
  leg2.dest AS z,
  leg2.uniquecarrier AS airline2,
  leg2.flightnum AS flight_num2,
  leg2.flightdate AS flight_date2,
  leg2.DepTime AS departure2,
  leg2.ArrDelay AS delay2,
  (leg1.ArrDelay + leg2.ArrDelay) AS total_delay,
  concat(leg1.origin, "-", leg1.dest, "-", leg2.dest) AS itinerary
FROM (
  SELECT
    origin,
    dest,
    uniquecarrier,
    flightnum,
    DepTime,
    ArrDelay,
    FlightDate,
    rank() over (PARTITION BY origin, dest, yeard, monthd, dayofmonth ORDER BY ArrDelay) rnk
  FROM aviation
  WHERE DepTime < '1200' AND Yeard = 2008 AND Cancelled = 0
) leg1
JOIN (
  SELECT
    origin,
    dest,
    uniquecarrier,
    flightnum,
    DepTime,
    ArrDelay,
    FlightDate,
    rank() over (PARTITION BY origin, dest, yeard, monthd, dayofmonth ORDER BY ArrDelay) rnk
  FROM aviation
  WHERE DepTime > '1200' AND Yeard = 2008 AND Cancelled = 0
) leg2
ON leg1.rnk = 1 AND leg2.rnk = 1 AND leg1.dest = leg2.origin AND date_add(leg1.FlightDate, 2) = leg2.FlightDate;


SELECT *
FROM aviation_dataset_processing_3_2
WHERE (itinerary = "CMI-ORD-LAX" AND flightdate1 = "2008-03-04")
  OR (itinerary = "JAX-DFW-CRP" AND flightdate1 = "2008-09-09")
  OR (itinerary = "SLC-BFL-LAX" AND flightdate1 = "2008-04-01")
  OR (itinerary = "LAX-SFO-PHX" AND flightdate1 = "2008-07-12")
  OR (itinerary = "DFW-ORD-DFW" AND flightdate1 = "2008-06-10")
  OR (itinerary = "LAX-ORD-JFK" AND flightdate1 = "2008-01-01")
ORDER BY itinerary;
