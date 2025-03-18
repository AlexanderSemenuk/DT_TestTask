-- Query 1: Find out which PULocationId has the highest tip_amount on average
SELECT
    PULocationID,
    AVG(TipAmount) AS AverageTipAmount,
    COUNT(*) AS TripCount
FROM
    TaxiTrips
GROUP BY
    PULocationID
ORDER BY
    AverageTipAmount DESC;

-- Query 2: Find the top 100 longest fares in terms of trip_distance
SELECT TOP 100
    Id,
    PickupDateTime,
    DropoffDateTime,
    PULocationID,
    DOLocationID,
    TripDistance,
    FareAmount,
    TipAmount
FROM
    TaxiTrips
ORDER BY
    TripDistance DESC;

-- Query 3: Find the top 100 longest fares in terms of time spent traveling
SELECT TOP 100
    Id,
    PickupDateTime,
    DropoffDateTime,
    PULocationID,
    DOLocationID,
    TripDistance,
    DATEDIFF(second, PickupDateTime, DropoffDateTime) AS TripDurationSeconds,
    FareAmount,
    TipAmount
FROM
    TaxiTrips
ORDER BY
    TripDurationSeconds DESC;

-- Query 4: Search, where part of the conditions is PULocationId
SELECT
    Id,
    PickupDateTime,
    DropoffDateTime,
    PULocationID,
    DOLocationID,
    TripDistance,
    FareAmount,
    TipAmount
FROM
    TaxiTrips
WHERE
    PULocationID = 236 -- Example value
    AND TripDistance > 10
    AND FareAmount > 30;