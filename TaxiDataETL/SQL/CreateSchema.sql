IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TaxiTrips')
BEGIN
    CREATE TABLE TaxiTrips (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        PickupDateTime DATETIME2 NOT NULL,
        DropoffDateTime DATETIME2 NOT NULL,
        PassengerCount INT,
        TripDistance DECIMAL(18, 2),
        StoreAndFwdFlag NVARCHAR(3),
        PULocationID INT,
        DOLocationID INT,
        FareAmount DECIMAL(18, 2),
        TipAmount DECIMAL(18, 2)
    );
    
    ALTER TABLE TaxiTrips ADD TripDurationSeconds AS DATEDIFF(second, PickupDateTime, DropoffDateTime) PERSISTED;
    
    CREATE INDEX IX_TaxiTrips_PULocationID_TipAmount ON TaxiTrips(PULocationID) INCLUDE (TipAmount);
    CREATE INDEX IX_TaxiTrips_TripDistance_DESC ON TaxiTrips(TripDistance DESC);
    CREATE INDEX IX_TaxiTrips_TripDurationSeconds_DESC ON TaxiTrips(TripDurationSeconds DESC);
    
    PRINT 'Schema created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema already exists.';
END