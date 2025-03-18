using System.Data.SqlClient;
using Serilog;
using TaxiDataETL.Core.Abstractions;

namespace TaxiDataETL.Infrastructure.Services;

public class SqlDatabaseManager : IDatabaseManager
{
    private readonly string _connectionString;

    public SqlDatabaseManager(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task EnsureSchemaExistsAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("Ensuring database schema exists");

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = @"
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
                        
                        -- Computed column for trip duration
                        ALTER TABLE TaxiTrips ADD TripDurationSeconds AS DATEDIFF(second, PickupDateTime, DropoffDateTime) PERSISTED;
                        
                        -- Create optimized indexes
                        CREATE INDEX IX_TaxiTrips_PULocationID_TipAmount ON TaxiTrips(PULocationID) INCLUDE (TipAmount);
                        CREATE INDEX IX_TaxiTrips_TripDistance_DESC ON TaxiTrips(TripDistance DESC);
                        CREATE INDEX IX_TaxiTrips_TripDurationSeconds_DESC ON TaxiTrips(TripDurationSeconds DESC);
                        
                        PRINT 'Schema created successfully.';
                    END
                    ELSE
                    BEGIN
                        PRINT 'Schema already exists.';
                    END
                ";

            using var command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            Log.Information("Database schema setup completed");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error creating database schema");
            throw;
        }
    }

    public async Task CreateTempTableAsync(CancellationToken cancellationToken = default)
    {
        Log.Debug("Creating temporary table for data processing");

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = @"
                    IF OBJECT_ID('TaxiTripsTemp') IS NOT NULL
                    BEGIN
                        DROP TABLE TaxiTripsTemp;
                        PRINT 'Dropped existing temp table.';
                    END
                        
                    CREATE TABLE TaxiTripsTemp (
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
                    
                    PRINT 'Created temporary table.';
                ";

            using var command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            Log.Debug("Temporary table created successfully");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error creating temporary table");
            throw;
        }
    }

    public async Task DropTempTableAsync(CancellationToken cancellationToken = default)
    {
        Log.Debug("Dropping temporary table");

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            var sql = "IF OBJECT_ID('TaxiTripsTemp') IS NOT NULL DROP TABLE TaxiTripsTemp;";

            using var command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            Log.Debug("Temporary table dropped successfully");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error dropping temporary table");
        }
    }
}
