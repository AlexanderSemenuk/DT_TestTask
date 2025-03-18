using System.Data;
using System.Data.SqlClient;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Infrastructure.Services;

public class SqlBulkInserter : IBulkInserter<TaxiTrip>, IDisposable
{
    private readonly string _connectionString;
    private SqlConnection _connection;

    public SqlBulkInserter(string connectionString)
    {
        _connectionString = connectionString;
        _connection = new SqlConnection(connectionString);
    }
    
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_connection.State != ConnectionState.Open)
        {
            await _connection.OpenAsync(cancellationToken);
        }
    }

    public async Task<int> BulkInsertIntoTempTableAsync(List<TaxiTrip> records,
    CancellationToken cancellationToken = default)
{
    if (records == null || records.Count == 0)
    {
        return 0;
    }

    Log.Information("Bulk inserting {Count} records into temporary table", records.Count);

    try
    {
        await InitializeAsync(cancellationToken);

        var dataTable = new DataTable();
        dataTable.Columns.Add("PickupDateTime", typeof(DateTime));
        dataTable.Columns.Add("DropoffDateTime", typeof(DateTime));
        dataTable.Columns.Add("PassengerCount", typeof(int));
        dataTable.Columns.Add("TripDistance", typeof(decimal));
        dataTable.Columns.Add("StoreAndFwdFlag", typeof(string));
        dataTable.Columns.Add("PULocationID", typeof(int));
        dataTable.Columns.Add("DOLocationID", typeof(int));
        dataTable.Columns.Add("FareAmount", typeof(decimal));
        dataTable.Columns.Add("TipAmount", typeof(decimal));

        foreach (var record in records)
        {
            if (record == null) continue; 
            
            var row = dataTable.NewRow();
            
            row["PickupDateTime"] = record.PickupDateTime;
            row["DropoffDateTime"] = record.DropoffDateTime;
            row["PassengerCount"] = record.PassengerCount ?? (object)DBNull.Value;
            row["TripDistance"] = record.TripDistance ?? (object)DBNull.Value;
            row["StoreAndFwdFlag"] = record.StoreAndFwdFlag ?? (object)DBNull.Value;
            row["PULocationID"] = record.PULocationID ?? (object)DBNull.Value;
            row["DOLocationID"] = record.DOLocationID ?? (object)DBNull.Value;
            row["FareAmount"] = record.FareAmount ?? (object)DBNull.Value;
            row["TipAmount"] = record.TipAmount ?? (object)DBNull.Value;
            
            dataTable.Rows.Add(row);
        }

        if (dataTable.Rows.Count == 0)
        {
            Log.Warning("After filtering out null records, no valid records remain to insert");
            return 0;
        }

        using var bulkCopy = new SqlBulkCopy(_connection)
        {
            DestinationTableName = "TaxiTripsTemp",
            BulkCopyTimeout = 0
        };

        bulkCopy.ColumnMappings.Add("PickupDateTime", "PickupDateTime");
        bulkCopy.ColumnMappings.Add("DropoffDateTime", "DropoffDateTime");
        bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
        bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
        bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
        bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
        bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
        bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
        bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

        Log.Information("Successfully inserted {Count} records", dataTable.Rows.Count);
        return dataTable.Rows.Count;
    }
    catch (Exception ex)
    {
        Log.Error(ex, "Error bulk inserting records into temporary table");
        throw;
    }
}
    public async Task<int> TransferFromTempToFinalTableAsync(CancellationToken cancellationToken = default)
    {
        Log.Information("Transferring data from temporary table to final table");
            
        try
        {
            await InitializeAsync(cancellationToken);
                
            var sql = @"
                    -- Insert from temp table to final table
                    INSERT INTO TaxiTrips (
                        PickupDateTime, DropoffDateTime, PassengerCount, 
                        TripDistance, StoreAndFwdFlag, PULocationID, 
                        DOLocationID, FareAmount, TipAmount
                    )
                    SELECT 
                        PickupDateTime, DropoffDateTime, PassengerCount, 
                        TripDistance, StoreAndFwdFlag, PULocationID, 
                        DOLocationID, FareAmount, TipAmount
                    FROM TaxiTripsTemp;
                    
                    -- Return count of rows inserted
                    SELECT COUNT(*) FROM TaxiTrips;
                ";
                
            using var command = new SqlCommand(sql, _connection);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            var rowCount = result != null ? Convert.ToInt32(result) : 0;
                
            Log.Information("Successfully transferred data to final table. Total row count: {RowCount}", rowCount);
            return rowCount;
        }
        catch (Exception ex)
        {
            Log.Information(ex, "Error transferring data from temporary to final table");
            throw;
        }
    }
    
    public void Dispose()
    {
        _connection?.Dispose();
    }
}