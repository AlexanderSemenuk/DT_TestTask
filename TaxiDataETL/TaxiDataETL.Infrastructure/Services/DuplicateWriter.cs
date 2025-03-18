using System.Globalization;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Infrastructure.Services;

public class DuplicateWriter : IDuplicateWriter<TaxiTripCsvRecord>
{
    private readonly string _duplicatesCsvPath;
    private readonly SemaphoreSlim _writeLock = new (1, 5);
    
    public DuplicateWriter(string duplicatesCsvPath)
    {
        _duplicatesCsvPath = duplicatesCsvPath;
    }
    
    public async Task WriteAsync(List<TaxiTripCsvRecord> duplicates, CancellationToken cancellationToken = default)
    {
        if (duplicates.Count == 0)
        {
            return;
        }
            
        Log.Information("Writing {Count} duplicates to file: {FilePath}", duplicates.Count, _duplicatesCsvPath);
            
        try
        {
            await _writeLock.WaitAsync(cancellationToken);
            
            var fileExists = File.Exists(_duplicatesCsvPath);
                
            await using var writer = new StreamWriter(_duplicatesCsvPath, append: fileExists);
            await using var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture);

            if (!fileExists)
            {
                csv.WriteHeader<TaxiTripCsvRecord>();
                await csv.NextRecordAsync();
            }
            
            foreach (var record in duplicates)
            {
                csv.WriteRecord(record);
                await csv.NextRecordAsync();
            }
                
            Log.Debug("Successfully wrote {Count} duplicates to file", duplicates.Count);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error writing duplicates to file: {FilePath}", _duplicatesCsvPath);
            throw;
        }
        finally
        {
            _writeLock.Release();
        }
    }
}