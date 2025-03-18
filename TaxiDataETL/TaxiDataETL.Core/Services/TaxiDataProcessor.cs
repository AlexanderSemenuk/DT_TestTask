using System.Diagnostics;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Core.Services;

public class TaxiDataProcessor : IDataProcessor
{
    private readonly IFileReader<TaxiTripCsvRecord> _fileReader;
    private readonly IDataTransformer<TaxiTripCsvRecord, TaxiTrip> _transformer;
    private readonly IDuplicateChecker<TaxiTrip> _duplicateChecker;
    private readonly IDuplicateWriter<TaxiTripCsvRecord> _duplicateWriter;
    private readonly IBulkInserter<TaxiTrip> _bulkInserter;
    private readonly IDatabaseManager _databaseManager;
    private readonly IProgressReporter _progressReporter;
    private readonly int _batchSize;
    
    
    public TaxiDataProcessor(
        IFileReader<TaxiTripCsvRecord> fileReader,
        IDataTransformer<TaxiTripCsvRecord, TaxiTrip> transformer,
        IDuplicateChecker<TaxiTrip> duplicateChecker,
        IDuplicateWriter<TaxiTripCsvRecord> duplicateWriter,
        IBulkInserter<TaxiTrip> bulkInserter,
        IDatabaseManager databaseManager,
        IProgressReporter progressReporter,
        int batchSize = 100000)
    {
        _fileReader = fileReader;
        _transformer = transformer;
        _duplicateChecker = duplicateChecker;
        _duplicateWriter = duplicateWriter;
        _bulkInserter = bulkInserter;
        _databaseManager = databaseManager;
        _progressReporter = progressReporter;
        _batchSize = batchSize;
    }
    
     public async Task<int> ProcessDataAsync(CancellationToken cancellationToken = default)
        {
            Log.Information("Beginning data processing");
            var stopwatch = Stopwatch.StartNew();
            
            await _databaseManager.EnsureSchemaExistsAsync(cancellationToken);
            
            await _databaseManager.CreateTempTableAsync(cancellationToken);
            
            var totalRowsInserted = 0;
            var duplicatesCount = 0;
            var processedCount = 0;
            
            try
            {
                await foreach (var chunk in _fileReader.ReadChunksAsync(_batchSize, cancellationToken))
                {
                    processedCount += chunk.Count;
                    _progressReporter.ReportProgress(processedCount, 0, $"Processing chunk of {chunk.Count} records");
                    
                    var (uniqueRecords, duplicateRecords) = _transformer.ProcessChunk(chunk, _duplicateChecker);

                    if (duplicateRecords.Count > 0)
                    {
                        await _duplicateWriter.WriteAsync(duplicateRecords, cancellationToken);
                        duplicatesCount += duplicateRecords.Count;
                        Log.Information("Found {DuplicateCount} duplicates. Total duplicates: {TotalDuplicates:N0}", 
                            duplicateRecords.Count, duplicatesCount);
                    }
                    
                    if (uniqueRecords.Count > 0)
                    {
                        var rowsInserted = await _bulkInserter.BulkInsertIntoTempTableAsync(uniqueRecords, cancellationToken);
                        totalRowsInserted += rowsInserted;
                        Log.Information("Inserted {RowsInserted:N0} records. Total inserted: {TotalRowsInserted:N0}", 
                            rowsInserted, totalRowsInserted);
                    }
                    
                    cancellationToken.ThrowIfCancellationRequested();
                }
                
                Log.Information("Transferring data from temporary table to final table");
                var finalRowCount = await _bulkInserter.TransferFromTempToFinalTableAsync(cancellationToken);
                
                stopwatch.Stop();
                _progressReporter.ReportCompletion(finalRowCount, stopwatch.Elapsed);
                Log.Information("Final row count after all processing: {FinalRowCount:N0}", finalRowCount);
                
                return finalRowCount;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error processing data");
                throw;
            }
            finally
            {
                Log.Information("Cleaning up temporary resources");
                await _databaseManager.DropTempTableAsync(cancellationToken);
            }
        }
}