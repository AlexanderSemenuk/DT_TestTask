using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using CsvHelper;
using CsvHelper.Configuration;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Mapping;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Infrastructure.Services;

public class CsvReader : IFileReader<TaxiTripCsvRecord>
{
    private readonly string _csvFilePath;
    
    public CsvReader(string csvFilePath)
    {
        _csvFilePath = csvFilePath;
    }
    
    public async IAsyncEnumerable<List<TaxiTripCsvRecord>> ReadChunksAsync(
    int chunkSize, 
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    {
        HasHeaderRecord = true,
        MissingFieldFound = null,
        BadDataFound = context => Log.Warning("Bad data found: {Field} at line {LineNumber}", context.RawRecord, context.RawRecord),
        ReadingExceptionOccurred = context => 
        {
            Log.Warning("Error reading CSV: {Error}", context.Exception.Message);
            return false;
        },
        TrimOptions = TrimOptions.Trim,
        AllowComments = true,
        IgnoreBlankLines = true,
        Mode = CsvMode.RFC4180
    };
    
    Log.Information("Beginning to read file: {FilePath}", _csvFilePath);
    
    using var mmf = MemoryMappedFile.CreateFromFile(_csvFilePath, FileMode.Open);
    using var mmvs = mmf.CreateViewStream();
    using var reader = new StreamReader(mmvs);
    using var csv = new CsvHelper.CsvReader(reader, config);
    
    csv.Context.RegisterClassMap<TaxiTripCsvRecordMap>();
    
    var records = new List<TaxiTripCsvRecord>();
    int totalProcessed = 0;
    
    await foreach (var record in csv.GetRecordsAsync<TaxiTripCsvRecord>().WithCancellation(cancellationToken))
    {
        if (record != null)
        {
            records.Add(record);
            totalProcessed++;
            
            if (records.Count >= chunkSize)
            {
                Log.Debug("Read chunk of {ChunkSize} records. Total processed: {TotalProcessed}", records.Count, totalProcessed);
                yield return records;
                records = new List<TaxiTripCsvRecord>();
            }
        }
    }
    
    if (records.Count > 0)
    {
        Log.Debug("Read final chunk of {ChunkSize} records. Total processed: {TotalProcessed}", records.Count, totalProcessed);
        yield return records;
    }
    
    Log.Information("Completed reading file: {FilePath}. Total records: {TotalProcessed}", _csvFilePath, totalProcessed);
}
}