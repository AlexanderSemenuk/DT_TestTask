using Serilog;
using TaxiDataETL.Core.Abstractions;

namespace TaxiDataETL.Core.Services;

public class ProgressReporter : IProgressReporter
{
    public void ReportProgress(int processedCount, int totalCount, string message)
    {
        if (totalCount > 0)
        {
            var percentage = (double)processedCount / totalCount * 100;
            Log.Information("{Message}: {ProcessedCount:N0}/{TotalCount:N0} ({Percentage:F2}%)",
                message, processedCount, totalCount, percentage);
        }
        else
        {
            Log.Information("{Message}: {ProcessedCount:N0} records processed",
                message, processedCount);
        }
    }
    
    public void ReportCompletion(int totalProcessed, TimeSpan elapsedTime)
    {
        Log.Information("Processing completed: {TotalProcessed:N0} records in {ElapsedTime:g}",
            totalProcessed, elapsedTime);
            
        if (elapsedTime.TotalSeconds > 0)
        {
            var recordsPerSecond = totalProcessed / elapsedTime.TotalSeconds;
            Log.Information("Processing speed: {RecordsPerSecond:F2} records/second", recordsPerSecond);
        }
    }
}