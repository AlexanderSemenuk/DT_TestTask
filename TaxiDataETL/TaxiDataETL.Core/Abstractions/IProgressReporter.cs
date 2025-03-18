namespace TaxiDataETL.Core.Abstractions;

public interface IProgressReporter
{
    void ReportProgress(int processedCount, int totalCount, string message);
    void ReportCompletion(int totalProcessed, TimeSpan elapsedTime);
}