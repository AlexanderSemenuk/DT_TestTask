namespace TaxiDataETL.Core.Abstractions;

public interface IDataProcessor
{
    Task<int> ProcessDataAsync(CancellationToken cancellationToken = default);
}