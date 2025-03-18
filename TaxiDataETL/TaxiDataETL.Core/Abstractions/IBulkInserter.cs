namespace TaxiDataETL.Core.Abstractions;

public interface IBulkInserter<T>
{
    Task<int> BulkInsertIntoTempTableAsync(List<T> records, CancellationToken cancellationToken = default);
    Task<int> TransferFromTempToFinalTableAsync(CancellationToken cancellationToken = default);
}