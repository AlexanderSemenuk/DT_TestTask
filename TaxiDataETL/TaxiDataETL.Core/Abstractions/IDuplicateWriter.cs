namespace TaxiDataETL.Core.Abstractions;

public interface IDuplicateWriter<T>
{
    Task WriteAsync(List<T> duplicates, CancellationToken cancellationToken = default);
}