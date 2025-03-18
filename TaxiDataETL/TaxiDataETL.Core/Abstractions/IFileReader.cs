namespace TaxiDataETL.Core.Abstractions;

public interface IFileReader<T>
{
    IAsyncEnumerable<List<T>> ReadChunksAsync(int chunkSize, CancellationToken cancellationToken = default);
}
