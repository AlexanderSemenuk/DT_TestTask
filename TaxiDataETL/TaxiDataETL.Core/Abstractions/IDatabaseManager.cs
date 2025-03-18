namespace TaxiDataETL.Core.Abstractions;

public interface IDatabaseManager
{
    Task EnsureSchemaExistsAsync(CancellationToken cancellationToken = default);
    Task CreateTempTableAsync(CancellationToken cancellationToken = default);
    Task DropTempTableAsync(CancellationToken cancellationToken = default);
}