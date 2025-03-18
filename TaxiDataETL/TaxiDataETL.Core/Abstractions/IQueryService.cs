namespace TaxiDataETL.Core.Abstractions;

public interface IQueryService
{
    Task<T> ExecuteScalarAsync<T>(string sql, CancellationToken cancellationToken = default);
    Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, CancellationToken cancellationToken = default);
}