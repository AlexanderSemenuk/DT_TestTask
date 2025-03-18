using System.ComponentModel;
using System.Data.SqlClient;
using Serilog;
using TaxiDataETL.Core.Abstractions;

namespace TaxiDataETL.Infrastructure.Services;

public class SqlQueryService : IQueryService
{
    private readonly string _connectionString;
    
    public SqlQueryService(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    public async Task<T> ExecuteScalarAsync<T>(string sql, CancellationToken cancellationToken = default)
    {
        Log.Debug("Executing scalar query: {Sql}", sql);
            
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
                
            using var command = new SqlCommand(sql, connection);
            var result = await command.ExecuteScalarAsync(cancellationToken);
                
            if (result == null || result == DBNull.Value)
            {
                return default;
            }
                
            return (T)Convert.ChangeType(result, typeof(T));
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error executing scalar query: {Sql}", sql);
            throw;
        }
    }
    
    public async Task<List<TResult>> ExecuteQueryAsync<TResult>(string sql, CancellationToken cancellationToken = default)
    {
        Log.Debug("Executing query: {Sql}", sql);
            
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
                
            await using var command = new SqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                
            var results = new List<TResult>();
                
            while (await reader.ReadAsync(cancellationToken))
            {
                if (typeof(TResult).IsValueType || typeof(TResult) == typeof(string))
                {
                    var value = reader[0];
                    if (value == DBNull.Value)
                    {
                        results.Add(default);
                    }
                    else
                    {
                        results.Add((TResult)Convert.ChangeType(value, typeof(TResult)));
                    }
                }
                else
                {
                    throw new NotSupportedException($"Complex type {typeof(TResult).Name} is not supported by this simple query service");
                }
            }
                
            return results;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error executing query: {Sql}", sql);
            throw;
        }
    }
}