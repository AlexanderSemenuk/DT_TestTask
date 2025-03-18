using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;
using TaxiDataETL.Infrastructure.Services;

namespace TaxiDataETL.Infrastructure.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddInfrastructureServices(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<IFileReader<TaxiTripCsvRecord>>(provider => 
                new CsvReader(
                    configuration["CsvFilePath"]
                ));
                
            services.AddSingleton<IDataTransformer<TaxiTripCsvRecord, TaxiTrip>, TripDataTransformer>();
            services.AddSingleton<IDuplicateChecker<TaxiTrip>, DuplicateChecker>();
            
            services.AddSingleton<IDuplicateWriter<TaxiTripCsvRecord>>(provider => 
                new DuplicateWriter(
                    configuration["DuplicatesCsvPath"] ?? "duplicates.csv"
                ));
                
            services.AddSingleton<IDatabaseManager>(provider => 
                new SqlDatabaseManager(
                    configuration.GetConnectionString("DefaultConnection")
                ));
                
            services.AddSingleton<IBulkInserter<TaxiTrip>>(provider => 
                new SqlBulkInserter(
                    configuration.GetConnectionString("DefaultConnection")
                ));
                
            services.AddSingleton<IQueryService>(provider => 
                new SqlQueryService(
                    configuration.GetConnectionString("DefaultConnection")
                ));
                
            return services;
        }
}