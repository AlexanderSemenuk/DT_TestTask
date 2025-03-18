using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Services;
using TaxiDataETL.Infrastructure.DependencyInjection;

namespace TaxiDataETL.Console
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();
            
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.FromLogContext()
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("logs/taxi-etl-.log", rollingInterval: RollingInterval.Day)
                .CreateLogger();
            
            try
            {
                Log.Information("Starting ETL application");
                
                var csvFilePath = configuration["CsvFilePath"];
                var duplicatesCsvPath = configuration["DuplicatesCsvPath"] ?? "duplicates.csv";
                var connectionString = configuration.GetConnectionString("DefaultConnection");
                
                if (string.IsNullOrEmpty(csvFilePath) || string.IsNullOrEmpty(connectionString))
                {
                    Log.Error("Missing required configuration. Please provide CsvFilePath and ConnectionString.");
                    return;
                }
                
                Log.Information("Starting ETL process for file: {CsvFilePath}", csvFilePath);
                Log.Information("Duplicates will be written to: {DuplicatesCsvPath}", duplicatesCsvPath);
                
                var stopwatch = Stopwatch.StartNew();

                var serviceProvider = ConfigureServices(configuration);
                
                var processor = serviceProvider.GetRequiredService<IDataProcessor>();
                var rowsProcessed = await processor.ProcessDataAsync();
                
                stopwatch.Stop();
                
                Log.Information("ETL process completed successfully");
                Log.Information("Total rows in final table: {RowCount:N0}", rowsProcessed);
                Log.Information("Total processing time: {Elapsed}", stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Application terminated unexpectedly");
                return;
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
        
        private static ServiceProvider ConfigureServices(IConfiguration configuration)
        {
            var services = new ServiceCollection();
            
            services.AddSingleton(configuration);
            
            services.AddLogging(loggingBuilder =>
            {
                loggingBuilder.ClearProviders();
                loggingBuilder.AddSerilog(dispose: true);
            });
            
            services.AddSingleton<IProgressReporter, ProgressReporter>();
            
            services.AddSingleton<IDataProcessor, TaxiDataProcessor>();
            
            services.AddInfrastructureServices(configuration);
            
            return services.BuildServiceProvider();
        }
    }
}