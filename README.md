# TaxiDataETL

A C# ETL (Extract, Transform, Load) application for processing NYC taxi trip data from CSV files and importing it into a SQL Server database. Built to be efficient, scalable, and capable of handling potentially unsafe data sources.

## Project Overview

This application imports taxi trip data from a CSV file into a SQL Server database, performing data cleaning, deduplication, and transformation while optimizing the database schema for specific query patterns.

## Key Features

- **Scalable Design**: Processes files of any size, from small samples to large datasets
- **Memory Efficiency**: Uses streaming and batched operations to minimize memory usage
- **Error Handling**: Comprehensive error handling and logging throughout
- **Data Validation**: Validates and sanitizes input data to handle potentially unsafe sources
- **Deduplication**: Identifies and removes duplicate records
- **Optimized Database**: Schema designed for specific query patterns

## Architecture

I implemented a clean architecture approach with proper separation of concerns:

- **Core Layer**: Contains domain models, interfaces, and business logic
- **Infrastructure Layer**: Implements data access and external service integration
- **Console Application**: Provides the entry point and configuration

## Technical Implementation

### Optimizations for Large Files

- **Memory-Mapped Files**: For efficient processing of large files
- **Streaming Processing**: Processes data in chunks to manage memory usage
- **Bulk Database Operations**: Uses SqlBulkCopy for fast data insertion
- **Parallel Processing**: Utilizes parallel operations for CPU-bound transformations
- **Optimized Batching**: Configurable batch sizes for performance tuning

### Data Safety Measures

- **Input Validation**: Thorough validation of all input data
- **Data Sanitization**: Cleaning and normalization of data values
- **Error Handling**: Skip-and-continue approach for bad records
- **Logging**: Detailed logging of processing steps and issues

### Database Optimizations

- **Optimized Schema**: Tables designed for the required query patterns
- **Strategic Indexing**: Indexes created based on query requirements
- **Computed Columns**: For frequently queried calculations
- **Efficient Loading**: Two-phase loading process with a staging table

## How It Works

The ETL process follows these steps:

1. **Extract**: Reads CSV data in chunks using memory-mapped files
2. **Transform**: 
   - Converts timezone from EST to UTC
   - Normalizes flag values ('Y'/'N' to 'Yes'/'No')
   - Removes whitespace and validates data
3. **Load**: 
   - Uses a staging table for bulk operations
   - Deduplicates records
   - Creates optimized indexes

## Handling Larger Files (10GB+)

For scaling to extremely large files (10GB+), I would make these additional improvements:

1. **Distributed Processing**: Implement distributed computing using technologies like Azure Data Factory, AWS Glue, Hadoop, or SSIS for parallel processing across multiple nodes
2. **Database Partitioning**: Add table partitioning to improve both loading and query performance
3. **Incremental Processing**: Support for delta loads rather than processing the entire file every time
4. **Checkpointing**: Add resume capability to restart from the last successful checkpoint if interrupted
5. **Resource Monitoring**: Dynamically adjust batch sizes based on available system resources

## Usage

1. Configure the application in `appsettings.json`
2. Run the application with `dotnet run`
