NYC Taxi Trips ETL Processor
This project implements a highly efficient Extract, Transform, and Load (ETL) pipeline in C# to process large CSV files of NYC taxi trip data and perform bulk insertion into an optimized SQL Server database.
The core goal is to demonstrate best practices in data handling: streaming for memory efficiency, in-memory deduplication, data transformation (including timezone conversion), and high-performance database loading using SqlBulkCopy


Features

1. High-Performance ETL Architecture
Streaming CSV Reading: Uses CsvHelper and asynchronous operations to process data line-by-line, ensuring low memory usage suitable for large files.
Batch Processing: Data is inserted into SQL Server in fixed batches (default 50,000 rows), optimizing throughput and minimizing the duration of individual database locks.
Repository Pattern: Separation of concerns using ITripRepository for database operations and IDataTableFactory for data structure preparation.
2. Data Cleaning and Transformation
Deduplication: Duplicates are identified and removed based on a combination of tpep_pickup_datetime, tpep_dropoff_datetime, and passenger_count. Removed duplicates are logged to a separate file.
Timezone Conversion: Input timestamps (assumed EST) are converted to UTC and stored as DATETIMEOFFSET in the database.
Data Normalization: Converts store_and_fwd_flag values ('Y'/'N') to the required standard ('Yes'/'No').
Safety: Ensures all string-based fields are trimmed of leading/trailing whitespace.

Database Schema & Optimization
The SQL Server schema is designed for speed and includes specific indexes to support common analytical queries based on the project requirements.
Table Definition (Trips)

CREATE TABLE [dbo].[Trips] (
    [TripID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    [tpep_pickup_datetime] DATETIMEOFFSET(0) NOT NULL,
    [tpep_dropoff_datetime] DATETIMEOFFSET(0) NOT NULL,
    [passenger_count] TINYINT NOT NULL,
    [trip_distance] DECIMAL(10, 2) NOT NULL,
    [store_and_fwd_flag] NVARCHAR(3) NOT NULL,
    [PULocationID] SMALLINT NOT NULL,
    [DOLocationID] SMALLINT NOT NULL,
    [fare_amount] MONEY NOT NULL,
    [tip_amount] MONEY NOT NULL,    
    [TripDurationMinutes] AS (DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)) PERSISTED
);

Setup and Requirements

Requirements
.NET 8+ SDK (or compatible .NET version)
SQL Server instance (LocalDB or full instance)

NuGet Packages
The project relies on:
Microsoft.Data.SqlClient
CsvHelper

How to Run
Database Setup: Execute the SQL scripts to create the TaxiTripData database and the Trips table with the defined indexes.
Run Application:
dotnet run

Future Improvements
The current design is robust for most inputs but can be extended for ultimate scale:
Massive Scale Deduplication: For inputs exceeding 10GB, modify the CsvProcessor to bypass in-memory deduplication entirely. Instead, stream data to a SQL Staging Table, and perform deduplication using set-based operations (ROW_NUMBER() or GROUP BY) directly on the database.
Error Logging: Implement a robust logging framework (e.g., Serilog) to capture data quality issues and SQL Server errors more effectively than console output.
Dependency Injection Container: Use a proper DI container (e.g., built-in Microsoft.Extensions.DependencyInjection) for managing service lifetimes.

