using ETLProject.Csv;
using ETLProject.Domain;
using ETLProject.Interfaces;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text;


namespace ETLProject.Database
{
    public class TripRepository : ITripRepository
    {
        private readonly string _connectionString;

        public TripRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task CreateTableIfNotExistsAsync()
        {
            var createTableSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Trips')
            BEGIN
                CREATE TABLE dbo.Trips (
                    TripID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
                    tpep_pickup_datetime DATETIMEOFFSET NOT NULL,
                    tpep_dropoff_datetime DATETIMEOFFSET NOT NULL,
                    passenger_count TINYINT NOT NULL,
                    trip_distance DECIMAL(10,2) NOT NULL,
                    store_and_fwd_flag NVARCHAR(3) NOT NULL,
                    PULocationID SMALLINT NOT NULL,
                    DOLocationID SMALLINT NOT NULL,
                    fare_amount MONEY NOT NULL,
                    tip_amount MONEY NOT NULL,
                    TripDurationMinutes AS DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
                );

                CREATE NONCLUSTERED INDEX IX_Trips_PULocationID_Tip ON dbo.Trips(PULocationID) INCLUDE(tip_amount);
                CREATE NONCLUSTERED INDEX IX_Trips_TripDistance ON dbo.Trips(trip_distance DESC);
                CREATE NONCLUSTERED INDEX IX_Trips_TripDuration ON dbo.Trips(TripDurationMinutes DESC);
            END";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(createTableSql, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<(short PULocationID, decimal AvgTip)> GetTopTipLocationAsync()
        {
            const string sql = @"
                SELECT TOP 1 PULocationID, AVG(tip_amount) AS AvgTip
                FROM dbo.Trips
                GROUP BY PULocationID
                ORDER BY AvgTip DESC;";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                return ((short)reader["PULocationID"], (decimal)reader["AvgTip"]);
            }
            return (0, 0m);
        }

        public async Task<DataTable> GetTopTripDistanceAsync(int top = 100)
        {
            var dt = new DataTable();
            string sql = $"SELECT TOP {top} * FROM dbo.Trips ORDER BY trip_distance DESC";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var adapter = new SqlDataAdapter(sql, conn);
            adapter.Fill(dt);
            return dt;
        }

        public async Task<DataTable> GetTopTripDurationAsync(int top = 100)
        {
            var dt = new DataTable();
            string sql = $"SELECT TOP {top} * FROM dbo.Trips ORDER BY TripDurationMinutes DESC";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var adapter = new SqlDataAdapter(sql, conn);
            adapter.Fill(dt);
            return dt;
        }

        public async Task<DataTable> SearchByPULocationAsync(short puLocationId)
        {
            var dt = new DataTable();
            const string sql = "SELECT * FROM dbo.Trips WHERE PULocationID = @pu";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@pu", puLocationId);

            using var adapter = new SqlDataAdapter(cmd);
            adapter.Fill(dt);
            return dt;
        }


        public async Task BulkInsertAsync(DataTable table)
        {
            using var sqlConn = new SqlConnection(_connectionString);
            await sqlConn.OpenAsync();

            using var bulk = new SqlBulkCopy(sqlConn)
            {
                DestinationTableName = "dbo.Trips",
                BatchSize = table.Rows.Count
            };

            foreach (DataColumn col in table.Columns)
            {
                Console.WriteLine($"Column: {col.ColumnName}");
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await bulk.WriteToServerAsync(table);
        }

        public async Task InsertParsedRowsAsync(CsvRowMapper mapper, DataTableFactory dataTable)
        {
            var dedupeSet = new HashSet<string>();
            var table = dataTable.Create();
            using var dupWriter = new StreamWriter(GlobalSettings.DuplicatesCsvPath, false, new UTF8Encoding(false));
            using var dupCsv = new CsvHelper.CsvWriter(dupWriter, System.Globalization.CultureInfo.InvariantCulture);
            dupCsv.WriteHeader<OriginalRow>();
            dupCsv.NextRecord();

            long totalInserted = 0;
            var parsedRows = await mapper.ReadCsvAsync(GlobalSettings.InputCsvPath);

            foreach (var row in parsedRows)
            {
                string key = $"{row.PickupUtc.Ticks}|{row.DropoffUtc.Ticks}|{row.PassengerCount}";
                if (!dedupeSet.Add(key))
                {
                    dupCsv.WriteRecord(row.Original);
                    dupCsv.NextRecord();
                    continue;
                }

                var dr = table.NewRow();
                dr["tpep_pickup_datetime"] = row.PickupUtc;
                dr["tpep_dropoff_datetime"] = row.DropoffUtc;
                dr["passenger_count"] = row.PassengerCount ?? 0;
                dr["trip_distance"] = row.TripDistance;
                dr["store_and_fwd_flag"] = row.StoreAndFwdFlag;
                dr["PULocationID"] = row.PULocationID;
                dr["DOLocationID"] = row.DOLocationID;
                dr["fare_amount"] = row.FareAmount;
                dr["tip_amount"] = row.TipAmount;

                table.Rows.Add(dr);

                if (table.Rows.Count >= GlobalSettings.BATCH_SIZE)
                {
                    await BulkInsertAsync(table);
                    totalInserted += table.Rows.Count;
                    table.Clear();
                    Console.WriteLine($"Inserted {totalInserted} rows...");
                }
            }

            if (table.Rows.Count > 0)
            {
                await BulkInsertAsync(table);
                totalInserted += table.Rows.Count;
                Console.WriteLine($"Inserted {totalInserted} rows. ETL complete.");
            }
        }
    }

}
