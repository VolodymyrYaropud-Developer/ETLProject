using CsvHelper;
using CsvHelper.Configuration;
using ETLProject.Domain;
using ETLProject.Interfaces;
using System.Globalization;

namespace ETLProject.Csv
{
    public class CsvRowMapper : ICsvRowMapper
    {
        private readonly TimeZoneInfo _sourceTz;

        public CsvRowMapper(TimeZoneInfo sourceTz)
        {
            _sourceTz = sourceTz;
        }

        public async Task<IEnumerable<ParsedRow>> ReadCsvAsync(string path)
        {
            var result = new List<ParsedRow>();
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                TrimOptions = TrimOptions.Trim,
                IgnoreBlankLines = true,
                MissingFieldFound = null,
                BadDataFound = null
            };

            using var reader = new StreamReader(path);
            using var csv = new CsvReader(reader, config);
            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var row = MapRow(csv);
                if (row != null)
                    result.Add(row);
            }

            return result;
        }

        public ParsedRow MapRow(CsvReader csv)
        {
            try
            {
                var orig = new OriginalRow
                {
                    tpep_pickup_datetime = csv.GetField("tpep_pickup_datetime"),
                    tpep_dropoff_datetime = csv.GetField("tpep_dropoff_datetime"),
                    passenger_count = csv.GetField("passenger_count"),
                    trip_distance = csv.GetField("trip_distance"),
                    store_and_fwd_flag = csv.GetField("store_and_fwd_flag"),
                    PULocationID = csv.GetField("PULocationID"),
                    DOLocationID = csv.GetField("DOLocationID"),
                    fare_amount = csv.GetField("fare_amount"),
                    tip_amount = csv.GetField("tip_amount")
                };

                if (orig.tpep_pickup_datetime == null || orig.tpep_dropoff_datetime == null)
                {
                    throw new Exception("Pickup or Dropoff datetime is null");
                }

                DateTime pickupLocal = DateTime.Parse(orig.tpep_pickup_datetime); 
                DateTime dropoffLocal = DateTime.Parse(orig.tpep_dropoff_datetime);

                var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, _sourceTz);
                var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, _sourceTz);

                if (orig.store_and_fwd_flag == null)
                {
                    throw new Exception("store_and_fwd_flag is null");
                }

                string? rawFlag = orig.store_and_fwd_flag?.Trim().ToUpperInvariant();
                string flag;

                if (rawFlag == "Y")
                {
                    flag = "Yes";
                }
                else if (rawFlag == "N")
                {
                    flag = "No";
                }
                else
                {
                    flag = orig.store_and_fwd_flag;
                }

                return new ParsedRow
                {
                    Original = orig,
                    PickupUtc = pickupUtc,
                    DropoffUtc = dropoffUtc,
                    PassengerCount = byte.TryParse(orig.passenger_count, out var pc) ? pc : null,
                    TripDistance = decimal.TryParse(orig.trip_distance, out var td) ? td : null,
                    StoreAndFwdFlag = flag,
                    PULocationID = int.TryParse(orig.PULocationID, out var pu) ? pu : null,
                    DOLocationID = int.TryParse(orig.DOLocationID, out var doId) ? doId : null,
                    FareAmount = decimal.TryParse(orig.fare_amount, out var fa) ? fa : null,
                    TipAmount = decimal.TryParse(orig.tip_amount, out var ta) ? ta : null
                };
            }
            catch
            {
                throw new Exception("Something went wrong.\nPlease try again later");
            }

        }
    }
}
