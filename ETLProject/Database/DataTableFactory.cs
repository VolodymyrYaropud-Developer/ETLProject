using ETLProject.Domain;
using ETLProject.Interfaces;
using System.Data;

namespace ETLProject.Database
{
    public class DataTableFactory : IDataTableFactory
    {
        public DataTable Create()
        {
            var dt = new DataTable();
            dt.Columns.Add(new DataColumn("tpep_pickup_datetime", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("tpep_dropoff_datetime", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("passenger_count", typeof(byte)));
            dt.Columns.Add(new DataColumn("trip_distance", typeof(decimal)));
            dt.Columns.Add(new DataColumn("store_and_fwd_flag", typeof(string)));
            dt.Columns.Add(new DataColumn("PULocationID", typeof(short)));
            dt.Columns.Add(new DataColumn("DOLocationID", typeof(short)));
            dt.Columns.Add(new DataColumn("fare_amount", typeof(decimal)));
            dt.Columns.Add(new DataColumn("tip_amount", typeof(decimal)));
            return dt;
        }

        public DataRow CreateRow(DataTable table, ParsedRow row)
        {
            var dr = table.NewRow();
            dr["tpep_pickup_datetime"] = DateTime.SpecifyKind(row.PickupUtc, DateTimeKind.Utc);
            dr["tpep_dropoff_datetime"] = DateTime.SpecifyKind(row.DropoffUtc, DateTimeKind.Utc);
            dr["passenger_count"] = row.PassengerCount ?? (byte)0;
            dr["trip_distance"] = row.TripDistance ?? (object)DBNull.Value;
            dr["store_and_fwd_flag"] = row.StoreAndFwdFlag ?? (object)DBNull.Value;
            dr["PULocationID"] = row.PULocationID ?? (object)DBNull.Value;
            dr["DOLocationID"] = row.DOLocationID ?? (object)DBNull.Value;
            dr["fare_amount"] = row.FareAmount ?? (object)DBNull.Value;
            dr["tip_amount"] = row.TipAmount ?? (object)DBNull.Value;
            return dr;
        }
    }
}
