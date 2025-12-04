
using System.Data;

namespace ETLProject.Interfaces
{
    public interface ITripRepository
    {
        Task CreateTableIfNotExistsAsync();
        Task BulkInsertAsync(DataTable table);

        Task<(short PULocationID, decimal AvgTip)> GetTopTipLocationAsync();
        Task<DataTable> GetTopTripDistanceAsync(int top = 100);
        Task<DataTable> GetTopTripDurationAsync(int top = 100);
        Task<DataTable> SearchByPULocationAsync(short puLocationId);
    }
}
