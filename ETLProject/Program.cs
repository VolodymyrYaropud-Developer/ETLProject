using ETLProject.Csv;
using ETLProject.Database;
using ETLProject.Dedupe;
using ETLProject.Download;

class Program
{
    static async Task Main(string[] args)
    {
        var mapper = new CsvRowMapper(GlobalSettings.SourceTz);
        var dupeChecker = new HashSetDupeChecker();
        var tableFactory = new DataTableFactory();
        var repo = new TripRepository(GlobalSettings.ConnectionString);
        await repo.CreateTableIfNotExistsAsync();
        var downloader = new FileDownloader(new HttpClient());
        await downloader.DownloadToFileAsync();

        var processor = new CsvProcessor(mapper, tableFactory, repo, dupeChecker);

        await processor.ProcessAsync(
            GlobalSettings.InputCsvPath,
            GlobalSettings.DuplicatesCsvPath);

        await repo.InsertParsedRowsAsync(mapper, tableFactory);


        var topTip = await repo.GetTopTipLocationAsync();
        Console.WriteLine($"Top tip PULocationID: {topTip.PULocationID}, AvgTip: {topTip.AvgTip}");

        var topDistance = await repo.GetTopTripDistanceAsync();
        Console.WriteLine($"Top trip by distance: {topDistance.Rows[0]["trip_distance"]} miles");

        var topDuration = await repo.GetTopTripDurationAsync();
        Console.WriteLine($"Longest trip in minutes: {topDuration.Rows[0]["TripDurationMinutes"]}");

        var search = await repo.SearchByPULocationAsync(237);
        Console.WriteLine($"Trips from PULocationID 237: {search.Rows.Count}");
    }
}    

