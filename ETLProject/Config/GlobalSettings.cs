public static class GlobalSettings
{
    public const int BATCH_SIZE = 50_000;
    public const string directDownloadUrl = "https://drive.google.com/uc?export=download&id=1l2ARvh1-tJBqzomww45TrGtIh5j8Vud4";
    public const string ConnectionString = "Server=COMPUTER\\SQLEXPRESS;Database=ETLProjectDB;Integrated Security=true;TrustServerCertificate=true;";
    public const string InputCsvPath = "input.csv";
    public const string DuplicatesCsvPath = "duplicates.csv";
    public static readonly TimeZoneInfo SourceTz = GetEasternTimeZone();

    private static TimeZoneInfo GetEasternTimeZone()
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        }
        catch
        {
            throw new Exception("Could not find Eastern time zone on this machine.");
        }

    }
}