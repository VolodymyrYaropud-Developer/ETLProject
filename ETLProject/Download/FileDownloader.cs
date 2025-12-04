using ETLProject.Interfaces;

namespace ETLProject.Download
{
    public class FileDownloader : IFileDownloader, IDisposable
    {
        private readonly HttpClient _httpClient;

        public FileDownloader(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        public async Task DownloadToFileAsync()
        {
            try
            {
                Console.WriteLine($"Attempting to download file from: {GlobalSettings.directDownloadUrl}");

                using (var response = await _httpClient.GetAsync(GlobalSettings.directDownloadUrl))
                {
                    response.EnsureSuccessStatusCode();
                    using (var streamToReadFrom = await response.Content.ReadAsStreamAsync())
                    using (var streamToWriteTo = File.Open(GlobalSettings.InputCsvPath, FileMode.Create))
                    {
                        await streamToReadFrom.CopyToAsync(streamToWriteTo);
                        Console.WriteLine($"File successfully downloaded to: {GlobalSettings.InputCsvPath}");
                    }
                }
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine($"\nError HTTP Request failed: {e.Message}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"\nError An unexpected error occurred: {e.Message}");
            }
        }

        public void Dispose() => _httpClient?.Dispose();
    }

}
