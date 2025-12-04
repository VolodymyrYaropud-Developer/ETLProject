using CsvHelper;
using ETLProject.Domain;

namespace ETLProject.Interfaces
{
    public interface ICsvRowMapper
    {
        Task<IEnumerable<ParsedRow>> ReadCsvAsync(string path);
        ParsedRow MapRow(CsvReader csv);

    }
}
