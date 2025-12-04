using CsvHelper;
using CsvHelper.Configuration;
using ETLProject.Domain;
using ETLProject.Interfaces;
using System.Globalization;

namespace ETLProject.Csv
{
    public class CsvProcessor
    {
        private readonly ICsvRowMapper _mapper;
        private readonly IDataTableFactory _tableFactory;
        private readonly ITripRepository _repo;
        private readonly IDupeChecker _dupeChecker;

        public CsvProcessor(
            ICsvRowMapper mapper,
            IDataTableFactory tableFactory,
            ITripRepository repo,
            IDupeChecker dupeChecker)
        {
            _mapper = mapper;
            _tableFactory = tableFactory;
            _repo = repo;
            _dupeChecker = dupeChecker;
        }

        public async Task ProcessAsync(string inputCsv, string duplicatesCsv)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                TrimOptions = TrimOptions.Trim
            };

            using var reader = new StreamReader(inputCsv);
            using var csv = new CsvReader(reader, config);

            using var dupWriter = new StreamWriter(duplicatesCsv);
            using var dupCsv = new CsvWriter(dupWriter, CultureInfo.InvariantCulture);
            dupCsv.WriteHeader<OriginalRow>();
            dupCsv.NextRecord();

            var batch = _tableFactory.Create();
            int batchCount = 0;

            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var row = _mapper.MapRow(csv);
                if (row == null) continue;

                if (_dupeChecker.IsDuplicate(row))
                {
                    dupCsv.WriteRecord(row.Original);
                    dupCsv.NextRecord();
                    continue;
                }

                batch.Rows.Add(_tableFactory.CreateRow(batch, row));
                batchCount++;

                if (batchCount >= 50000)
                {
                    await _repo.BulkInsertAsync(batch);
                    batch.Clear();
                    batchCount = 0;
                }
            }

            if (batch.Rows.Count > 0)
                await _repo.BulkInsertAsync(batch);
        }
    }

}
