using ETLProject.Domain;
using System.Data;

namespace ETLProject.Interfaces
{
    public interface IDataTableFactory
    {
        DataTable Create();
        DataRow CreateRow(DataTable table, ParsedRow row);
    }
}
