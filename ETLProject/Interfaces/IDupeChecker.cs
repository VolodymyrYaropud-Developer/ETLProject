using ETLProject.Domain;

namespace ETLProject.Interfaces
{
    public interface IDupeChecker
    {
        bool IsDuplicate(ParsedRow row);
    }
}
