using ETLProject.Domain;
using ETLProject.Interfaces;

namespace ETLProject.Dedupe
{
    public class HashSetDupeChecker : IDupeChecker
    {
        private readonly HashSet<string> _keys = new();

        public bool IsDuplicate(ParsedRow row)
        {
            string key = $"{row.PickupUtc.Ticks}|{row.DropoffUtc.Ticks}|{row.PassengerCount}";
            return !_keys.Add(key);
        }
    }

}
