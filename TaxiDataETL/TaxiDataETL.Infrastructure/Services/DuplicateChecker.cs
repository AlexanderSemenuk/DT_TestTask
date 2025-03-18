using System.Collections.Concurrent;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Infrastructure.Services;

public class DuplicateChecker : IDuplicateChecker<TaxiTrip>
{
    private readonly ConcurrentDictionary<string, byte> _uniqueKeys = new();
    
    
    public bool IsDuplicate(TaxiTrip trip)
    {
        if (trip == null)
        {
            Log.Warning("Cannot check for duplicates on a null trip");
            return false;
        }
        
        try
        {
            var key = GetUniqueKey(trip);
            var isDuplicate = !_uniqueKeys.TryAdd(key, 1);
            
            if (isDuplicate)
            {
                Log.Warning("Duplicate found with key: {Key}", key);
            }
            
            return isDuplicate;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error checking for duplicate: {Trip}", trip);
            return false;
        }
    }
    
    private string GetUniqueKey(TaxiTrip trip)
    {
        if (trip == null)
        {
            throw new ArgumentNullException(nameof(trip));
        }
        
        return $"{trip.PickupDateTime:yyyy-MM-dd HH:mm:ss}|{trip.DropoffDateTime:yyyy-MM-dd HH:mm:ss}|{trip.PassengerCount ?? 0}";
    }
}