using System.Collections.Concurrent;
using Serilog;
using TaxiDataETL.Core.Abstractions;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Infrastructure.Services;

public class TripDataTransformer : IDataTransformer<TaxiTripCsvRecord, TaxiTrip>
{
    public TaxiTrip Transform(TaxiTripCsvRecord record)
{
    try
    {
        if (record == null)
        {
            Log.Warning("Null record encountered");
            return null;
        }
        
        if (record.tpep_pickup_datetime == null || record.tpep_dropoff_datetime == null)
        {
            Log.Warning("Record missing required datetime fields: {Record}", record);
            return null;
        }
        
        if (record.tpep_pickup_datetime > record.tpep_dropoff_datetime)
        {
            Log.Warning("Pickup time is after dropoff time: {Record}", record);
            return null;
        }
        
        var estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        
        var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(record.tpep_pickup_datetime.Value, estTimeZone);
        var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(record.tpep_dropoff_datetime.Value, estTimeZone);
        
        var passengerCount = record.passenger_count;
        if (passengerCount.HasValue && (passengerCount.Value < 0 || passengerCount.Value > 100))
        {
            Log.Warning("Invalid passenger count {PassengerCount}, setting to null", passengerCount);
            passengerCount = null;
        }

        var tripDistance = record.trip_distance;
        if (tripDistance.HasValue && (tripDistance.Value < 0 || tripDistance.Value > 1000))
        {
            Log.Warning("Invalid trip distance {TripDistance}, setting to null", tripDistance);
            tripDistance = null;
        }

        var fareAmount = record.fare_amount;
        if (fareAmount.HasValue && (fareAmount.Value < 0 || fareAmount.Value > 10000))
        {
            Log.Warning("Invalid fare amount {FareAmount}, setting to null", fareAmount);
            fareAmount = null;
        }

        var tipAmount = record.tip_amount;
        if (tipAmount.HasValue && (tipAmount.Value < 0 || tipAmount.Value > 1000))
        {
            Log.Warning("Invalid tip amount {TipAmount}, setting to null", tipAmount);
            tipAmount = null;
        }
        
        var storeAndFwdFlag = record.store_and_fwd_flag?.Trim();
        if (storeAndFwdFlag != null)
        {
            if (storeAndFwdFlag.Equals("Y", StringComparison.OrdinalIgnoreCase))
                storeAndFwdFlag = "Yes";
            else if (storeAndFwdFlag.Equals("N", StringComparison.OrdinalIgnoreCase))
                storeAndFwdFlag = "No";
            else
            {
                Log.Warning("Invalid store_and_fwd_flag value: {Flag}, setting to No", storeAndFwdFlag);
                storeAndFwdFlag = "No";
            }
        }
        else
        {
            storeAndFwdFlag = "No";
        }
        
        var puLocationId = record.PULocationID;
        if (puLocationId.HasValue && (puLocationId.Value <= 0 || puLocationId.Value > 10000))
        {
            Log.Warning("Invalid PULocationID {PULocationID}, setting to null", puLocationId);
            puLocationId = null;
        }

        var doLocationId = record.DOLocationID;
        if (doLocationId.HasValue && (doLocationId.Value <= 0 || doLocationId.Value > 10000))
        {
            Log.Warning("Invalid DOLocationID {DOLocationID}, setting to null", doLocationId);
            doLocationId = null;
        }

        return new TaxiTrip
        {
            PickupDateTime = pickupUtc,
            DropoffDateTime = dropoffUtc,
            PassengerCount = passengerCount,
            TripDistance = tripDistance,
            StoreAndFwdFlag = storeAndFwdFlag,
            PULocationID = puLocationId,
            DOLocationID = doLocationId,
            FareAmount = fareAmount,
            TipAmount = tipAmount
        };
    }
    catch (Exception ex)
    {
        Log.Error(ex, "Error transforming record: {Record}", record);
        return null;
    }
}
        
    public (List<TaxiTrip> uniqueRecords, List<TaxiTripCsvRecord> duplicateRecords) ProcessChunk(
        List<TaxiTripCsvRecord> chunk, 
        IDuplicateChecker<TaxiTrip> duplicateChecker)
    {
        var uniqueRecords = new List<TaxiTrip>();
        var duplicateRecords = new List<TaxiTripCsvRecord>();
    
        var results = new ConcurrentBag<(TaxiTrip trip, TaxiTripCsvRecord originalRecord, bool isDuplicate)>();
    
        Parallel.ForEach(chunk, record => 
        {
            try
            {
                if (record == null) return;
            
                var trip = Transform(record);
                
                if (trip == null) return;
            
                var isDuplicate = duplicateChecker.IsDuplicate(trip);
            
                results.Add((trip, record, isDuplicate));
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error processing record: {Record}", record);
            }
        });
    
        foreach (var (trip, originalRecord, isDuplicate) in results)
        {
            if (trip == null) continue; 
        
            if (isDuplicate)
                duplicateRecords.Add(originalRecord);
            else
                uniqueRecords.Add(trip);
        }
    
        Log.Debug("Processed chunk: {TotalCount} records, {UniqueCount} unique, {DuplicateCount} duplicates", 
            chunk.Count, uniqueRecords.Count, duplicateRecords.Count);
        
        return (uniqueRecords, duplicateRecords);
    }
}