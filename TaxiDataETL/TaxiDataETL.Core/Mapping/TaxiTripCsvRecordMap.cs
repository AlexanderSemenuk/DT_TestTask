using CsvHelper.Configuration;
using TaxiDataETL.Core.Models;

namespace TaxiDataETL.Core.Mapping;

public sealed class TaxiTripCsvRecordMap : ClassMap<TaxiTripCsvRecord>
{
    public TaxiTripCsvRecordMap()
    {
        Map(m => m.tpep_pickup_datetime).Name("tpep_pickup_datetime")
            .TypeConverterOption.Format(new[] { "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy h:mm:ss tt" })
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.tpep_dropoff_datetime).Name("tpep_dropoff_datetime")
            .TypeConverterOption.Format(new[] { "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy h:mm:ss tt" })
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
        
        Map(m => m.passenger_count).Name("passenger_count")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.trip_distance).Name("trip_distance")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.store_and_fwd_flag).Name("store_and_fwd_flag")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.PULocationID).Name("PULocationID")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.DOLocationID).Name("DOLocationID")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.fare_amount).Name("fare_amount")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
            
        Map(m => m.tip_amount).Name("tip_amount")
            .TypeConverterOption.NullValues(string.Empty, "NULL", "null");
    }
}