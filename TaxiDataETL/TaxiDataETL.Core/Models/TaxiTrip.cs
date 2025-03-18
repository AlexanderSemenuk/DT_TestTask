namespace TaxiDataETL.Core.Models;

public class TaxiTrip
{
    public int Id { get; set; }
    public DateTime PickupDateTime { get; set; }
    public DateTime DropoffDateTime { get; set; }
    public int? PassengerCount { get; set; }
    public decimal? TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; }
    public int? PULocationID { get; set; }
    public int? DOLocationID { get; set; }
    public decimal? FareAmount { get; set; }
    public decimal? TipAmount { get; set; }
    
    public int TripDurationSeconds => (int)(DropoffDateTime - PickupDateTime).TotalSeconds;
}