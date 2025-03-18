namespace TaxiDataETL.Core.Abstractions;

public interface IDuplicateChecker<T>
{
    bool IsDuplicate(T item);
}