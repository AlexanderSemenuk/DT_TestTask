namespace TaxiDataETL.Core.Abstractions;

public interface IDataTransformer<TInput, TOutput>
{
    TOutput Transform(TInput input);
    (List<TOutput> uniqueRecords, List<TInput> duplicateRecords) ProcessChunk(
        List<TInput> chunk, 
        IDuplicateChecker<TOutput> duplicateChecker);
}