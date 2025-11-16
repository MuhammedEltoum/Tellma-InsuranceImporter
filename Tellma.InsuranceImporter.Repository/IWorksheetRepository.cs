using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public interface IWorksheetRepository<T>
    {
        Task<List<T>> GetWorksheets(CancellationToken token);
        Task<List<T>> GetMappingAccounts(CancellationToken token);
        Task UpdateDocumentIds(string tenantCode, IEnumerable<T> worksheets, CancellationToken token);
        Task UpdateImportedWorksheets(string tenantCode, IEnumerable<T> worksheets, CancellationToken token);
    }
}