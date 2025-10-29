using Tellma.Api.Dto;
using Tellma.Client;
using Tellma.InsuranceImporter.Contract;
using Tellma.Model.Application;

namespace Tellma.InsuranceImporter
{
    public interface ITellmaService
    {
        Task SaveExchangeRates(int tenantId, List<ExchangeRateForSave> exchangeRates, CancellationToken token);
        Task<List<object>> GetClientEntities(int tenantId, string clientProperty, int? entityDefinitionId = null, string? filter = null, CancellationToken token = default);
        Task<int> GetIdByCodeAsync(int tenantId, string clientProperty, string code, int? definitionId = null, bool? isBankAccount = false, CancellationToken token = default);
        Task<int> GetAgentMaxSerialNumber(int tenantId, int agentDefinitionId, CancellationToken token);
        Task<List<Agent>> SaveAgents(int tenantId, int agentDefinitionId, List<AgentForSave> agentForSave, CancellationToken token);
        Task<List<Agent>> SyncAgents(int tenantId, string definitionCode, List<Agent> dbAgents, CancellationToken token);
        Task DeleteAgentsByDefinition(int tenantId, int agentDefinitionId, CancellationToken token);
        Task<DocumentForSave> GetDocumentById(int tenantId, int documentDefinitionId, int documentId, CancellationToken token);
        Task<List<Document>> SaveDocuments(int tenantId, int documentDefinitionId, List<DocumentForSave> documents, CancellationToken token);
        Task CloseDocuments(int tenantId, int documentDefinitionId, List<int> documentIds, CancellationToken token);
        Task DeleteDocumentsByDefinitionId(int tenantId, int documentDefinitionId, CancellationToken token);
        void LogTellmaError(Exception ex);
    }
}