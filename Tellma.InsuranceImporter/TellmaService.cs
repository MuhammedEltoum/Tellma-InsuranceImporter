using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Data;
using System.Net.Sockets;
using Tellma.Api.Dto;
using Tellma.Client;
using Tellma.InsuranceImporter.Enums;
using Tellma.Model.Application;

namespace Tellma.InsuranceImporter
{
    public class TellmaService : ITellmaService
    {
        private const int DefaultPageSize = 500;
        private const int BatchSize = 1000;
        private const int ChunkSize = 200;
        private const int MaxFilterLength = 1024;
        private const string CreatedByFilter = "CreatedBy.Id = 77";

        private readonly TellmaClient _client;
        private readonly ILogger<TellmaService> _logger;

        public TellmaService(ILogger<TellmaService> logger, IOptions<TellmaOptions> options)
        {
            if (options?.Value == null)
            {
                throw new ArgumentNullException(nameof(options), "TellmaOptions cannot be null");
            }

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _client = new TellmaClient(
                baseUrl: "https://web.tellma.com",
                authorityUrl: "https://web.tellma.com",
                clientId: options.Value.ClientId,
                clientSecret: options.Value.ClientSecret
            );
        }

        public async Task SaveExchangeRates(int tenantId, List<ExchangeRateForSave> exchangeRates,
            CancellationToken cancellationToken = default)
        {
            await RetryAsync(async () =>
            {
                var tellmaClient = GetApplicationClient(tenantId);

                try
                {
                    await tellmaClient.ExchangeRates.Save(exchangeRates);
                    _logger.LogInformation("Exchange rates created!");
                }
                catch (Exception ex)
                {
                    LogTellmaError(ex);
                    throw;
                }
                return 0;
            }, nameof(SaveExchangeRates));
        }

        public async Task<int> GetIdByCodeAsync(int tenantId, string clientProperty, string code,
            int? definitionId = null, bool? isBankAccount = false, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () => {
                if (string.IsNullOrWhiteSpace(code))
                {
                    throw new ArgumentException("Code cannot be null or empty", nameof(code));
                }

                var tellmaClient = GetApplicationClient(tenantId);
                var crudClient = GetCrudClient(tellmaClient, clientProperty, definitionId);

                var filter = BuildCodeFilter(code, clientProperty, isBankAccount);
                var getArgs = new GetArguments { Filter = filter, Top = 1 };

                var entities = await GetEntitiesAsync(crudClient, getArgs, cancellationToken);

                foreach (var entity in entities)
                {
                    var idProp = entity.GetType().GetProperty("Id");
                    if (idProp != null)
                    {
                        return (int)idProp.GetValue(entity);
                    }
                }

                _logger.LogError("No definition found for {ClientProperty} with code {Code}", clientProperty, code);
                return 0;

            }, nameof(GetIdByCodeAsync));
        }

        public async Task<List<object>> GetClientEntities(int tenantId, string clientProperty,
            int? entityDefinitionId = null, string? filter = null, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                var tellmaClient = GetApplicationClient(tenantId);
                var crudClient = GetCrudClient(tellmaClient, clientProperty, entityDefinitionId);

                var entities = new List<object>();
                int skip = 0;

                while (true)
                {
                    var getArgs = new GetArguments
                    {
                        Filter = filter,
                        Top = DefaultPageSize,
                        Skip = skip
                    };

                    var batch = await GetEntitiesAsync(crudClient, getArgs, cancellationToken);

                    if (batch.Count == 0)
                    {
                        break;
                    }

                    entities.AddRange(batch);

                    if (batch.Count < DefaultPageSize)
                    {
                        break;
                    }

                    skip += DefaultPageSize;
                    await Task.Delay(100, cancellationToken);
                }

                return entities;
            }, nameof(GetClientEntities));
        }

        public async Task<List<Agent>> SyncAgents(int tenantId, string definitionCode,
            List<Agent> dbAgents, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                if (string.IsNullOrWhiteSpace(definitionCode))
                {
                    throw new ArgumentException("Definition code cannot be null or empty", nameof(definitionCode));
                }

                var dbAgentsCopy = dbAgents
                    .Where(agent => !string.IsNullOrWhiteSpace(agent.Code))
                    .ToList();

                if (dbAgentsCopy.Count == 0)
                {
                    _logger.LogDebug("No agents with valid codes to sync");
                    return new List<Agent>();
                }

                int agentDefinitionId = await GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.AgentDefinitions.AsString(), definitionCode,
                    cancellationToken: cancellationToken);

                string batchFilter = BuildBatchFilter(dbAgentsCopy, definitionCode);
                batchFilter = TruncateFilterIfTooLong(batchFilter);

                var agentsObjectResult = await GetClientEntities(tenantId,
                    TellmaClientProperty.Agents.AsString(), agentDefinitionId,
                    batchFilter, cancellationToken);

                var existingAgents = agentsObjectResult.ConvertAll(agent => (Agent)agent);

                FilterOutExistingAgents(dbAgentsCopy, existingAgents, definitionCode);

                if (dbAgentsCopy.Count == 0 && definitionCode != "InsuranceAgent")
                {
                    _logger.LogDebug("{DefinitionCode}/{DefinitionId} has no new agents!",
                        definitionCode, agentDefinitionId);
                    return existingAgents;
                }

                var (agentsToCreate, agentsToUpdate) = PrepareAgentsForSave(
                    dbAgentsCopy, existingAgents, definitionCode);

                return await ProcessAgentChanges(tenantId, agentDefinitionId, definitionCode,
                    agentsToCreate, agentsToUpdate, existingAgents, cancellationToken);

            }, nameof(SyncAgents));
        }

        public async Task<List<Agent>> SaveAgents(int tenantId, int agentDefinitionId,
            List<AgentForSave> agentsForSave, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                if (agentsForSave == null || agentsForSave.Count == 0)
                {
                    return new List<Agent>();
                }

                var tellmaClient = GetApplicationClient(tenantId);
                var createdAgents = new List<Agent>();

                try
                {
                    await tellmaClient.Agents(agentDefinitionId).Save(agentsForSave);
                    _logger.LogDebug("Agents/{DefinitionId} updated!", agentDefinitionId);

                    // Business partners don't need to be fetched back
                    if (agentDefinitionId == 103)
                    {
                        return createdAgents;
                    }

                    return await FetchSavedAgents(tellmaClient, agentDefinitionId, agentsForSave, cancellationToken);
                }
                catch (Exception ex)
                {
                    LogTellmaError(ex);
                    throw;
                }
            }, nameof(SaveAgents));
        }

        public async Task DeleteDocumentsByDefinitionId(int tenantId, int documentDefinitionId,
            CancellationToken cancellationToken = default)
        {
            var tellmaClient = GetApplicationClient(tenantId);
            var documentResult = await tellmaClient
                .Documents(documentDefinitionId)
                .GetFact(new FactArguments
                {
                    Select = "Id, State",
                    Filter = CreatedByFilter,
                    Top = 10000
                }, cancellationToken);

            var documents = documentResult.Data.Select(d => new
            {
                Id = Convert.ToInt32(d[0]),
                State = Convert.ToInt32(d[1])
            }).ToList();

            var allDocumentIds = documents.Select(d => d.Id).ToList();
            var closedDocumentIds = documents.Where(d => d.State == 1).Select(d => d.Id).ToList();

            if (closedDocumentIds.Any())
            {
                await tellmaClient.Documents(documentDefinitionId).Open(closedDocumentIds);
            }

            try
            {
                await ProcessInChunks(allDocumentIds, async chunk =>
                {
                    await tellmaClient.Documents(documentDefinitionId).DeleteByIds(chunk.ToList());
                }, ChunkSize);

                _logger.LogInformation("For tenant {TenantId}: All Documents/{DefinitionId} are deleted!",
                    tenantId, documentDefinitionId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Bulk delete failed for tenant {TenantId} Documents/{DefinitionId}",
                    tenantId, documentDefinitionId);
                throw;
            }
        }

        public async Task<DocumentForSave> GetDocumentById(int tenantId, int documentDefinitionId,
            int documentId, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                var tellmaClient = GetApplicationClient(tenantId);
                return await tellmaClient.Documents(documentDefinitionId).GetByIdForSave(documentId);

            }, nameof(GetDocumentById));
        }

        public async Task<List<Document>> SaveDocuments(int tenantId, int documentDefinitionId,
            List<DocumentForSave> documents, CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                if (documents == null || documents.Count == 0)
                {
                    return new List<Document>();
                }

                CleanDocumentEntries(documents);
                var tellmaClient = GetApplicationClient(tenantId);
                var newDocuments = new List<Document>();

                try
                {
                    await ProcessInBatches(documents, async batch =>
                    {
                        await tellmaClient.Documents(documentDefinitionId).Save(batch.ToList());
                    }, BatchSize);

                    _logger.LogInformation("Documents created!");

                    var (minSerial, maxSerial) = GetDocumentSerialRange(documents);
                    string documentsFilter = $"State = 0 AND SerialNumber >= {minSerial} AND SerialNumber <= {maxSerial}";

                    return await FetchSavedDocuments(tellmaClient, documentDefinitionId,
                        documentsFilter, documents.Count, cancellationToken);
                }
                catch (Exception ex)
                {
                    LogTellmaError(ex);
                    throw;
                }
            }, nameof(SaveDocuments));
        }

        public async Task CloseDocuments(int tenantId, int documentDefinitionId,
            List<int> documentIds, CancellationToken cancellationToken = default)
        {
            await RetryAsync(async () =>
            {
                if (documentIds == null || documentIds.Count == 0)
                {
                    return 0;
                }

                var applicationClient = GetApplicationClient(tenantId);

                try
                {
                    await ProcessInBatches(documentIds, async batch =>
                    {
                        await applicationClient.Documents(documentDefinitionId).Close(batch.ToList());
                    }, BatchSize);

                    _logger.LogInformation("Documents closed!");
                }
                catch (Exception ex)
                {
                    LogTellmaError(ex);
                    throw;
                }
                return 0;
            }, nameof(CloseDocuments));
        }

        public async Task<int> GetAgentMaxSerialNumber(int tenantId, int agentDefinitionId,
            CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                var tellmaClient = GetApplicationClient(tenantId);

                var result = await tellmaClient
                    .Agents(agentDefinitionId)
                    .GetEntities(new Request<GetArguments>
                    {
                        Arguments = new GetArguments
                        {
                            Top = 1,
                            OrderBy = "Code desc"
                        }
                    }, cancellationToken);

                if (result.Data.Count > 0 && result.Data.First().Code.Length > 2)
                {
                    var code = result.Data.First().Code;
                    if (int.TryParse(code.Substring(2), out int serial))
                    {
                        return serial;
                    }
                }
                return 0;
            }, nameof(GetAgentMaxSerialNumber));
        }

        public async Task DeleteAgentsByDefinition(int tenantId, int agentDefinitionId,
            CancellationToken cancellationToken = default)
        {
            var tellmaClient = GetApplicationClient(tenantId);

            var agentsResult = await tellmaClient
                .Agents(agentDefinitionId)
                .GetFact(new FactArguments
                {
                    Select = "Id",
                    OrderBy = "Id Desc",
                    Filter = CreatedByFilter,
                    Top = 5000
                }, cancellationToken);

            var agentIds = agentsResult.Data.Select(d => Convert.ToInt32(d[0])).ToList();

            try
            {
                int counter = 1;
                await ProcessInChunks(agentIds, async chunk =>
                {
                    _logger.LogInformation("Deleting batch {BatchNumber} of {TotalBatches} for Agent/{DefinitionId}",
                        counter, (int)Math.Ceiling((double)agentIds.Count / ChunkSize), agentDefinitionId);

                    await tellmaClient.Agents(agentDefinitionId).DeleteByIds(chunk.ToList());
                    counter++;
                }, ChunkSize);

                _logger.LogInformation("For tenant {TenantId}: All Agents/{DefinitionId} are deleted!",
                    tenantId, agentDefinitionId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Bulk delete failed for tenant {TenantId} Agents/{DefinitionId}",
                    tenantId, agentDefinitionId);
            }
        }

        public async Task<SettingsForClient> GetTenantProfile(int tenantId,
            CancellationToken cancellationToken = default)
        {
            return await RetryAsync(async () =>
            {
                var tellmaClient = GetApplicationClient(tenantId);
                var tenantSettings = await tellmaClient.GeneralSettings.SettingsForClient();
                return tenantSettings.Data;
            }, nameof(GetTenantProfile));
        }

        public void LogTellmaError(Exception ex)
        {
            _logger.LogError("Tellma API Error {Error}", ex.ToString());
        }

        #region Private Helper Methods

        private TellmaClient.ApplicationClientBehavior GetApplicationClient(int tenantId)
        {
            return _client.Application(tenantId);
        }

        private object GetCrudClient(TellmaClient.ApplicationClientBehavior client,
            string propertyName, int? definitionId = null)
        {
            if (definitionId.HasValue)
            {
                var method = client.GetType().GetMethod(propertyName, new[] { typeof(int) });
                if (method == null)
                {
                    throw new ArgumentException($"No client method named {propertyName} found that accepts an integer parameter.");
                }
                return method.Invoke(client, new object[] { definitionId.Value });
            }

            var prop = client.GetType().GetProperty(propertyName);
            if (prop == null)
            {
                throw new ArgumentException($"No client property named {propertyName} found.");
            }
            return prop.GetValue(client);
        }

        private async Task<List<object>> GetEntitiesAsync(object crudClient, GetArguments getArgs,
            CancellationToken cancellationToken)
        {
            var getEntitiesMethod = crudClient.GetType().GetMethod("GetEntities");
            var getEntitiesArgs = new object[]
            {
                new Request<GetArguments> { Arguments = getArgs },
                cancellationToken
            };

            var task = (Task)getEntitiesMethod.Invoke(crudClient, getEntitiesArgs);
            await task.ConfigureAwait(false);

            var resultProperty = task.GetType().GetProperty("Result");
            var result = resultProperty.GetValue(task);
            var dataProp = result.GetType().GetProperty("Data");

            return ((System.Collections.IEnumerable)dataProp.GetValue(result)).Cast<object>().ToList();
        }

        private string BuildCodeFilter(string code, string clientProperty, bool? isBankAccount)
        {
            string filterType = clientProperty == nameof(TellmaClient.ApplicationClientBehavior.EntryTypes)
                ? "Concept="
                : "Code=";

            if (isBankAccount == true)
            {
                filterType = "Text3=";
            }

            return $"{filterType}'{code}'";
        }

        private string BuildBatchFilter(List<Agent> agents, string definitionCode)
        {
            if (definitionCode == TellmaEntityCode.BusinessPartner.AsString())
            {
                return string.Join(" OR ", agents.Select(bp =>
                    $"(Agent1Id={bp.Agent1Id} AND Agent2Id={bp.Agent2Id} AND Lookup1Id={bp.Lookup1Id})"));
            }

            var codes = agents.Select(t => t.Code).Distinct();
            return string.Join(" OR ", codes.Select(code => $"Code='{code}'"));
        }

        private string TruncateFilterIfTooLong(string filter)
        {
            return filter?.Length < MaxFilterLength ? filter : null;
        }

        private void FilterOutExistingAgents(List<Agent> dbAgents, List<Agent> existingAgents, string definitionCode)
        {
            bool isBusinessPartner = definitionCode == TellmaEntityCode.BusinessPartner.AsString();

            for (int i = dbAgents.Count - 1; i >= 0; i--)
            {
                var dbAgent = dbAgents[i];
                var existingAgent = FindMatchingExistingAgent(dbAgent, existingAgents, isBusinessPartner);

                if (existingAgent != null)
                {
                    if (isBusinessPartner)
                    {
                        dbAgents.RemoveAll(a => a.Agent1Id == dbAgent.Agent1Id && a.Lookup1Id == dbAgent.Lookup1Id);
                    }
                    else
                    {
                        dbAgents.RemoveAll(a => a.Code == dbAgent.Code);
                    }
                }
                else if (isBusinessPartner)
                {
                    // Update existing business partner with matching properties
                    var matchingAgent = existingAgents.FirstOrDefault(a =>
                        a.Agent1Id == dbAgent.Agent1Id && a.Lookup1Id == dbAgent.Lookup1Id);

                    if (matchingAgent != null)
                    {
                        dbAgent.Id = matchingAgent.Id;
                        dbAgent.Code = matchingAgent.Code;
                    }
                }
            }
        }

        private Agent FindMatchingExistingAgent(Agent dbAgent, List<Agent> existingAgents, bool isBusinessPartner)
        {
            if (isBusinessPartner)
            {
                return existingAgents.FirstOrDefault(a =>
                    a.Agent1Id == dbAgent.Agent1Id &&
                    a.Agent2Id == dbAgent.Agent2Id &&
                    a.Lookup1Id == dbAgent.Lookup1Id);
            }

            return existingAgents.FirstOrDefault(a =>
                a.Code == dbAgent.Code &&
                (a.Name == dbAgent.Name || a.Name == $"{dbAgent.Name} - {dbAgent.Code}" ||
                 $"{a.Code}: {a.Name}" == $"{dbAgent.Code}: {dbAgent.Name}") &&
                (a.Name2 == dbAgent.Name2 || a.Name2 == $"{dbAgent.Name2} - {dbAgent.Code}" ||
                 $"{a.Code}: {a.Name}" == $"{dbAgent.Code}: {dbAgent.Name}") &&
                a.Agent1Id == dbAgent.Agent1Id &&
                a.Agent2Id == dbAgent.Agent2Id &&
                a.Lookup1Id == dbAgent.Lookup1Id &&
                a.Lookup2Id == dbAgent.Lookup2Id &&
                a.FromDate?.ToString("yyyy-MM-dd") == dbAgent.FromDate?.ToString("yyyy-MM-dd") &&
                a.ToDate?.ToString("yyyy-MM-dd") == dbAgent.ToDate?.ToString("yyyy-MM-dd") &&
                (a.Description == dbAgent.Description ||
                 (string.IsNullOrWhiteSpace(a.Description) && string.IsNullOrWhiteSpace(dbAgent.Description))) &&
                a.Description2 == dbAgent.Description2);
        }

        private (List<AgentForSave> ToCreate, List<AgentForSave> ToUpdate) PrepareAgentsForSave(
            List<Agent> dbAgents, List<Agent> existingAgents, string definitionCode)
        {
            var agentsToCreate = new List<AgentForSave>();
            var agentsToUpdate = new List<AgentForSave>();

            foreach (var dbAgent in dbAgents)
            {
                var existingAgent = existingAgents.FirstOrDefault(a => a.Code == dbAgent.Code);
                var agentForSave = CreateAgentForSave(dbAgent, existingAgent, definitionCode);

                if (existingAgent == null)
                {
                    agentsToCreate.Add(agentForSave);
                }
                else
                {
                    agentForSave.Id = existingAgent.Id;
                    agentsToUpdate.Add(agentForSave);
                }
            }

            return (agentsToCreate, agentsToUpdate);
        }

        private AgentForSave CreateAgentForSave(Agent dbAgent, Agent existingAgent, string definitionCode)
        {
            string code = dbAgent.Code;
            string agentName = string.Empty;

            switch (definitionCode)
            {
                case "InsuranceAgent":
                    agentName = $"{dbAgent.Name} - {dbAgent.Code}";
                    break;

                case "InsuranceContract":
                    agentName = (dbAgent.Name == existingAgent?.Name) ? existingAgent.Name : dbAgent.Name;
                    return new AgentForSave
                    {
                        Code = code,
                        Name = agentName,
                        Name2 = agentName,
                        Lookup1Id = dbAgent.Lookup1Id,
                        Lookup3Id = dbAgent.Lookup3Id,
                        Agent2Id = dbAgent.Agent2Id,
                        Description = dbAgent.Description,
                        Description2 = dbAgent.Description2,
                        FromDate = CalculateFromDate(dbAgent, existingAgent),
                        ToDate = CalculateToDate(dbAgent, existingAgent)
                    };

                case "TradeReceivableAccount":
                    agentName = (dbAgent.Name == existingAgent?.Name) ? existingAgent.Name : dbAgent.Name;
                    return new AgentForSave
                    {
                        Code = code,
                        Name = agentName,
                        Name2 = agentName,
                        Agent1Id = dbAgent.Agent1Id,
                        Agent2Id = dbAgent.Agent2Id,
                        Lookup2Id = dbAgent.Lookup2Id
                    };

                case "BusinessPartner":
                    // This would require the serial number logic from the original method
                    agentName = $"{code}: {dbAgent.Name}";
                    return new AgentForSave
                    {
                        Code = code,
                        Name = agentName,
                        Name2 = agentName,
                        Agent1Id = dbAgent.Agent1Id,
                        Agent2Id = dbAgent.Agent2Id,
                        Lookup1Id = dbAgent.Lookup1Id
                    };

                default:
                    throw new InvalidOperationException($"Unknown agent definition code: {definitionCode}");
            }

            return new AgentForSave
            {
                Code = code,
                Name = agentName,
                Name2 = agentName
            };
        }

        private DateTime? CalculateFromDate(Agent dbAgent, Agent existingAgent)
        {
            if (existingAgent != null && dbAgent.FromDate.HasValue && existingAgent.FromDate.HasValue)
            {
                return dbAgent.FromDate <= existingAgent.FromDate ? dbAgent.FromDate : existingAgent.FromDate;
            }
            return dbAgent.FromDate ?? existingAgent?.FromDate;
        }

        private DateTime? CalculateToDate(Agent dbAgent, Agent existingAgent)
        {
            if (existingAgent != null && dbAgent.ToDate.HasValue && existingAgent.ToDate.HasValue)
            {
                return dbAgent.ToDate >= existingAgent.ToDate ? dbAgent.ToDate : existingAgent.ToDate;
            }
            return dbAgent.ToDate ?? existingAgent?.ToDate;
        }

        private async Task<List<Agent>> ProcessAgentChanges(int tenantId, int agentDefinitionId,
            string definitionCode, List<AgentForSave> agentsToCreate, List<AgentForSave> agentsToUpdate,
            List<Agent> existingAgents, CancellationToken cancellationToken)
        {
            if (agentsToCreate.Count == 0 && agentsToUpdate.Count == 0)
            {
                _logger.LogDebug("{DefinitionCode} Agent sync completed! No changes detected.", definitionCode);
                return existingAgents;
            }

            LogAgentChanges(agentsToCreate.Count, agentsToUpdate.Count, definitionCode);

            var allAgentsForSave = agentsToCreate.Concat(agentsToUpdate).ToList();
            var createdAgents = await SaveAgents(tenantId, agentDefinitionId, allAgentsForSave, cancellationToken);

            _logger.LogDebug("{DefinitionCode} Agent sync completed!", definitionCode);

            // Remove updated agents to avoid duplicates
            existingAgents.RemoveAll(a => agentsToUpdate.Any(updated => updated.Id == a.Id));
            existingAgents.AddRange(createdAgents);

            return existingAgents;
        }

        private void LogAgentChanges(int createCount, int updateCount, string definitionCode)
        {
            if (updateCount > 0)
            {
                _logger.LogInformation("Updating {Count} existing {DefinitionCode} agents...",
                    updateCount, definitionCode);
            }

            if (createCount > 0)
            {
                _logger.LogInformation("Creating {Count} new {DefinitionCode} agents...",
                    createCount, definitionCode);
            }
        }

        private async Task<List<Agent>> FetchSavedAgents(TellmaClient.ApplicationClientBehavior tellmaClient,
            int agentDefinitionId, List<AgentForSave> agentsForSave, CancellationToken cancellationToken)
        {
            string agentsFilter = string.Join(" or ", agentsForSave.Select(a => $"Code = '{a.Code}'"));
            agentsFilter = TruncateFilterIfTooLong(agentsFilter);

            if (!string.IsNullOrEmpty(agentsFilter))
            {
                var result = await tellmaClient.Agents(agentDefinitionId).GetEntities(
                    new Request<GetArguments>
                    {
                        Arguments = new GetArguments
                        {
                            Top = agentsForSave.Count,
                            OrderBy = "Id desc",
                            Filter = agentsFilter
                        }
                    }, cancellationToken);

                return result.Data.ToList();
            }

            return await FetchAllAgentsPaginated(tellmaClient, agentDefinitionId, cancellationToken);
        }

        private async Task<List<Agent>> FetchAllAgentsPaginated(TellmaClient.ApplicationClientBehavior tellmaClient,
            int agentDefinitionId, CancellationToken cancellationToken)
        {
            var allAgents = new List<Agent>();
            int skip = 0;

            while (true)
            {
                var result = await tellmaClient.Agents(agentDefinitionId).GetEntities(
                    new Request<GetArguments>
                    {
                        Arguments = new GetArguments
                        {
                            Top = DefaultPageSize,
                            Skip = skip
                        }
                    }, cancellationToken);

                allAgents.AddRange(result.Data);

                if (result.Data.Count < DefaultPageSize)
                {
                    break;
                }

                skip += DefaultPageSize;
            }

            return allAgents;
        }

        private void CleanDocumentEntries(List<DocumentForSave> documents)
        {
            foreach (var doc in documents)
            {
                if (doc.Lines?.Count > 0)
                {
                    doc.Lines[0].Entries = doc.Lines[0].Entries
                        .Where(e => e.Value != 0 || e.MonetaryValue != 0)
                        .ToList();
                }
            }
        }

        private (int minSerial, int maxSerial) GetDocumentSerialRange(List<DocumentForSave> documents)
        {
            var serials = documents.Where(d => d.SerialNumber.HasValue).Select(d => d.SerialNumber.Value);
            return serials.Any() ? (serials.Min(), serials.Max()) : (0, 0);
        }

        private async Task<List<Document>> FetchSavedDocuments(TellmaClient.ApplicationClientBehavior tellmaClient,
            int documentDefinitionId, string filter, int expectedCount, CancellationToken cancellationToken)
        {
            var documents = new List<Document>();
            int skip = 0;
            int pageSize = Math.Min(expectedCount, 1000);

            while (true)
            {
                var result = await tellmaClient.Documents(documentDefinitionId).GetFact(
                    new Request<FactArguments>
                    {
                        Arguments = new FactArguments
                        {
                            Select = "Id, SerialNumber",
                            Top = pageSize,
                            Skip = skip,
                            OrderBy = "Id desc",
                            Filter = filter
                        }
                    }, cancellationToken);

                documents.AddRange(result.Data.Select(doc => new Document
                {
                    Id = Convert.ToInt32(doc[0]),
                    SerialNumber = Convert.ToInt32(doc[1])
                }));

                if (result.Data.Count < pageSize)
                {
                    break;
                }

                skip += pageSize;
            }

            return documents;
        }

        private async Task ProcessInBatches<T>(IEnumerable<T> items, Func<IEnumerable<T>, Task> batchAction, int batchSize)
        {
            var batches = items.Chunk(batchSize);

            foreach (var batch in batches)
            {
                await batchAction(batch);
                Task.Delay(50);
            }
        }

        private async Task ProcessInChunks<T>(IEnumerable<T> items, Func<IEnumerable<T>, Task> chunkAction, int chunkSize)
        {
            var chunks = items.Chunk(chunkSize);

            foreach (var chunk in chunks)
            {
                await chunkAction(chunk);
                Task.Delay(50);
            }
        }

        private async Task<T> RetryAsync<T>(Func<Task<T>> operation, string methodName)
        {
            int retryCount = 0;
            TimeSpan delay = TimeSpan.FromSeconds(2);

            while (true)
            {
                try
                {
                    return await operation();
                }
                catch (Exception ex) when (
                    (ex is HttpRequestException ||
                     ex is IOException ||
                     (ex is SocketException sockEx && sockEx.ErrorCode == 10054))
                    && retryCount < 3)
                {
                    retryCount++;
                    Console.WriteLine($"Retry {retryCount} for {methodName} after {delay.TotalSeconds}s");
                    await Task.Delay(delay);
                    delay = TimeSpan.FromSeconds(delay.TotalSeconds * 1.5);
                }
            }
        }

        #endregion
    }
}