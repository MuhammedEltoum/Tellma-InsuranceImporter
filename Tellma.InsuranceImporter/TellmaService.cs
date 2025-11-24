using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Tellma.Api.Dto;
using Tellma.Client;
using Tellma.InsuranceImporter.Enums;
using Tellma.Model.Application;
using Tellma.Utilities.EmailLogger;

namespace Tellma.InsuranceImporter
{
    public class TellmaService : ITellmaService
    {
        private readonly TellmaClient _client;
        private readonly ILogger<TellmaService> _logger;
        public TellmaService(ILogger<TellmaService> logger, IOptions<TellmaOptions> options)
        {
            _client = new TellmaClient(
                baseUrl: "https://web.tellma.com",
                authorityUrl: "https://web.tellma.com",
                clientId: options.Value.ClientId,
                clientSecret: options.Value.ClientSecret
                );

            _logger = logger;
        }

        public async Task SaveExchangeRates(int tenantId, List<ExchangeRateForSave> exchangeRates, CancellationToken cancellationToken)
        {
            var tellmaClient = _client.Application(tenantId);
            try
            {
                var x = await tellmaClient
                .ExchangeRates
                .Save(exchangeRates);
                _logger.LogInformation("Exchange rates created!");
            }
            catch (Exception ex)
            {
                LogTellmaError(ex);
            }
        }
        // Generic helper for ID by code using reflection for GetEntities and Id
        public async Task<int> GetIdByCodeAsync(int tenantId, string clientProperty, string code, int? definitionId = null, bool? isBankAccount = false, CancellationToken token = default)
        {
            var tellmaClient = _client.Application(tenantId);
            var crudClient = GetCrudClient(tellmaClient, clientProperty, definitionId);
            var getEntitiesMethod = crudClient.GetType().GetMethod("GetEntities");
            var filterType = clientProperty == nameof(TellmaClient.ApplicationClientBehavior.EntryTypes) ? "Concept=" : "Code=";
            if (isBankAccount.HasValue)
                filterType = isBankAccount.Value ? "Text3=" : filterType;
            var getArgs = new GetArguments { Filter = $"{filterType}'{code}'", Top = 1 };
            var getEntitiesArgs = new object[] { new Request<GetArguments> { Arguments = getArgs }, token };
            var task = (Task)getEntitiesMethod.Invoke(crudClient, getEntitiesArgs);
            await task.ConfigureAwait(false);
            var resultProperty = task.GetType().GetProperty("Result");
            var result = resultProperty.GetValue(task);
            var dataProp = result.GetType().GetProperty("Data");
            var data = (System.Collections.IEnumerable)dataProp.GetValue(result);

            foreach (var entity in data)
            {
                var idProp = entity.GetType().GetProperty("Id");
                if (idProp != null)
                    return (int)idProp.GetValue(entity);
            }

            _logger.LogError($"There is no definition for {clientProperty} on {code}!");
            return 0;
        }

        public async Task<List<object>> GetClientEntities(int tenantId, string clientProperty, int? entityDefinitionId, string? filter = null, CancellationToken cancellationToken = default)
        {
            var tellmaClient = _client.Application(tenantId);
            var crudClient = GetCrudClient(tellmaClient, clientProperty, entityDefinitionId);
            var getEntitiesMethod = crudClient.GetType().GetMethod("GetEntities");

            int pageSize = 500;
            int skip = 0;

            List<object> entities = new List<object>();
            while (true)
            {
                var getArgs = new GetArguments { Filter = $"{filter}", Top = pageSize, Skip = skip };
                var getEntitiesArgs = new object[] { new Request<GetArguments> { Arguments = getArgs }, cancellationToken };
                var task = (Task)getEntitiesMethod.Invoke(crudClient, getEntitiesArgs);
                await task.ConfigureAwait(false);
                var resultProperty = task.GetType().GetProperty("Result");
                var result = resultProperty.GetValue(task);
                var dataProp = result.GetType().GetProperty("Data");
                var data = (System.Collections.IList)dataProp.GetValue(result);
                if (data.Count == 0)
                    break;

                foreach (var entity in data)
                    entities.Add(entity);

                if (data.Count < pageSize)
                    break;

                skip += pageSize;

                await Task.Delay(100, cancellationToken);
            }
            return entities;
        }

        // Reflection-based CrudClient factory
        private object GetCrudClient(TellmaClient.ApplicationClientBehavior client, string propertyName, int? definitionId = null)
        {
            if (definitionId.HasValue)
            {
                var method = client.GetType().GetMethod(propertyName, new Type[] { typeof(int) });
                if (method == null)
                    throw new ArgumentException($"No client named {propertyName} found.");
                return method.Invoke(client, new object[] { definitionId.Value });
            }

            var prop = client.GetType().GetProperty(propertyName);
            if (prop == null)
                throw new ArgumentException($"No client named {propertyName} found.");
            return prop.GetValue(client);
        }

        public async Task<List<Agent>> SyncAgents(int tenantId, string definitionCode, List<Agent> dbAgents, CancellationToken cancellationToken)
        {
            var dbAgentsCopy = dbAgents.Where(agent => !String.IsNullOrWhiteSpace(agent.Code)).ToList();
            bool isBusinessPartnerAgent = definitionCode == TellmaEntityCode.BusinessPartner.AsString() ? true : false;
            int serial = 0;

            int agentDefinitionId = await GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), definitionCode, token: cancellationToken);

            // batch validation for agents.
            var agentsCodesFromDB = dbAgentsCopy
                .Select(t => t.Code)
                .Distinct()
                .ToList();
            string? batchFilter = String.Join(" OR ", agentsCodesFromDB.Select(t => $"Code='{t}'"));

            if (isBusinessPartnerAgent)
            {
                batchFilter = String.Join(" OR ", dbAgents.Select(bp => $"(Agent1Id={bp.Agent1Id} AND Agent2Id={bp.Agent2Id} AND Lookup1Id={bp.Lookup1Id})"));
                serial = await GetAgentMaxSerialNumber(tenantId, agentDefinitionId, cancellationToken);
            }

            batchFilter = batchFilter.Length < 1024 ? batchFilter : null;
            var agentsObjectResult = await GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(), agentDefinitionId, batchFilter, cancellationToken: cancellationToken);

            var agentsResult = agentsObjectResult.ConvertAll(agent => (Agent)agent);

            // Remove agents from dbAgentsCopy that are already existing in tellma with same properties.
            foreach (var dbAgent in dbAgentsCopy)
            {
                if (agentsResult.Any(tellmaAgent => (tellmaAgent.Code == dbAgent.Code
                    && (tellmaAgent.Name == dbAgent.Name || tellmaAgent.Name == $"{dbAgent.Name} - {dbAgent.Code}" || $"{tellmaAgent.Code}: {tellmaAgent.Name}" == $"{dbAgent.Code}: {dbAgent.Name}")
                    && (tellmaAgent.Name2 == dbAgent.Name2 || tellmaAgent.Name2 == $"{dbAgent.Name2} - {dbAgent.Code}" || $"{tellmaAgent.Code}: {tellmaAgent.Name}" == $"{dbAgent.Code}: {dbAgent.Name}")
                    && tellmaAgent.Agent1Id == dbAgent.Agent1Id
                    && tellmaAgent.Agent2Id == dbAgent.Agent2Id
                    && tellmaAgent.Lookup1Id == dbAgent.Lookup1Id
                    && tellmaAgent.Lookup2Id == dbAgent.Lookup2Id
                    && tellmaAgent.FromDate?.ToString("yyyy-MM-dd") == dbAgent.FromDate?.ToString("yyyy-MM-dd")
                    && tellmaAgent.ToDate?.ToString("yyyy-MM-dd") == dbAgent.ToDate?.ToString("yyyy-MM-dd")
                    && ((tellmaAgent.Description == dbAgent.Description) || (String.IsNullOrWhiteSpace(tellmaAgent.Description) && String.IsNullOrWhiteSpace(dbAgent.Description)))
                    && tellmaAgent.Description2 == dbAgent.Description2)
                    || (isBusinessPartnerAgent && tellmaAgent.Agent1Id == dbAgent.Agent1Id //business partner check
                    && tellmaAgent.Agent2Id == dbAgent.Agent2Id
                    && tellmaAgent.Lookup1Id == dbAgent.Lookup1Id)))
                {
                    if (isBusinessPartnerAgent)
                    {
                        var x = agentsResult.First(agentsResult => agentsResult.Agent1Id == dbAgent.Agent1Id
                            && agentsResult.Agent2Id == dbAgent.Agent2Id
                            && agentsResult.Lookup1Id == dbAgent.Lookup1Id);

                        dbAgentsCopy = dbAgentsCopy.Where(a => !(a.Agent1Id == dbAgent.Agent1Id
                                                            && a.Lookup1Id == dbAgent.Lookup1Id)).ToList();
                    }
                    else
                    {
                        dbAgentsCopy = dbAgentsCopy.Where(a => a.Code != dbAgent.Code).ToList();
                    }
                }
                else
                {
                    // Condition for updating business partners. This extra check is needed because business partners don't have unique codes from SICS.
                    if (isBusinessPartnerAgent && agentsResult.Any(tellmaAgent => tellmaAgent.Agent1Id == dbAgent.Agent1Id
                        && tellmaAgent.Lookup1Id == dbAgent.Lookup1Id))
                    {
                        var tellmaAgent = agentsResult.First(tellmaAgent => tellmaAgent.Agent1Id == dbAgent.Agent1Id && tellmaAgent.Lookup1Id == dbAgent.Lookup1Id);
                        dbAgent.Id = tellmaAgent.Id;
                        dbAgent.Code = tellmaAgent.Code;
                    }
                }
            }

            if (dbAgentsCopy.Count == 0)
            {
                _logger.LogInformation($"{definitionCode}/{agentDefinitionId} has no new agents!");
                return agentsResult;

            }

            //Update tellma agents with updated agents from DB or create new agents from DB.
            var agentsToCreate = new List<AgentForSave>();
            var agentsToUpdate = new List<AgentForSave>();

            // CONDITION FOR INSURANCE AGENTS: Check for duplicate names
            if (definitionCode == "InsuranceAgent")
            {
                // Group agents by name to find duplicates
                var nameGroups = dbAgentsCopy.GroupBy(a => a.Name.ToLower())
                                            .Where(g => g.Count() > 1)
                                            .ToList();

                foreach (var group in nameGroups)
                {
                    foreach (var dbAgent in group)
                    {
                        // For duplicate names, concatenate code with name
                        dbAgent.Name = $"{dbAgent.Name} - {dbAgent.Code}";
                        dbAgent.Name2 = $"{dbAgent.Name2} - {dbAgent.Code}";
                    }
                }
            }

            foreach (var dbAgent in dbAgentsCopy)
            {
                string code = dbAgent.Code;
                string agentName = String.Empty;
                int? agent1Id = null;
                int? agent2Id = null;
                int? lookup1Id = null;
                int? lookup2Id = null;
                int? lookup3Id = null;
                DateTime? fromDate = null;
                DateTime? toDate = null;
                string? description = null;
                string? description2 = null;

                var tellmaAgent = agentsResult.FirstOrDefault(a => a.Code == code);

                switch (definitionCode)
                {
                    case "InsuranceAgent":
                        // For InsuranceAgent, use the potentially modified name (with concatenated code)
                        agentName = dbAgent.Name;
                        break;

                    case "InsuranceContract":
                        agentName = (dbAgent.Name == tellmaAgent?.Name) ? tellmaAgent.Name : dbAgent.Name;
                        lookup1Id = dbAgent.Lookup1Id; // Business Type
                        lookup3Id = dbAgent.Lookup3Id; // Risk Country
                        agent2Id = dbAgent.Agent2Id;   // Broker
                        description = dbAgent.Description; // Description
                        description2 = dbAgent.Description2; // Final closing date
                        fromDate = dbAgent.FromDate;

                        if (tellmaAgent != null)
                            fromDate = (dbAgent.FromDate <= tellmaAgent.FromDate ? dbAgent.FromDate : tellmaAgent.FromDate) ?? dbAgent.FromDate;

                        toDate = dbAgent.ToDate;
                        break;

                    case "TradeReceivableAccount":
                        agentName = (dbAgent.Name == tellmaAgent?.Name) ? tellmaAgent.Name : dbAgent.Name;
                        agent1Id = dbAgent.Agent1Id; // Insurance Agent
                        agent2Id = dbAgent.Agent2Id; // Contract
                        lookup2Id = dbAgent.Lookup2Id; // Main Business Class
                        break;

                    case "BusinessPartner":
                        code = dbAgent.Code != "-" ? dbAgent.Code : "BP" + (++serial).ToString("00000");
                        agentName = $"{code}: {dbAgent.Name}";

                        agent1Id = dbAgent.Agent1Id; // Contract
                        agent2Id = dbAgent.Agent2Id; // Partner Agent
                        lookup1Id = dbAgent.Lookup1Id; // Partnership Type
                        break;

                    default:
                        throw new InvalidOperationException($"Unknown agent definition code: {definitionCode}");
                }

                if (tellmaAgent == null)
                {
                    // Create new agent
                    agentsToCreate.Add(new AgentForSave
                    {
                        Code = code,
                        Name = agentName,
                        Name2 = agentName,
                        Agent1Id = agent1Id,
                        Agent2Id = agent2Id,
                        Lookup1Id = lookup1Id,
                        Lookup2Id = lookup2Id,
                        Lookup3Id = lookup3Id,
                        FromDate = fromDate,
                        ToDate = toDate,
                        Description = description,
                        Description2 = description2
                    });
                }
                else
                {
                    agentsToUpdate.Add(new AgentForSave
                    {
                        Id = tellmaAgent.Id,
                        Code = code,
                        Name = agentName,
                        Name2 = agentName,
                        Agent1Id = agent1Id,
                        Agent2Id = agent2Id,
                        Lookup1Id = lookup1Id,
                        Lookup2Id = lookup2Id,
                        Lookup3Id = lookup3Id,
                        FromDate = fromDate,
                        ToDate = toDate,
                        Description = description,
                        Description2 = description2
                    });

                }
            }

            if (agentsToCreate.Count == 0 && agentsToUpdate.Count == 0)
            {
                _logger.LogInformation($"{definitionCode} Agent sync completed! No changes detected.");
                return agentsResult;
            }

            if (agentsToUpdate.Count != 0)
            {
                _logger.LogInformation($"Updating {agentsToUpdate.Count} existing {definitionCode} agents...");
            }

            if (agentsToCreate.Count != 0)
            {
                _logger.LogInformation($"Creating {agentsToCreate.Count} new {definitionCode} agents...");
            }

            agentsToCreate.AddRange(agentsToUpdate);

            var createdAgents = await SaveAgents(tenantId, agentDefinitionId, agentsToCreate, cancellationToken);
            _logger.LogInformation($"{definitionCode} Agent sync completed!");
            agentsResult.AddRange(createdAgents);

            return agentsResult;
        }

        public async Task<List<Agent>> SaveAgents(int tenantId, int agentDefinitionId, List<AgentForSave> agentForSave, CancellationToken token)
        {
            var tellmaClient = _client.Application(tenantId);
            var createdAgents = new List<Agent>();

            try
            {
                await tellmaClient
                    .Agents(agentDefinitionId)
                    .Save(agentForSave);

                _logger.LogInformation($"Agents/{agentDefinitionId} updated!");

                string? agentsFilter = string.Join(" or ", agentForSave.Select(a => $"Code = '{a.Code}'"));
                agentsFilter = agentsFilter?.Length < 1024 ? agentsFilter : null;

                //Business partners (AgentId: 103) is not needed to be fetched back
                if (agentDefinitionId == 103)
                    return createdAgents;

                if (agentsFilter != null)
                {
                    var createdAgentsResult = await tellmaClient
                        .Agents(agentDefinitionId)
                        .GetEntities(new Request<GetArguments>
                        {
                            Arguments = new GetArguments
                            {
                                Top = agentForSave.Count,
                                OrderBy = "Id desc",
                                Filter = agentsFilter
                            }
                        }, token);
                 
                    return createdAgentsResult.Data.ToList();
                }
                else
                {
                    int pageSize = 500;
                    int skip = 0;

                    while (true) 
                    {
                        var createdAgentResult = await tellmaClient
                            .Agents(agentDefinitionId)
                            .GetEntities(new Request<GetArguments>
                            {
                                Arguments = new GetArguments
                                {
                                    Top = pageSize,
                                    Skip = skip
                                }
                            }, token);

                        createdAgents.AddRange(createdAgentResult.Data);

                        if (createdAgentResult.Data.Count < pageSize) break;

                        skip += pageSize;
                    }
                    return createdAgents;
                }
            }
            catch (Exception ex)
            {
                LogTellmaError(ex);
                return createdAgents;
            }
        }

        public async Task DeleteDocumentsByDefinitionId(int tenantId, int documentDefinitionId, CancellationToken token)
        {
            int[] worksheetSerials = [3837, 3457, 1396, 4245, 2752, 3455, 1437, 2699, 1429, 2913, 3825, 3456, 1310, 1424, 1432, 1420, 5104, 5119, 5249, 5356, 5621, 5619, 5649, 5791, 5797, 6089, 6103, 6141, 6201, 6221, 6222, 6249, 1369];
            int minSerial = worksheetSerials.Min();
            int maxSerial = worksheetSerials.Max();

            string serialFilter = $"SerialNumber >= {minSerial} AND SerialNumber <= {maxSerial}";

            var tellmaClient = _client.Application(tenantId);
            var documentResult = await tellmaClient
                .Documents(documentDefinitionId)
                .GetFact(new FactArguments
                {
                    Select = "Id, State",
                    Filter = $"CreatedBy.Id = 77 AND {serialFilter}",
                    Top = 1000
                }, token);

            var documentsState = documentResult
                .Data
                .Select(d => new
                {
                    Id = Convert.ToInt32(d[0]),
                    State = Convert.ToInt32(d[1])
                }).ToList();

            var allDocumentIds = documentsState
                .Select(d => d.Id)
                .ToList();

            var closedDocumentIds = documentsState
                .Where(d => d.State == 1) // Closed state
                .Select(d => d.Id)
                .ToList();

            await tellmaClient
                .Documents(documentDefinitionId)
                .Open(closedDocumentIds);

            try
            {
                int chunkSize = 200;
                var documentIdsChunk = allDocumentIds.Chunk(chunkSize);

                foreach (var chunk in documentIdsChunk)
                {
                    await tellmaClient
                        .Documents(documentDefinitionId)
                        .DeleteByIds(chunk.ToList());

                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Bulk delete failed for tenant: {tenantId} Documents/{documentDefinitionId}, trying individual deletes.\n\n Error: {ex.Message}");

                foreach (int id in allDocumentIds)
                    await tellmaClient
                        .Documents(documentDefinitionId)
                        .DeleteById(id);
            }

            _logger.LogInformation($"For tenant: {tenantId} All Documents/{documentDefinitionId} are deleted!");
        }

        public async Task<DocumentForSave> GetDocumentById(int tenantId, int documentDefinitionId, int documentId, CancellationToken cancellationToken)
        {
            var tellmaClient = _client.Application(tenantId);
            return await tellmaClient
                .Documents(documentDefinitionId)
                .GetByIdForSave(documentId);
        }

        public async Task<List<Document>> SaveDocuments(int tenantId, int documentDefinitionId, List<DocumentForSave> documents, CancellationToken cancellationToken)
        {
            var newDocuments = new List<Document>();
            var tellmaClient = _client.Application(tenantId);
            try
            {
                var x = await tellmaClient
                .Documents(documentDefinitionId)
                .Save(documents);

                _logger.LogInformation("Documents created!");

                var minSerial = documents.Min(d => d.SerialNumber) ?? 0;
                var maxSerial = documents.Max(d => d.SerialNumber) ?? 0;
                string documentsFilter = $"State = 0 AND SerialNumber >= {minSerial} AND SerialNumber <= {maxSerial}";
                documentsFilter = documentsFilter.Length < 1024 ? documentsFilter : null;

                var createdDocsResult = await tellmaClient
                    .Documents(documentDefinitionId)
                    .GetEntities(new Request<GetArguments>
                    {
                        Arguments = new GetArguments
                        {
                            Top = documents.Count,
                            OrderBy = "Id desc",
                            Filter = documentsFilter
                        }
                    }, cancellationToken);
                newDocuments.AddRange(createdDocsResult.Data);
                return newDocuments;
            }
            catch (Exception ex)
            {
                LogTellmaError(ex);
                return newDocuments;
            }
        }

        public async Task CloseDocuments(int tenantId, int documentDefinitionId, List<int> documentIds, CancellationToken cancellationToken)
        {
            var applicationClient = _client.Application(tenantId);
            try
            {
                if (documentIds.Count < 200)
                {
                    await applicationClient
                        .Documents(documentDefinitionId)
                        .Close(documentIds);
                }
                else
                {
                    int chunkSize = 200;
                    var chunkedDocumentIds = documentIds.Chunk(chunkSize);
                    
                    foreach (var chunk in chunkedDocumentIds)
                    {
                        await applicationClient
                            .Documents(documentDefinitionId)
                            .Close(chunk.ToList());
                    }

                }
                    _logger.LogInformation("Documents closed!");
            }
            catch (Exception ex)
            {
                LogTellmaError(ex);
            }
        }

        public void LogTellmaError(Exception ex)
        {
            _logger.LogError(new Exception(ex.ToString()), "Tellma API Error");
            throw ex;
        }

        public async Task<int> GetAgentMaxSerialNumber(int tenantId, int agentDefinitionId, CancellationToken token)
        {
            var tellmaClient = _client.Application(tenantId);
            var result = await tellmaClient
                .Agents(agentDefinitionId)
                .GetEntities(new Request<GetArguments>
                {
                    Arguments = new GetArguments
                    {
                        Top = 1,
                        OrderBy = "Code desc"
                    }
                }, token);

            if (result.Data.Count > 0)
                return Convert.ToInt32(result.Data.First().Code.Substring(2));

            return 0;
        }

        public async Task DeleteAgentsByDefinition(int tenantId, int agentDefinitionId, CancellationToken token)
        {
            var tellmaClient = _client.Application(tenantId);
            var agentsResult = await tellmaClient
                .Agents(agentDefinitionId)
                .GetFact(new FactArguments
                {
                    Select = "Id",
                    OrderBy = "Id Desc",
                    Filter = "CreatedBy.Id = 77",
                    Top = 5000
                }, token);

            var agentIds = agentsResult
                .Data
                .Select(d => Convert.ToInt32(d[0]))
                .ToList();

            try
            {
                int chunkSize = 200;
                var agentIdsChunk = agentIds.Chunk(chunkSize);

                int counter = 1;
                foreach (var chunk in agentIdsChunk)
                {
                    _logger.LogInformation("Deleting {batch} out of {count} batches for Agent/{definition}", counter++, agentIdsChunk.Count(), agentDefinitionId);
                    await tellmaClient
                        .Agents(agentDefinitionId)
                        .DeleteByIds(chunk.ToList());

                }

                _logger.LogInformation($"\n\nFor tenant: {tenantId} All Agents/{agentDefinitionId} are deleted!\n\n");

            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Bulk delete failed for tenant: {tenantId} Agents/{agentDefinitionId} \n\n Error: {ex.ToString()}");
            }

        }

        public async Task<SettingsForClient> GetTenantProfile(int tenantId, CancellationToken token)
        {
            var tellmaClient = _client.Application(tenantId);

            var tenantSettings = await tellmaClient
                .GeneralSettings
                .SettingsForClient();

            return tenantSettings.Data;

        }
    }
}