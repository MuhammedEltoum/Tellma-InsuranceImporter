using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Data;
using System.Diagnostics;
using Tellma.Api.Dto;
using Tellma.Client;
using Tellma.Model.Application;

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
            var filterType = clientProperty == nameof(TellmaClient.ApplicationClientBehavior.EntryTypes) ? "Concept = " : "Code = ";
            if(isBankAccount.HasValue)
                filterType = isBankAccount.Value ? "Text3 = " : filterType;
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
            if(definitionId.HasValue)
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
                    createdAgents.AddRange(createdAgentsResult.Data);
                    return createdAgents;
                }
                else
                {
                    foreach (var agent in agentForSave)
                    {
                        var createdAgentResult = await tellmaClient
                            .Agents(agentDefinitionId)
                            .GetEntities(new Request<GetArguments>
                            {
                                Arguments = new GetArguments
                                {
                                    Top = 1,
                                    OrderBy = "Id desc",
                                    Filter = $"Code = '{agent.Code}'"
                                }
                            }, token);
                        createdAgents.Add(createdAgentResult.Data[0]);
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
            var tellmaClient = _client.Application(tenantId);
            var documentResult = await tellmaClient
                .Documents(documentDefinitionId)
                .GetFact(new FactArguments
                {
                    Select = "Id, State",
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
                await applicationClient
                    .Documents(documentDefinitionId)
                    .Close(documentIds);
                _logger.LogInformation("Documents closed!");
            }
            catch (Exception ex)
            {
                LogTellmaError(ex);
                throw new Exception($"Error closing documents in Tellma for tenant {tenantId} and document definition {documentDefinitionId}.");
            }
        }

        public void LogTellmaError(Exception ex)
        {
            _logger.LogError(ex.ToString());
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

                foreach (var chunk in agentIdsChunk)
                {
                    await tellmaClient
                        .Agents(agentDefinitionId)
                        .DeleteByIds(chunk.ToList());
                }

                _logger.LogInformation($"For tenant: {tenantId} All Agents/{agentDefinitionId} are deleted!");

            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Bulk delete failed for tenant: {tenantId} Agents/{agentDefinitionId}, trying individual deletes.\n\n Error: {ex.Message}");
            }

        }
    }
}