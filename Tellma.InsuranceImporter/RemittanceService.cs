using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Repository;
using Tellma.Model.Application;
using Tellma.InsuranceImporter.Enums;

namespace Tellma.InsuranceImporter
{
    public class RemittanceService : IImportService<Remittance>
    {
        private readonly ITellmaService _service;
        private readonly IWorksheetRepository<Remittance> _repository;
        private readonly ILogger<TellmaService> _logger;
        public RemittanceService(IWorksheetRepository<Remittance> repository, IOptions<TellmaOptions> options, ILogger<TellmaService> logger)
        {
            _logger = logger;
            _service = new TellmaService(logger, options);
            _repository = repository;
        }
        
        public async Task Import(CancellationToken cancellationToken)
        {
            //Start validation
            var validRemittanceList = await _repository.GetWorksheets(cancellationToken);
            string worksheetIds = String.Empty;

            //Direction validation
            var invalidDirections = validRemittanceList
                .Where(r => Math.Abs(r.Direction) != 1)
                .Select(r => r.WorksheetId)
                .Distinct();
            if (invalidDirections.Any())
            {
                validRemittanceList = validRemittanceList
                    .Where(r => !invalidDirections.Contains(r.WorksheetId))
                    .ToList();

                int removedInvalidDirection = invalidDirections.Count();
                worksheetIds = string.Join(", ", invalidDirections);
                _logger.LogError($"Validation Error: ({removedInvalidDirection}) WorksheetId [{worksheetIds}] have an invalid direction.");
            }

            //RemittanceType validation
            var invalidRemmitancetype = validRemittanceList
                .Where(r => String.IsNullOrWhiteSpace(r.RemittanceType)) //No need for bank fee check.
                .Select(r => r.WorksheetId)
                .Distinct();
            if (invalidRemmitancetype.Any())
            {
                validRemittanceList = validRemittanceList
                    .Where(r => !invalidRemmitancetype.Contains(r.WorksheetId))
                    .ToList();

                int removedRemiTypes = invalidRemmitancetype.Count();
                worksheetIds = string.Join(", ", invalidRemmitancetype);
                _logger.LogError($"Validation Error: ({removedRemiTypes}) WorksheetId [{worksheetIds}] has no remittance type.");
            }

            var tenants = validRemittanceList
                .Select(r => r.TenantCode)
                .Distinct()
                .ToList();
            foreach (var tenantCode in tenants)
            {
                var remittanceDocuments = new List<DocumentForSave>();
                int tenantId = 0;
                switch (tenantCode)
                {
                    case "IR1":
                        tenantId += 601;
                        break;
                    case "IR160":
                        tenantId += 602;
                        break;
                    default:
                        tenantId += 1303;
                        break;
                }
                tenantId = 1303;
                validRemittanceList = validRemittanceList
                        .Where(r => r.TenantCode == tenantCode)
                        .ToList();

                //Accounts Batch
                var aAccountsCodes = validRemittanceList
                        .Where(v => v.TenantCode == tenantCode)
                        .Select(a => a.AAccount)
                        .Distinct()
                        .ToList();
                var bAccountsCodes = validRemittanceList
                        .Where(v => v.TenantCode == tenantCode)
                        .Select(b => b.BAccount)
                        .Distinct()
                        .ToList();
                aAccountsCodes.AddRange(bAccountsCodes);
                var accountsCode = aAccountsCodes.Distinct();
                string? accountsFilter = String.Join(" OR ", accountsCode.Select(a => $"Code = '{a}'"));
                var accountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Accounts.AsString(), filter: accountsFilter, token: cancellationToken);
                var accountsResult = accountsObjectResult.ConvertAll(a => (Account)a);

                //BankAccounts validation
                int bankAccountDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.BankAccount.AsString(), token: cancellationToken);
                string? baBatchFilter = String.Join(" OR ", validRemittanceList.Where(ba => !String.IsNullOrWhiteSpace(ba.BankAccountCode)).Select(r => $"Text3  = '{r.BankAccountCode}'").Distinct());
                baBatchFilter = baBatchFilter.Length < 1024 ? baBatchFilter : null;
                var bankAccountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(), bankAccountDefinitionId, baBatchFilter, token: cancellationToken);
                var bankAccountsResult = bankAccountsObjectResult.ConvertAll(agent => (Agent)agent);
                var tellmaBankAccountsCodes = bankAccountsResult.Select(ba => ba.Text3);
                var missingBARemittances = validRemittanceList
                    .Where(r => !tellmaBankAccountsCodes.Contains(r.BankAccountCode))
                    .Select(r => r.WorksheetId)
                    .Distinct();

                if (missingBARemittances.Any())
                {
                    validRemittanceList = validRemittanceList
                        .Where(r => !missingBARemittances.Contains(r.WorksheetId))
                        .ToList();

                    int removedBACount = missingBARemittances.Count();
                    worksheetIds = string.Join(", ", missingBARemittances);
                    _logger.LogError($"Validation Error: ({removedBACount}) WorksheetIds [{worksheetIds}] bank accounts are not defined in tellma.");
                }

                //BankAccounts-Currencies validation
                var tellmaBankAccCurrencies = bankAccountsResult
                    .Select(ba => ba.Text3 + " - " + ba.CurrencyId)
                    .Distinct();

                var missingBACurrencyRemittances = validRemittanceList
                    .Where(r => !tellmaBankAccCurrencies.Contains(r.BankAccountCode + " - " + r.BankAccountCurrencyId))
                    .Select(r => r.WorksheetId)
                    .Distinct();

                if (missingBACurrencyRemittances.Any())
                {
                    validRemittanceList = validRemittanceList
                        .Where(r => !missingBACurrencyRemittances.Contains(r.WorksheetId))
                        .ToList();

                    int removedBACurrenciesCount = missingBACurrencyRemittances.Count();
                    worksheetIds = string.Join(", ", missingBACurrencyRemittances);
                    _logger.LogError($"Validation Error: ({removedBACurrenciesCount}) WorksheetIds [{worksheetIds}] bank accounts and their currencies do not match tellma.");
                }

                //Will use batch validation for insurance agents.
                int insuranceAgentDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.InsuranceAgent.AsString(), token: cancellationToken);
                var insuranceAgentsFromDB = validRemittanceList
                    .Where(r => !String.IsNullOrWhiteSpace(r.AgentCode) && !String.IsNullOrWhiteSpace(r.AgentName))
                    .Select(r => new { r.AgentCode, r.AgentName })
                    .Distinct()
                    .ToList();
                var insuranceAgentsCodesFromDB = insuranceAgentsFromDB
                    .Select(r => r.AgentCode)
                    .ToList();
                string? iaBatchFilter = String.Join(" OR ", insuranceAgentsCodesFromDB.Select(r => $"Code = '{r}'"));
                iaBatchFilter = iaBatchFilter.Length < 1024 ? iaBatchFilter : null;
                var insuranceAgentsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(), insuranceAgentDefinitionId, iaBatchFilter, token: cancellationToken);

                //Update tellma insurance agents with updated insurance agents from remittances
                var insuranceAgentsResult = insuranceAgentsObjectResult.ConvertAll(agent => (Agent)agent);

                var agentsToCreate = new List<AgentForSave>();
                var agentsToUpdate = new List<AgentForSave>();

                foreach (var dbAgent in insuranceAgentsFromDB)
                {
                    var existingAgent = insuranceAgentsResult.FirstOrDefault(a => a.Code == dbAgent.AgentCode);

                    if (existingAgent == null)
                    {
                        // Create new agent
                        agentsToCreate.Add(new AgentForSave
                        {
                            Code = dbAgent.AgentCode,
                            Name = $"{dbAgent.AgentName} - {dbAgent.AgentCode}",
                            Name2 = $"{dbAgent.AgentName} - {dbAgent.AgentCode}"
                        });
                    }
                    else
                    {
                        // Check if agent needs update (name changed)
                        var expectedName = (dbAgent.AgentName == existingAgent.Name) ? existingAgent.Name : $"{dbAgent.AgentName} - {dbAgent.AgentCode}";
                        if (existingAgent.Name != expectedName || existingAgent.Name2 != expectedName)
                        {
                            agentsToUpdate.Add(new AgentForSave
                            {
                                Id = existingAgent.Id,
                                Code = dbAgent.AgentCode,
                                Name = expectedName,
                                Name2 = expectedName
                            });
                        }
                    }
                }

                agentsToCreate.AddRange(agentsToUpdate);
                var createdAgents = await _service.SaveAgents(tenantId, insuranceAgentDefinitionId, agentsToCreate, cancellationToken);
                _logger.LogInformation("Insurance agent sync completed!");

            //End of validation
            if (validRemittanceList.Count == 0)
                {
                    _logger.LogWarning($"No valid remittances records!");
                    return;
                }
                //start import
                int documentDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.DocumentDefinitions.AsString(), TellmaEntityCode.RemittanceWorksheet.AsString(), token: cancellationToken);
                int lineDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LineDefinitions.AsString(), TellmaEntityCode.ManualLine.AsString(), token: cancellationToken);

                string operationCenterCode = TellmaEntityCode.OperationCenter.AsString();
                int operationCenterId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Centers.AsString(), operationCenterCode, token: cancellationToken);

                int inwardOutwardDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.TechnicalInOutward.AsString(), token: cancellationToken);
                int inwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Inward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);
                int outwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Outward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);

                foreach (var remittance in validRemittanceList)
                {
                    if (validRemittanceList.Count() == 0)
                    {
                        _logger.LogWarning($"No valid remittances records for tenant {tenantCode}!");
                        return;
                    }
                    int insuranceAgentId = createdAgents.FirstOrDefault(ia => ia.Code == remittance.AgentCode)?.Id ?? 0;
                    insuranceAgentId = insuranceAgentId == 0 ? insuranceAgentsResult.FirstOrDefault(ia => ia.Code == remittance.AgentCode).Id : insuranceAgentId;

                    int bankAccountId = bankAccountsResult.FirstOrDefault(ba => ba.Text3 == remittance.BankAccountCode).Id;
                    bankAccountId = bankAccountId == 0 ? await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Agents.AsString(), remittance.BankAccountCode, bankAccountDefinitionId, true, token: cancellationToken) : bankAccountId;

                    int inwardOutwardLookupId = remittance.RemittanceType.ToLower() == "wire2" ? outwardLookupId : inwardLookupId;

                    int serialNumber = Convert.ToInt32(remittance.WorksheetId?.Substring(2));

                    string memo = remittance.RemittanceTypeName + ", " +
                        remittance.RemittanceType + ", " +
                        "DIR = " + remittance.Direction + ", " +
                        "PK = " + remittance.PK + ", " +
                        remittance.RemittanceNotes;
                    //for memo tellma validation 255 char
                    if (memo.Length > 255)
                    {
                        _logger.LogWarning($"Memo for remittance RW{serialNumber} exceeds 255 characters and will be truncated.");
                        memo = memo.Substring(0, 255);
                    }

                    string? insuranceAgentName = remittance.AgentName ?? null;
                    if (insuranceAgentName != null && insuranceAgentName.Length > 50)
                    {
                        _logger.LogWarning($"Insurance agent name for remittance RW{serialNumber} exceeds 50 characters and will be truncated.");
                        insuranceAgentName = insuranceAgentName.Substring(0, 50);
                    }

                    int accountAId = accountsResult.FirstOrDefault(a => a.Code == remittance.AAccount).Id;
                    int accountBId = accountsResult.FirstOrDefault(a => a.Code == remittance.BAccount).Id;
                    int entryTypeAId = remittance.APurposeId;
                    int entryTypeBId = remittance.BPurposeId;

                    int agentAId = remittance.AIsBankAcc ? bankAccountId : insuranceAgentId;
                    int agentBId = remittance.BIsBankAcc ? bankAccountId : insuranceAgentId;

                    var entriesList = new List<EntryForSave> {
                            new EntryForSave
                            {
                                //A Account
                                AccountId = accountAId,
                                EntryTypeId = entryTypeAId,
                                Direction = remittance.ADirection,
                                Value = remittance.ValueFC2,
                                MonetaryValue = remittance.TransferAmount,
                                AgentId = agentAId,
                                NotedAgentId = remittance.ANotedAgentId ?? null,
                                ResourceId = remittance.AResourceId ?? null,
                                NotedResourceId = remittance.ANotedResourceId ?? null,
                                NotedDate = remittance.AHasNOTEDDATE ? remittance.PostingDate : null,
                                Quantity = remittance.AQuantity,
                                CurrencyId = remittance.AIsBankAcc ? remittance.BankAccountCurrencyId : remittance.TransferCurrencyId,
                                ExternalReference = remittance.AIsBankAcc ? remittance.Reference : null,
                                CenterId = operationCenterId,
                                NotedAgentName = remittance.AIsBankAcc ? insuranceAgentName : null
                            },
                            new EntryForSave
                            {
                                //B Account
                                AccountId = accountBId,
                                EntryTypeId = entryTypeBId,
                                Direction = remittance.BDirection,
                                Value = remittance.ValueFC2,
                                MonetaryValue = remittance.TransferAmount,
                                CurrencyId = remittance.BIsBankAcc ? remittance.BankAccountCurrencyId : remittance.TransferCurrencyId,
                                AgentId = agentBId,
                                NotedAgentId = remittance.BNotedAgentId ?? null,
                                ResourceId = remittance.BResourceId ?? null,
                                NotedResourceId = remittance.BNotedResourceId ?? null,
                                NotedDate = remittance.BHasNOTEDDATE ? remittance.PostingDate : null,
                                Quantity = remittance.BQuantity,
                                ExternalReference = remittance.BIsBankAcc ? remittance.Reference : null,
                                CenterId = operationCenterId,
                                NotedAgentName = remittance.BIsBankAcc ? insuranceAgentName : null
                            }
                        };

                    DocumentForSave document;
                    if (remittance.DocumentId > 0)
                    {
                        document = await _service.GetDocumentById(tenantId, documentDefinitionId, remittance.DocumentId, cancellationToken);
                        document.SerialNumber = serialNumber;
                        document.PostingDate = remittance.PostingDate;
                        document.PostingDateIsCommon = true;
                        document.Lookup1Id = inwardOutwardLookupId;
                        document.Memo = memo;
                        document.MemoIsCommon = true;
                        document.Lines = new List<LineForSave>();
                    }
                    else
                    {
                        document = new DocumentForSave
                        {
                            SerialNumber = serialNumber,
                            PostingDate = remittance.PostingDate,
                            PostingDateIsCommon = true,
                            Lookup1Id = inwardOutwardLookupId,
                            Memo = memo,
                            MemoIsCommon = true,
                            CenterIsCommon = true,
                            Lines = new List<LineForSave>()
                        };
                    }
                    document.Lines.Add(
                        new LineForSave
                        {
                            DefinitionId = lineDefinitionId,
                            Entries = entriesList
                        }
                    );

                    remittanceDocuments.Add(document);
                }
                try
                {
                    var remittanceDocumentsResult = await _service.SaveDocuments(tenantId, documentDefinitionId, remittanceDocuments, cancellationToken);
                    var remittanceRecords = remittanceDocumentsResult
                        .Select(r => new Remittance
                                {
                                    WorksheetId = $"RW{r.SerialNumber}",
                                    DocumentId = r.Id
                                });
                    
                    await _repository.UpdateDocumentIds(tenantCode, remittanceRecords, cancellationToken);
                    var documentIds = remittanceRecords.Select(r => r.DocumentId).ToList();
                    await _service.CloseDocuments(tenantId, documentDefinitionId, documentIds, cancellationToken);
                    await _repository.UpdateImportedWorksheets(tenantCode, remittanceRecords, cancellationToken);
                    _logger.LogInformation($"Remittance import finished!");
                }
                catch (Exception ex)
                {
                    //continue insurance upload.
                    _logger.LogError($"An error occured while importing remittances. \r {ex.ToString()}");
                }
            }
        }

    }
}