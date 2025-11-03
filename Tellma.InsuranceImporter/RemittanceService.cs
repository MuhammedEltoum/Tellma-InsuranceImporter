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
        private readonly ILogger<RemittanceService> _logger;
        public RemittanceService(ITellmaService service, IWorksheetRepository<Remittance> repository, IOptions<TellmaOptions> options, ILogger<RemittanceService> logger)
        {
            _service = service ?? throw new ArgumentNullException(nameof(service));
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        
        public async Task Import(CancellationToken cancellationToken)
        {
            var allWorksheets = (await _repository.GetWorksheets(cancellationToken)).ToList();

            var validWorksheets = ValidateWorksheets(allWorksheets);

            if (!validWorksheets.Any())
            {
                _logger.LogWarning("No valid worksheets found to import.");
                return;
            }


            var tenantCodes = validWorksheets.Select(t => t.TenantCode).Distinct().ToList();

            foreach (var tenantCode in tenantCodes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                //only process IR1 tenant for now
                if (tenantCode != "IR160") continue;

                var tenantWorks = validWorksheets.Where(t => t.TenantCode == tenantCode).ToList();

                try
                {
                    await ProcessTenant(tenantCode, tenantWorks, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred while processing tenant {TenantCode}", tenantCode);
                }
            }
        }

        private async Task ProcessTenant(string tenantCode, List<Remittance> validRemittanceList, CancellationToken cancellationToken)
        {
            if (!validRemittanceList.Any())
            {
                _logger.LogWarning("No worksheets for tenant {TenantCode}", tenantCode);
                return;
            }

            var tenantId = InsuranceHelper.GetTenantId(tenantCode);

            var tenantProfile = await _service.GetTenantProfile(tenantId, cancellationToken);

            // Log tenant info
            _logger.LogInformation("\n \n Processing tenant {TenantCode} (ID: {TenantId}, Name: {TenantName}) with {Count} remittance worksheets. \n \n",
                tenantCode, tenantId, tenantProfile.CompanyName, validRemittanceList.Count);

            // validate creation or updating of worksheets
            RemoveIf(ref validRemittanceList, r => r.DocumentId > 0 && r.PostingDate < tenantProfile.ArchiveDate, "have a posting date before or on the archive date for existing remittances");
            RemoveIf(ref validRemittanceList, r => r.PostingDate < tenantProfile.FreezeDate, "have a posting date before or on the freeze date for new remittances");

            // Collections to hold documents before saving
            var remittanceDocuments = new List<DocumentForSave>();

            // Agents/partners/accounts to be used in building documents
            var insuranceAgents = new List<Agent>();

            // Insurance Agents (primary)
            int insuranceAgentDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.InsuranceAgent.AsString(), token: cancellationToken);

            var primaryAgents = validRemittanceList
                .Select(a => new { Code = a.AgentCode, Name = a.AgentName })
                .Distinct()
                .Select(a => new Agent { Code = a.Code, Name = a.Name, Name2 = a.Name })
                .ToList();

            if (primaryAgents.Any())
            {
                var synced = await _service.SyncAgents(tenantId, TellmaEntityCode.InsuranceAgent.AsString(), primaryAgents, cancellationToken);
                insuranceAgents.AddRange(synced);
            }

            //BankAccounts validation
            // Can't sync due to missing bankId
            foreach (var remittance in validRemittanceList.Where(r => (r.AIsBankAcc || r.BIsBankAcc) && String.IsNullOrWhiteSpace(r.BankAccountCode)))
                remittance.BankAccountCode = "For control Purpose";

            var missingBankAcc = validRemittanceList
                .Where(r => String.IsNullOrWhiteSpace(r.BankAccountCode) && (r.AIsBankAcc || r.BIsBankAcc))
                .ToList();

            int bankAccountDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.BankAccount.AsString(), token: cancellationToken);
            string? baBatchFilter = String.Join(" OR ", validRemittanceList.Where(ba => !String.IsNullOrWhiteSpace(ba.BankAccountCode)).Select(r => $"Text3='{r.BankAccountCode}'").Distinct());
            baBatchFilter = baBatchFilter.Length < 1024 ? baBatchFilter : null;
            var bankAccountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(), bankAccountDefinitionId, baBatchFilter, token: cancellationToken);
            var bankAccountsResult = bankAccountsObjectResult.ConvertAll(agent => (Agent)agent);
            var tellmaBankAccountsCodes = bankAccountsResult.Select(ba => ba.Text3);
            var missingBARemittances = validRemittanceList
                .Where(r => !tellmaBankAccountsCodes.Contains(r.BankAccountCode))
                .Select(r => r.WorksheetId)
                .Distinct()
                .ToList();

            if (missingBARemittances.Any())
            {
                validRemittanceList = validRemittanceList
                    .Where(r => !missingBARemittances.Contains(r.WorksheetId))
                    .ToList();

                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] bank accounts are not defined in tellma.", missingBARemittances.Count, string.Join(", ", missingBARemittances));
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

                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] bank accounts and their currencies do not match tellma.", missingBACurrencyRemittances.Count(), string.Join(", ", missingBACurrencyRemittances));
            }

            if (!validRemittanceList.Any())
            {
                _logger.LogWarning("No new Remittance records to sync for tenant {Tenant}", tenantCode);
                return;
            }

            // Accounts batch
            var accountCodes = validRemittanceList.SelectMany(w => new[] { w.AAccount, w.BAccount })
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct()
                .ToList();
            string? accountsFilter = accountCodes.Any() ? string.Join(" OR ", accountCodes.Select(a => $"Code='{a}'")) : null;
            var accountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Accounts.AsString(), filter: accountsFilter, token: cancellationToken);
            var accountsResult = accountsObjectResult.ConvertAll(a => (Account)a);

            // Entry-types batch
            var entryTypesCodes = validRemittanceList.SelectMany(w => new[] { w.APurposeConcept, w.BPurposeConcept })
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct()
                .ToList();
            string? entryTypesFilter = entryTypesCodes.Any() ? string.Join(" OR ", entryTypesCodes.Select(a => $"Concept='{a}'")) : null;
            var entryTypesObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.EntryTypes.AsString(), filter: entryTypesFilter, token: cancellationToken);
            var entryTypesResult = entryTypesObjectResult.ConvertAll(entryType => (EntryType)entryType);

            // Document definitions and other ids
            int remittanceDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.DocumentDefinitions.AsString(), TellmaEntityCode.RemittanceWorksheet.AsString(), token: cancellationToken);
            int lineDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LineDefinitions.AsString(), TellmaEntityCode.ManualLine.AsString(), token: cancellationToken);

            string operationCenterCode = String.Empty;

            switch (tenantId)
            {
                case 601:
                    operationCenterCode = "20";
                    break;
                case 602:
                    operationCenterCode = "20";
                    break;
                case 1303:
                    operationCenterCode = "30";
                    break;
                default:
                    operationCenterCode = "30";
                    break;
            }

            int operationCenterId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Centers.AsString(), operationCenterCode, token: cancellationToken);

            int inwardOutwardDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.TechnicalInOutward.AsString(), token: cancellationToken);
            int inwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Inward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);
            int outwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Outward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);

            // Build documents
            foreach (var remittance in validRemittanceList)
            {
                if (validRemittanceList.Count() == 0)
                {
                    _logger.LogWarning($"No valid remittances records for tenant {tenantCode}!");
                    return;
                }
                int insuranceAgentId = insuranceAgents.FirstOrDefault(ia => ia.Code == remittance.AgentCode)?.Id ?? 0;

                var bankAccountId = bankAccountsResult.FirstOrDefault(ba => ba.Text3 == remittance.BankAccountCode)?.Id;
                bankAccountId = bankAccountId == 0 ? await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Agents.AsString(), remittance.BankAccountCode, bankAccountDefinitionId, isBankAccount: true, token: cancellationToken) : bankAccountId;

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
                    _logger.LogWarning("Memo for remittance RW{serialNumber} exceeds 255 characters and will be truncated.", serialNumber);
                    memo = memo.Substring(0, 255);
                }

                string? insuranceAgentName = remittance.AgentName ?? null;
                if (insuranceAgentName != null && insuranceAgentName.Length > 50)
                {
                    _logger.LogWarning("Insurance agent name for remittance RW{serialNumber} exceeds 50 characters and will be truncated.", serialNumber);
                    insuranceAgentName = insuranceAgentName.Substring(0, 50);
                }

                var accountAId = accountsResult.FirstOrDefault(a => a.Code == remittance.AAccount)?.Id;
                var accountBId = accountsResult.FirstOrDefault(a => a.Code == remittance.BAccount)?.Id;
                
                var entryTypeAId = entryTypesResult.FirstOrDefault(et => et.Concept == remittance.APurposeConcept)?.Id;
                var entryTypeBId = entryTypesResult.FirstOrDefault(et => et.Concept == remittance.BPurposeConcept)?.Id;

                var agentAId = remittance.AIsBankAcc ? bankAccountId : insuranceAgentId;
                var agentBId = remittance.BIsBankAcc ? bankAccountId : insuranceAgentId;

                var entriesList = new List<EntryForSave> {
                new EntryForSave
                {
                    //A Account
                    AccountId = accountAId,
                    EntryTypeId = entryTypeAId,
                    Direction = (short?)(remittance.RemittanceType.ToLower() != "exdiff" ? remittance.ADirection : -1 * remittance.ADirection),
                    Value = remittance.ValueFC2,
                    MonetaryValue = remittance.TransferAmount,
                    AgentId = agentAId,
                    NotedAgentId = remittance.ANotedAgentId,
                    ResourceId = remittance.AResourceId,
                    NotedResourceId = remittance.ANotedResourceId,
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
                    Direction = (short?)(remittance.RemittanceType.ToLower() != "exdiff" ? remittance.BDirection : -1 * remittance.BDirection),
                    Value = remittance.ValueFC2,
                    MonetaryValue = remittance.TransferAmount,
                    CurrencyId = remittance.BIsBankAcc ? remittance.BankAccountCurrencyId : remittance.TransferCurrencyId,
                    AgentId = agentBId,
                    NotedAgentId = remittance.BNotedAgentId,
                    ResourceId = remittance.BResourceId,
                    NotedResourceId = remittance.BNotedResourceId,
                    NotedDate = remittance.BHasNOTEDDATE ? remittance.PostingDate : null,
                    Quantity = remittance.BQuantity,
                    ExternalReference = remittance.BIsBankAcc ? remittance.Reference : null,
                    CenterId = operationCenterId,
                    NotedAgentName = remittance.BIsBankAcc ? insuranceAgentName : null
                }
            };

                if (entriesList[0].Direction < 0)
                    entriesList.Reverse();

                var document = new DocumentForSave
                {
                    Id = remittance.DocumentId,
                    SerialNumber = serialNumber,
                    PostingDate = remittance.PostingDate,
                    PostingDateIsCommon = true,
                    Lookup1Id = inwardOutwardLookupId,
                    Memo = memo,
                    MemoIsCommon = true,
                    CenterIsCommon = true,
                    Lines = new List<LineForSave>
                    {
                        new LineForSave
                        {
                            DefinitionId = lineDefinitionId,
                            Entries = entriesList
                        }
                    }
                };
                
                remittanceDocuments.Add(document);
            }
            try
            {
                var remittanceDocumentsResult = await _service.SaveDocuments(tenantId, remittanceDefinitionId, remittanceDocuments, cancellationToken);
                var remittanceRecords = remittanceDocumentsResult
                    .Select(r => new Remittance
                    {
                        WorksheetId = $"RW{r.SerialNumber}",
                        DocumentId = r.Id
                    });

                await _repository.UpdateDocumentIds(tenantCode, remittanceRecords, cancellationToken);
                var documentIds = remittanceRecords.Select(r => r.DocumentId).ToList();
                await _service.CloseDocuments(tenantId, remittanceDefinitionId, documentIds, cancellationToken);
                await _repository.UpdateImportedWorksheets(tenantCode, remittanceRecords, cancellationToken);
                _logger.LogInformation("Remittance import finished!");
            }
            catch (Exception ex)
            {
                //continue insurance upload.
                _logger.LogError($"An error occured while importing remittances. \r {ex.ToString()}");
            }
        }

        private List<Remittance> ValidateWorksheets(List<Remittance> worksheets)
        {
            var valid = worksheets.ToList();

            RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.AgentCode), "have an invalid insurance agent");
            RemoveIf(ref valid, w => Math.Abs(w.Direction) != 1, "have an invalid direction");

            // Keep only worksheets with supported prefixes
            var supportedPrefixes = new[] { "RW" };
            var invalidTypeIds = valid.Where(r => !supportedPrefixes.Any(p => r.WorksheetId.StartsWith(p))).Select(t => t.WorksheetId).Distinct().ToList();
            if (invalidTypeIds.Any())
            {
                valid = valid.Where(t => supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid technical type.", invalidTypeIds.Count, string.Join(", ", invalidTypeIds));
            }

            return valid;
        }

        private void RemoveIf(ref List<Remittance> list, Func<Remittance, bool> predicate, string errorMessage)
        {
            var invalid = list.Where(predicate).Select(t => t.WorksheetId).Distinct().ToList();
            if (!invalid.Any())
                return;

            list = list.Where(t => !invalid.Contains(t.WorksheetId)).ToList();
            _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] {Message}.", invalid.Count, string.Join(", ", invalid), errorMessage);
        }
    }
}