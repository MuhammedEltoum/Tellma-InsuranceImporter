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
        private readonly IOptionsMonitor<InsuranceOptions> _insuranceOptions;
        private readonly IOptions<TellmaOptions> _tellmaOptions;
        public RemittanceService(ITellmaService service, IWorksheetRepository<Remittance> repository, ILogger<RemittanceService> logger, IOptionsMonitor<InsuranceOptions> insuranceOptions, IOptions<TellmaOptions> tellmaOptions)
        {
            _service = service ?? throw new ArgumentNullException(nameof(service));
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _insuranceOptions = insuranceOptions ?? throw new ArgumentNullException(nameof(insuranceOptions));
            _tellmaOptions = tellmaOptions ?? throw new ArgumentNullException(nameof(tellmaOptions));
        }
        
        public async Task Import(string tenantCode, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string filter = $"[IMPORT_DATE] IS NULL AND [TENANT_CODE] = '{tenantCode}'";
            try
            {
                var allWorksheets = await _repository.GetWorksheets(filter, cancellationToken);
                await ProcessTenant(tenantCode, allWorksheets, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError("An error occurred while processing tenant {TenantCode}: {ErrorMessage}", tenantCode, ex.Message);
                throw;
            }
        }

        private async Task ProcessTenant(string tenantCode, List<Remittance> validRemittanceList, CancellationToken cancellationToken)
        {
            if (!validRemittanceList.Any())
            {
                _logger.LogInformation("Sucess! Remittance is up to date for tenant {TenantCode}!", tenantCode);
                return;
            }

            var mappingAccountsTask = _repository.GetMappingAccounts(cancellationToken);
            var tenantId = InsuranceHelper.GetTenantId(tenantCode, _tellmaOptions.Value.Tenants);

            var tenantProfile = await _service.GetTenantProfile(tenantId, cancellationToken);

            // Log tenant info
            _logger.LogInformation("\n \n Processing tenant {TenantCode} (ID: {TenantId}, Name: {TenantName}) with {Count} remittance worksheets. \n \n",
                tenantCode, tenantId, tenantProfile.CompanyName, validRemittanceList.Count);

            validRemittanceList = ValidateWorksheets(validRemittanceList);

            // validate creation or updating of worksheets
            RemoveIf(ref validRemittanceList, r => r.TellmaDocumentId > 0 && r.PostingDate < tenantProfile.ArchiveDate, $"have a posting date before or on the archive date {tenantProfile.ArchiveDate.ToString("yyyy-MMM-dd")} for existing remittances");
            RemoveIf(ref validRemittanceList, r => r.PostingDate < tenantProfile.FreezeDate, $"have a posting date before or on the freeze date {tenantProfile.FreezeDate.ToString("yyyy-MMM-dd")} for new remittances");

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

            // BankAccounts validation
            // Can't sync due to missing bankId
            int bankAccountDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.BankAccount.AsString(), token: cancellationToken);
            string? baBatchFilter = String.Join(" OR ", validRemittanceList.Where(ba => !String.IsNullOrWhiteSpace(ba.BankAccountCode)).Select(r => $"Text3='{r.BankAccountCode}'").Distinct());
            baBatchFilter = baBatchFilter.Length < 1024 ? baBatchFilter : null;
            var bankAccountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(), bankAccountDefinitionId, baBatchFilter, token: cancellationToken);
            var bankAccountsResult = bankAccountsObjectResult.ConvertAll(agent => (Agent)agent);
            var tellmaBankAccountsCodes = bankAccountsResult.Select(ba => ba.Text3);
            var missingBARemittances = validRemittanceList
                .Where(r => !tellmaBankAccountsCodes.Contains(r.BankAccountCode) && r.RemittanceType.ToLower() != "write_off" && r.RemittanceType.ToLower() != "bcharge")
                .Select(r => r.WorksheetId)
                .Distinct()
                .ToList();

            if (missingBARemittances.Any())
            {
                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] bank accounts [{bankAccounts}] are not defined in tellma.", missingBARemittances.Count, string.Join(", ", missingBARemittances), string.Join(", ", validRemittanceList.Where(r => missingBARemittances.Contains(r.WorksheetId)).Select(r => r.BankAccountCode).Distinct()));

                validRemittanceList = validRemittanceList
                    .Where(r => !missingBARemittances.Contains(r.WorksheetId))
                    .ToList();
            }

            // BankAccounts-Currencies validation
            var tellmaBankAccCurrencies = bankAccountsResult
                .Select(ba => ba.Text3 + " - " + ba.CurrencyId)
                .Distinct();

            var missingBACurrencyRemittances = validRemittanceList
                .Where(r => r.RemittanceType.ToLower() != "write_off" && r.RemittanceType.ToLower() != "bcharge")
                .Where(r => !tellmaBankAccCurrencies.Contains(r.BankAccountCode + " - " + r.BankAccountCurrencyId))
                .Select(r => r.WorksheetId)
                .Distinct();

            if (missingBACurrencyRemittances.Any())
            {
                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] bank accounts and their currencies [{Currencies}] do not match tellma.", missingBACurrencyRemittances.Count(), string.Join(", ", missingBACurrencyRemittances), string.Join(", ", validRemittanceList.Where(r => missingBACurrencyRemittances.Contains(r.WorksheetId)).Select(r => "bank account IBAN: " + r.BankAccountCode + " - Currency:" + r.BankAccountCurrencyId)));

                validRemittanceList = validRemittanceList
                    .Where(r => !missingBACurrencyRemittances.Contains(r.WorksheetId))
                    .ToList();
            }

            var mappingAccounts = await mappingAccountsTask;

            // Remove worksheets with invalid remittance type.
            var supportedRemittanceTypes = mappingAccounts.Select(ma => ma.RemittanceType.ToLower()).Distinct().ToList();
            var invalidTypeIds = validRemittanceList.Where(r => !supportedRemittanceTypes.Any(rt => r.RemittanceType.ToLower().Equals(rt))).Select(t => t.WorksheetId).Distinct().ToList();
            if (invalidTypeIds.Any())
            {
                _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid remittance type [{Types}].", invalidTypeIds.Count, string.Join(", ", invalidTypeIds), string.Join(", ", validRemittanceList.Where(r => invalidTypeIds.Contains(r.WorksheetId)).Select(r => r.RemittanceType)));
                validRemittanceList = validRemittanceList.Where(r => supportedRemittanceTypes.Any(rt => r.RemittanceType.ToLower().Equals(rt))).ToList();
            }

            validRemittanceList = MapRemittanceAccounts(validRemittanceList, mappingAccounts);

            validRemittanceList = ValidateWorksheets(validRemittanceList);

            if (!validRemittanceList.Any())
            {
                _logger.LogInformation("Sucess! Remittance is up to date for tenant {TenantCode}!", tenantCode);
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

            // Remove invalid accounts
            var invalidAccountIds = accountCodes.Where(ac => !accountsResult.Any(a => a.Code == ac)).ToList();
            if (invalidAccountIds.Any())
            {
                var invalidRemittanceIds = validRemittanceList
                    .Where(r => invalidAccountIds.Contains(r.AAccount) || invalidAccountIds.Contains(r.BAccount))
                    .Select(r => r.WorksheetId)
                    .Distinct()
                    .ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] have invalid accounts [{Accounts}].", invalidRemittanceIds.Count, string.Join(", ", invalidRemittanceIds), string.Join(", ", invalidAccountIds));
                validRemittanceList = validRemittanceList
                    .Where(r => !invalidRemittanceIds.Contains(r.WorksheetId))
                    .ToList();
            }

            // Currencies batch
            var currenciesObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Currencies.AsString(), token: cancellationToken);
            var currenciesResult = currenciesObjectResult.ConvertAll(c => (Currency)c);

            // Remove remittance with currencies not in Tellma
            var validCurrencies = currenciesResult.Select(c => c.Id).ToList();
            var invalidCurrencies = validRemittanceList
                .Where(r => r.RemittanceType.ToLower() != "write_off" && r.RemittanceType.ToLower() != "bcharge")
                .Where(w => !validCurrencies.Contains(w.BankAccountCurrencyId) || !validCurrencies.Contains(w.TransferCurrencyId)).Select(w => new{ w.BankAccountCurrencyId, w.TransferCurrencyId });
            RemoveIf(ref validRemittanceList,
                w => w.RemittanceType.ToLower() != "write_off" && w.RemittanceType.ToLower() != "bcharge"  && (!validCurrencies.Contains(w.BankAccountCurrencyId) || !validCurrencies.Contains(w.TransferCurrencyId)),
                $"have currencies {string.Join(", ", invalidCurrencies)} not found in Tellma.");

            if (!validRemittanceList.Any())
            {
                _logger.LogInformation("Sucess! Remittance is up to date for tenant {TenantCode}!", tenantCode);
                return;
            }

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
                if (!validRemittanceList.Any())
                {
                    _logger.LogInformation("Sucess! Remittance is up to date for tenant {TenantCode}!", tenantCode);
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
                    _logger.LogWarning("Recipient/Issuer name for remittance RW{serialNumber} exceeds 50 characters and will be truncated.", serialNumber);
                    insuranceAgentName = insuranceAgentName.Substring(0, 50);
                }

                var accountAId = accountsResult.FirstOrDefault(a => a.Code == remittance.AAccount)?.Id;
                var accountBId = accountsResult.FirstOrDefault(a => a.Code == remittance.BAccount)?.Id;
                
                var entryTypeAId = entryTypesResult.FirstOrDefault(et => et.Concept == remittance.APurposeConcept)?.Id;
                var entryTypeBId = entryTypesResult.FirstOrDefault(et => et.Concept == remittance.BPurposeConcept)?.Id;

                var agentAId = remittance.AIsBankAcc ? bankAccountId : insuranceAgentId;
                var agentBId = remittance.BIsBankAcc ? bankAccountId : insuranceAgentId;

                string reference = remittance.Reference;
                if (reference.Length > 50)
                {
                    _logger.LogWarning("Reference for remittance RW{serialNumber} exceeds 50 characters and will be truncated.", serialNumber);
                    reference = reference.Substring(0, 50);
                }

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
                    ExternalReference = remittance.AIsBankAcc ? reference : null,
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
                    ExternalReference = remittance.BIsBankAcc ? reference : null,
                    CenterId = operationCenterId,
                    NotedAgentName = remittance.BIsBankAcc ? insuranceAgentName : null
                }
            };

                if (entriesList[0].Direction < 0)
                    entriesList.Reverse();

                var document = new DocumentForSave
                {
                    Id = remittance.TellmaDocumentId,
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
                if (!remittanceDocuments.Any())
                {
                    _logger.LogWarning("No remittance documents to process for tenant {TenantCode}", tenantCode);
                    return;
                }

                _logger.LogInformation("Importing {count} remittance documents for tenant {Tenant}", remittanceDocuments.Count, tenantCode);
                var remittanceDocumentsResult = await _service.SaveDocuments(tenantId, remittanceDefinitionId, remittanceDocuments, cancellationToken);
                var remittanceRecords = remittanceDocumentsResult
                    .Select(r => new Remittance
                    {
                        WorksheetId = $"RW{r.SerialNumber}",
                        TellmaDocumentId = r.Id
                    });

                await _repository.UpdateDocumentIds(tenantCode, remittanceRecords, cancellationToken);
                var documentIds = remittanceRecords.Select(r => r.TellmaDocumentId).ToList();
                await _service.CloseDocuments(tenantId, remittanceDefinitionId, documentIds, cancellationToken);
                await _repository.UpdateImportedWorksheets(tenantCode, remittanceRecords, cancellationToken);
                _logger.LogInformation("Remittance import finished!");
            }
            catch (Exception ex)
            {
                // Continue insurance upload.
                _logger.LogError("An error occurred while importing remittances: {ErrorMessage}", ex.Message);
                throw;
            }
        }

        private List<Remittance> MapRemittanceAccounts(List<Remittance> validRemittanceList, List<Remittance> mappingAccounts)
        {
            foreach (var remittance in validRemittanceList)
            {
                var mappingAccount = mappingAccounts.FirstOrDefault(ma => ma.RemittanceType.ToLower() == remittance.RemittanceType.ToLower()
                                                                        && ma.Direction == remittance.Direction);
                if (mappingAccount != null)
                {
                    remittance.RemittanceTypeName = mappingAccount.RemittanceTypeName;
                    remittance.AAccount = mappingAccount.AAccount;
                    remittance.BAccount = mappingAccount.BAccount;
                    remittance.APurposeConcept = mappingAccount.APurposeConcept;
                    remittance.BPurposeConcept = mappingAccount.BPurposeConcept;
                    remittance.ADirection = mappingAccount.ADirection;
                    remittance.BDirection = mappingAccount.BDirection;
                    remittance.AIsBankAcc = mappingAccount.AIsBankAcc;
                    remittance.BIsBankAcc = mappingAccount.BIsBankAcc;
                    remittance.ANotedAgentId = mappingAccount.ANotedAgentId;
                    remittance.BNotedAgentId = mappingAccount.BNotedAgentId;
                    remittance.AResourceId = mappingAccount.AResourceId;
                    remittance.BResourceId = mappingAccount.BResourceId;
                    remittance.ANotedResourceId = mappingAccount.ANotedResourceId;
                    remittance.BNotedResourceId = mappingAccount.BNotedResourceId;
                    remittance.AHasNOTEDDATE = mappingAccount.AHasNOTEDDATE;
                    remittance.BHasNOTEDDATE = mappingAccount.BHasNOTEDDATE;
                }
            }
            return validRemittanceList;
        }

        private List<Remittance> ValidateWorksheets(List<Remittance> worksheets)
        {
            var valid = worksheets.ToList();

            RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.AgentCode), "have an invalid insurance agent");
            RemoveIf(ref valid, w => w.ValueFC2 == 0, "have zero functional value");
            RemoveIf(ref valid, w => Math.Abs(w.Direction) != 1 && w.ValueFC2 != 0 && w.TransferAmount != 0, $"Worksheet must have valid direction [{string.Join(", ", valid.Where(w => Math.Abs(w.Direction) != 1 && w.ValueFC2 != 0 && w.TransferAmount != 0).Select(w => w.Direction).Distinct())}], must be either 1 or -1.");

            // Keep only worksheets with supported prefixes
            var supportedPrefixes = (_insuranceOptions.CurrentValue.RemittanceSupportedPrefixes ?? "RW")
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();

            var invalidTypeIds = valid.Where(r => !supportedPrefixes.Any(p => r.WorksheetId.StartsWith(p))).Select(t => t.WorksheetId).Distinct().ToList();
            if (invalidTypeIds.Any())
            {
                valid = valid.Where(t => supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid remittance prefix.", invalidTypeIds.Count, string.Join(", ", invalidTypeIds));
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