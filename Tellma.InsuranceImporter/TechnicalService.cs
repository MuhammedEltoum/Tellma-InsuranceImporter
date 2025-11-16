using Microsoft.Extensions.Logging;
using System.Reflection.Metadata.Ecma335;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Enums;
using Tellma.InsuranceImporter.Repository;
using Tellma.Model.Application;
using Tellma.Utilities.EmailLogger;

namespace Tellma.InsuranceImporter
{
    public class TechnicalService : IImportService<Technical>
    {
        private readonly ITellmaService _service;
        private readonly IWorksheetRepository<Technical> _repository;
        private readonly ILogger<TechnicalService> _logger;

        public TechnicalService(IWorksheetRepository<Technical> repository, ITellmaService service, ILogger<TechnicalService> logger)
        {
            _service = service ?? throw new ArgumentNullException(nameof(service));
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task Import(string tenantCode, CancellationToken cancellationToken)
        {
            var allWorksheets = (await _repository.GetWorksheets(cancellationToken)).ToList();

            var validWorksheets = ValidateWorksheets(allWorksheets);

            cancellationToken.ThrowIfCancellationRequested();

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

        private async Task ProcessTenant(string tenantCode, List<Technical> tenantWorks, CancellationToken cancellationToken)
        {
            if (!tenantWorks.Any())
            {
                _logger.LogWarning("No worksheets for tenant {TenantCode}", tenantCode);
                return;
            }

            var mappingAccountsTask = _repository.GetMappingAccounts(cancellationToken);

            var tenantId = InsuranceHelper.GetTenantId(tenantCode);

            var tenantProfile = await _service.GetTenantProfile(tenantId, cancellationToken);

            // Log tenant info
            _logger.LogInformation("\n \n Processing tenant {TenantCode} (ID: {TenantId}, Name: {TenantName}) with {Count} technicals worksheets. \n \n",
                tenantCode, tenantId, tenantProfile.CompanyName, tenantWorks.Count);

            // validate creation or updating of worksheets
            RemoveIf(ref tenantWorks, r => r.TellmaDocumentId > 0 && r.PostingDate < tenantProfile.ArchiveDate, "have a posting date before or on the archive date for existing technicals");
            RemoveIf(ref tenantWorks, r => r.PostingDate < tenantProfile.FreezeDate, "have a posting date before or on the freeze date for new technicals");

            // Collections to hold documents before saving
            var technicalDocuments = new List<DocumentForSave>();
            var claimsDocuments = new List<DocumentForSave>();

            // Agents/partners/accounts to be used in building documents
            var insuranceAgents = new List<Agent>();

            // Insurance Agents (primary)
            int insuranceAgentDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.AgentDefinitions.AsString(), TellmaEntityCode.InsuranceAgent.AsString(), token: cancellationToken);

            var primaryAgents = tenantWorks
                .Select(a => new { Code = a.AgentCode, Name = a.AgentName })
                .Distinct()
                .Select(a => new Agent { Code = a.Code, Name = a.Name, Name2 = a.Name })
                .ToList();

            if (primaryAgents.Any())
            {
                var synced = await _service.SyncAgents(tenantId, TellmaEntityCode.InsuranceAgent.AsString(), primaryAgents, cancellationToken);
                insuranceAgents.AddRange(synced);
            }

            int insuranceAgentsCount = insuranceAgents.Count;

            // Broker
            await SyncAgentGroup(tenantWorks,
                insList: insuranceAgents,
                selector: w => new { Code = w.BrokerCode, Name = w.BrokerName },
                tenantId: tenantId,
                definitionCode: TellmaEntityCode.InsuranceAgent.AsString(),
                cancellationToken: cancellationToken);

            insuranceAgentsCount = CheckInsuranceAgentsCount(insuranceAgentsCount, insuranceAgents.Count, "brokers");

            // Channel
            await SyncAgentGroup(tenantWorks,
                insList: insuranceAgents,
                selector: w => new { Code = w.ChannelCode, Name = w.ChannelName },
                predicate: w => !string.IsNullOrWhiteSpace(w.ChannelCode) && !string.IsNullOrWhiteSpace(w.ChannelName),
                tenantId: tenantId,
                definitionCode: TellmaEntityCode.InsuranceAgent.AsString(),
                cancellationToken: cancellationToken);

            insuranceAgentsCount = CheckInsuranceAgentsCount(insuranceAgentsCount, insuranceAgents.Count, "channel");

            // Cedant
            await SyncAgentGroup(tenantWorks,
                insList: insuranceAgents,
                selector: w => new { Code = w.CedantCode, Name = w.CedantName },
                predicate: w => !string.IsNullOrWhiteSpace(w.CedantCode) && !string.IsNullOrWhiteSpace(w.CedantName),
                tenantId: tenantId,
                definitionCode: TellmaEntityCode.InsuranceAgent.AsString(),
                cancellationToken: cancellationToken);

            insuranceAgentsCount = CheckInsuranceAgentsCount(insuranceAgentsCount, insuranceAgents.Count, "cedant");

            // Reinsurer
            await SyncAgentGroup(tenantWorks,
                insList: insuranceAgents,
                selector: w => new { Code = w.ReinsurerCode, Name = w.ReinsurerName },
                predicate: w => !string.IsNullOrWhiteSpace(w.ReinsurerCode) && !string.IsNullOrWhiteSpace(w.ReinsurerName),
                tenantId: tenantId,
                definitionCode: TellmaEntityCode.InsuranceAgent.AsString(),
                cancellationToken: cancellationToken);
            
            insuranceAgentsCount = CheckInsuranceAgentsCount(insuranceAgentsCount, insuranceAgents.Count, "reinsurer");

            // Insured
            await SyncAgentGroup(tenantWorks,
                insList: insuranceAgents,
                selector: w => new { Code = w.InsuredCode, Name = w.InsuredName },
                predicate: w => !string.IsNullOrWhiteSpace(w.InsuredCode) && !string.IsNullOrWhiteSpace(w.InsuredName),
                tenantId: tenantId,
                definitionCode: TellmaEntityCode.InsuranceAgent.AsString(),
                cancellationToken: cancellationToken);

            insuranceAgentsCount = CheckInsuranceAgentsCount(insuranceAgentsCount, insuranceAgents.Count, "insured");


            // Business types
            var businessTypesCodes = tenantWorks.Select(w => w.BusinessTypeCode).Distinct().Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
            string? businessTypesFilter = businessTypesCodes.Any() ? string.Join(" OR ", businessTypesCodes.Select(b => $"Code='{b}'")) : null;
            if (businessTypesFilter != null && businessTypesFilter.Length >=1024)
            {
                businessTypesFilter = null;
            }

            int businessTypeDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.BusinessType.AsString(), token: cancellationToken);
            var businessTypesObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Lookups.AsString(), businessTypeDefinitionId, businessTypesFilter, token: cancellationToken);
            var businessTypesResult = businessTypesObjectResult.ConvertAll(bt => (Lookup)bt);

            var missingBusinessTypes = tenantWorks.Where(t => !string.IsNullOrWhiteSpace(t.BusinessTypeCode) && !businessTypesResult.Select(bt => bt.Code).Contains(t.BusinessTypeCode)).Select(t => t.WorksheetId).Distinct().ToList();
            if (missingBusinessTypes.Any())
            {
                tenantWorks = tenantWorks.Where(t => !missingBusinessTypes.Contains(t.WorksheetId)).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] business types do not exist in tellma.", missingBusinessTypes.Count, string.Join(", ", missingBusinessTypes));
            }

            // Main business classes
            var mainBusinessCodes = tenantWorks.Select(w => w.BusinessMainClassCode).Distinct().Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
            string? mainBusinessFilter = mainBusinessCodes.Any() ? string.Join(" OR ", mainBusinessCodes.Select(b => $"Code = '{b}'")) : null;
            int mainBusinessClassDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.MainBusinessClass.AsString(), token: cancellationToken);
            var mainBusinessObjects = await _service.GetClientEntities(tenantId, TellmaClientProperty.Lookups.AsString(), mainBusinessClassDefinitionId, mainBusinessFilter, token: cancellationToken);
            var mainBusinessResult = mainBusinessObjects.ConvertAll(m => (Lookup)m);

            var missingMainBusiness = tenantWorks.Where(t => !string.IsNullOrWhiteSpace(t.BusinessMainClassCode) && !mainBusinessResult.Select(m => m.Code).Contains(t.BusinessMainClassCode)).Select(t => t.WorksheetId).Distinct().ToList();
            if (missingMainBusiness.Any())
            {
                tenantWorks = tenantWorks.Where(t => !missingMainBusiness.Contains(t.WorksheetId)).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetIds [{Ids}] main business classes do not exist in tellma.", missingMainBusiness.Count, string.Join(", ", missingMainBusiness));
            }

            // Risk countries
            var riskCountries = tenantWorks.Select(t => t.RiskCountry).Distinct().Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
            string? riskCountriesFilter = riskCountries.Any() ? string.Join(" OR ", riskCountries.Select(r => $"Code='{r}'")) : null;
            if (riskCountriesFilter != null && riskCountriesFilter.Length >=1024) riskCountriesFilter = null;
            int riskCountryDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.Citizenship.AsString(), token: cancellationToken);
            var riskCountriesObjects = await _service.GetClientEntities(tenantId, TellmaClientProperty.Lookups.AsString(), riskCountryDefinitionId, riskCountriesFilter, token: cancellationToken);
            var riskCountriesResult = riskCountriesObjects.ConvertAll(rc => (Lookup)rc);


            // Contracts
            var groupedContracts = tenantWorks
                .GroupBy(c => c.ContractCode)
                .Select(g => g.OrderByDescending(c => c.PostingDate).First())
                .Select(c => new Agent
                {
                    Code = c.ContractCode,
                    Name = $"{c.ContractCode}: {c.ContractName}",
                    Name2 = $"{c.ContractCode}: {c.ContractName}",
                    Lookup1Id = businessTypesResult.FirstOrDefault(bt => bt.Code == c.BusinessTypeCode)?.Id,
                    Lookup3Id = riskCountriesResult.FirstOrDefault(rc => rc.Code == c.RiskCountry)?.Id,
                    FromDate = tenantWorks.Where(ws => ws.ContractCode == c.ContractCode).Min(ws => ws.EffectiveDate),
                    ToDate = c.ExpiryDate,
                    Agent2Id = insuranceAgents.FirstOrDefault(ia => ia.Code == c.BrokerCode)?.Id,
                    Description2 = $"Max closing date = {c.ClosingDate:yyyy-MM-dd}",
                    Description = c.Description,
                })
                .ToList();

            var contractsResult = await _service.SyncAgents(tenantId, TellmaEntityCode.InsuranceContract.AsString(), groupedContracts, cancellationToken);

            // Partnership types
            int partnershipTypeDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.PartnershipTypes.AsString(), token: cancellationToken);
            var partnershipTypeObjects = await _service.GetClientEntities(tenantId, TellmaClientProperty.Lookups.AsString(), partnershipTypeDefinitionId, token: cancellationToken);
            var partnershipTypesResult = partnershipTypeObjects.ConvertAll(l => (Lookup)l);

            // Business partners
            var cedantContracts = BuildPartnerList(
                tenantWorks,
                insuranceAgents,
                contractsResult,
                partnershipTypesResult,
                c => c.CedantCode,
                "Cedant");

            var channelContracts = BuildPartnerList(
                tenantWorks,
                insuranceAgents,
                contractsResult,
                partnershipTypesResult,
                c => c.ChannelCode,
                "BrokerCh");

            var insuredContracts = BuildPartnerList(
                tenantWorks,
                insuranceAgents,
                contractsResult,
                partnershipTypesResult,
                c => c.InsuredCode,
                "Insured");

            var reinsurerContracts = BuildPartnerList(
                tenantWorks,
                insuranceAgents,
                contractsResult,
                partnershipTypesResult,
                c => c.ReinsurerCode,
                "Reinsurer");

            var businessPartners = new List<Agent>();
            businessPartners.AddRange(cedantContracts);
            businessPartners.AddRange(channelContracts);
            businessPartners.AddRange(insuredContracts);
            businessPartners.AddRange(reinsurerContracts);

            businessPartners = businessPartners
                .Where(bp => bp.Agent1Id != null && bp.Agent2Id != null && bp.Lookup1Id != null)
                .OrderBy(bp => bp.Agent1Id)
                .ToList();

            var businessPartnersResult = await _service.SyncAgents(
                    tenantId,
                    TellmaEntityCode.BusinessPartner.AsString(),
                    businessPartners,
                    cancellationToken);

            // Customer accounts
            var dbCustomerAccounts = tenantWorks
                .Select(cstmr => new
                {
                    Code = $"{cstmr.ContractCode}-{cstmr.BusinessMainClassCode}-{cstmr.AgentCode}",
                    Name = $"{cstmr.ContractCode}-{cstmr.BusinessMainClassCode}-{cstmr.AgentCode}: " + tenantWorks
                        .Where(tw => tw.ContractCode == cstmr.ContractCode
                            && tw.BusinessMainClassCode == cstmr.BusinessMainClassCode
                            && tw.AgentCode == cstmr.AgentCode)
                        .OrderByDescending(w => w.PostingDate)
                        .FirstOrDefault()?
                        .ContractName,
                    Name2 = $"{cstmr.ContractCode}-{cstmr.BusinessMainClassCode}-{cstmr.AgentCode}: " + tenantWorks
                        .Where(tw => tw.ContractCode == cstmr.ContractCode
                            && tw.BusinessMainClassCode == cstmr.BusinessMainClassCode
                            && tw.AgentCode == cstmr.AgentCode)
                        .OrderByDescending(w => w.PostingDate)
                        .FirstOrDefault()?
                        .ContractName,
                    Agent1Id = insuranceAgents.FirstOrDefault(ia => ia.Code == cstmr.AgentCode)?.Id,
                    Agent2Id = contractsResult.FirstOrDefault(c => c.Code == cstmr.ContractCode)?.Id,
                    Lookup2Id = mainBusinessResult.FirstOrDefault(m => m.Code == cstmr.BusinessMainClassCode)?.Id,
                })
                .Distinct()
                .Select(cstmr => new Agent
                {
                    Code = cstmr.Code,
                    Name = cstmr.Name,
                    Name2 = cstmr.Name2,
                    Agent1Id = cstmr.Agent1Id,
                    Agent2Id = cstmr.Agent2Id,
                    Lookup2Id = cstmr.Lookup2Id,
                })
                .ToList();

            var customerAccResult = await _service.SyncAgents(tenantId, TellmaEntityCode.TradeReceivableAccount.AsString(), dbCustomerAccounts, cancellationToken);


            var mappingAccounts = await mappingAccountsTask;

            // Keep only worksheets with supported SICS Accounts
            var supportedSICSAcc = mappingAccounts.Select(ma => $"{ma.AccountCode}-{ma.IsInward}").Distinct().ToList();
            var invalidTypeIds = tenantWorks.Where(t => !supportedSICSAcc.Any(p => $"{t.AccountCode}-{t.IsInward}" == $"{p}")).Select(t => t.WorksheetId).Distinct().ToList();
            if (invalidTypeIds.Any())
            {
                tenantWorks = tenantWorks.Where(t => supportedSICSAcc.Any(p => $"{t.AccountCode}-{t.IsInward}" == $"{p}")).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid SICS account.", invalidTypeIds.Count, string.Join(", ", invalidTypeIds));
            }

            tenantWorks = MapTechnicalAccounts(tenantWorks, mappingAccounts);

            tenantWorks = ValidateWorksheets(tenantWorks);

            if (!tenantWorks.Any())
            {
                _logger.LogWarning("No new technical records to sync for tenant {Tenant}", tenantCode);
                return;
            }

            // Accounts batch
            var accountCodes = tenantWorks.SelectMany(w => new[] { w.AAccount, w.BAccount })
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct()
                .ToList();
            string? accountsFilter = accountCodes.Any() ? string.Join(" OR ", accountCodes.Select(a => $"Code = '{a}'")) : null;
            var accountsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Accounts.AsString(), filter: accountsFilter, token: cancellationToken);
            var accountsResult = accountsObjectResult.ConvertAll(a => (Account)a);

            // Entry types
            var entryTypeConcepts = tenantWorks.SelectMany(w => new[] { w.APurposeConcept, w.BPurposeConcept })
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct()
                .ToList();
            string? entryTypesFilter = entryTypeConcepts.Any() ? string.Join(" OR ", entryTypeConcepts.Select(et => $"Concept='{et}'")) : null;
            var entryTypesObjects = await _service.GetClientEntities(tenantId, TellmaClientProperty.EntryTypes.AsString(), filter: entryTypesFilter, token: cancellationToken);
            var entryTypesResult = entryTypesObjects.ConvertAll(et => (EntryType)et);

            // Document definitions and other ids
            int techDocDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.DocumentDefinitions.AsString(), TellmaEntityCode.TechnicalWorksheet.AsString(), token: cancellationToken);
            int claimDocDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.DocumentDefinitions.AsString(), TellmaEntityCode.ClaimWorksheet.AsString(), token: cancellationToken);

            int lineDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LineDefinitions.AsString(), TellmaEntityCode.ManualLine.AsString(), token: cancellationToken);

            int taxDepartmentDefinitionId = await _service.GetIdByCodeAsync(tenantId, 
                TellmaClientProperty.AgentDefinitions.AsString(), 
                TellmaEntityCode.TaxDepartment.AsString(), 
                token: cancellationToken);

            int vatDeptId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.Agents.AsString(),
                TellmaEntityCode.ValueAddedTax.AsString(),
                taxDepartmentDefinitionId,
                token: cancellationToken);

            string operationCenterCode = TellmaEntityCode.OperationCenter.AsString();
            int operationCenterId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Centers.AsString(), operationCenterCode, token: cancellationToken);

            int inwardOutwardDefinitionId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.LookupDefinitions.AsString(), TellmaEntityCode.TechnicalInOutward.AsString(), token: cancellationToken);
            int inwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Inward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);
            int outwardLookupId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Lookups.AsString(), TellmaEntityCode.Outward.AsString(), inwardOutwardDefinitionId, token: cancellationToken);

            // Build documents
            foreach (var technical in tenantWorks)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int inwardOutwardLookupId = technical.IsInward ? inwardLookupId : outwardLookupId;
                int serialNumber = Convert.ToInt32(technical.WorksheetId?.Substring(2));

                string? memo = tenantWorks.Where(w => w.WorksheetId == technical.WorksheetId).Max(w => w.TechnicalNotes) ?? "-";
                if (memo.Length >255)
                {
                    _logger.LogWarning("Memo for worksheet {WorksheetId} exceeds255 characters and will be truncated.", technical.WorksheetId);
                    memo = memo.Substring(0,255);
                }

                string customerAccCode = $"{technical.ContractCode}-{technical.BusinessMainClassCode}-{technical.AgentCode}";
                int? customerAccId = customerAccResult.FirstOrDefault(ca => ca.Code == customerAccCode)?.Id ??0;
                if (customerAccId ==0)
                {
                    customerAccId = await _service.GetIdByCodeAsync(tenantId, TellmaClientProperty.Agents.AsString(), customerAccCode, token: cancellationToken);
                }

                int accountAId = accountsResult.FirstOrDefault(a => a.Code == technical.AAccount)?.Id ??0;
                int accountBId = accountsResult.FirstOrDefault(a => a.Code == technical.BAccount)?.Id ??0;

                int? entryTypeAId = entryTypesResult.FirstOrDefault(et => et.Concept == technical.APurposeConcept)?.Id;
                int? entryTypeBId = entryTypesResult.FirstOrDefault(et => et.Concept == technical.BPurposeConcept)?.Id;

                var maxNotedDate = tenantWorks.Where(w => w.WorksheetId == technical.WorksheetId).Max(w => w.NotedDate);

                var entriesList = new List<EntryForSave>
                {
                    new EntryForSave
                    {
                        AccountId = accountAId,
                        EntryTypeId = entryTypeAId,
                        //Reverse direction based on Direction
                        Direction = (short)(technical.Direction > 0 ? 1 : -1),
                        Value = technical.ValueFc2,
                        MonetaryValue = technical.ContractAmount,
                        AgentId = !technical.ATaxAccount ? customerAccId : vatDeptId,
                        NotedAgentId = technical.ATaxAccount ? customerAccId : null,
                        CurrencyId = technical.ContractCurrencyId,
                        CenterId = operationCenterId,
                        NotedDate = technical.AHasNotedDate ? maxNotedDate : null,
                        Time1 = technical.EffectiveDate,
                        Time2 = technical.ExpiryDate
                    },
                    new EntryForSave
                    {
                        AccountId = accountBId,
                        EntryTypeId = entryTypeBId,
                        //Reverse direction based on Direction
                        Direction = (short)(technical.Direction < 0 ? 1 : -1),
                        Value = technical.ValueFc2,
                        MonetaryValue = technical.ContractAmount,
                        CurrencyId = technical.ContractCurrencyId,
                        AgentId = !technical.BTaxAccount ? customerAccId : vatDeptId,
                        NotedAgentId = technical.BTaxAccount ? customerAccId : null,
                        CenterId = operationCenterId,
                        NotedDate = technical.BHasNotedDate ? maxNotedDate : null,
                        Time1 = technical.EffectiveDate,
                        Time2 = technical.ExpiryDate
                    }
                };

                if (entriesList[0].Direction <0)
                {
                    entriesList.Reverse();
                }

                // Aggregate to existing document or create new
                DocumentForSave? document = null;
                if (technical.WorksheetId.StartsWith("TW"))
                {
                    document = technicalDocuments.FirstOrDefault(d => d.SerialNumber == serialNumber);
                    if (document != null)
                    {
                        document.Lines[0].Entries.AddRange(entriesList);
                    }
                }
                else if (technical.WorksheetId.StartsWith("CW"))
                {
                    document = claimsDocuments.FirstOrDefault(d => d.SerialNumber == serialNumber);
                    if (document != null)
                    {
                        document.Lines[0].Entries.AddRange(entriesList);
                    }
                }

                if (document == null)
                {
                    document = new DocumentForSave
                    {
                        Id = technical?.TellmaDocumentId ??0,
                        SerialNumber = serialNumber,
                        PostingDate = technical?.PostingDate.AddDays(1 - technical.PostingDate.Day),
                        PostingDateIsCommon = true,
                        Lookup1Id = inwardOutwardLookupId,
                        Memo = memo,
                        MemoIsCommon = true,
                        CenterIsCommon = true,
                        Lines = new List<LineForSave>()
                    };

                    document.Lines.Add(new LineForSave { DefinitionId = lineDefinitionId, Entries = entriesList });

                    if (technical.WorksheetId.StartsWith("TW")) technicalDocuments.Add(document);
                    if (technical.WorksheetId.StartsWith("CW")) claimsDocuments.Add(document);
                }
            }

            try
            {
                if (technicalDocuments.Any())
                {
                    var techResult = await _service.SaveDocuments(tenantId, techDocDefinitionId, technicalDocuments, cancellationToken);
                    var technicalRecords = techResult.Select(r => new Technical { WorksheetId = $"TW{r.SerialNumber}", TellmaDocumentId = r.Id });

                    await _repository.UpdateDocumentIds(tenantCode, technicalRecords, cancellationToken);
                    var documentIds = technicalRecords.Select(r => r.TellmaDocumentId).ToList();
                    await _service.CloseDocuments(tenantId, techDocDefinitionId, documentIds, cancellationToken);
                    await _repository.UpdateImportedWorksheets(tenantCode, technicalRecords, cancellationToken);
                    _logger.LogInformation("Technical import finished for tenant {Tenant}", tenantCode);
                }

                if (claimsDocuments.Any())
                {
                    var claimsResult = await _service.SaveDocuments(tenantId, claimDocDefinitionId, claimsDocuments, cancellationToken);
                    var claimRecords = claimsResult.Select(r => new Technical { WorksheetId = $"CW{r.SerialNumber}", TellmaDocumentId = r.Id });

                    await _repository.UpdateDocumentIds(tenantCode, claimRecords, cancellationToken);
                    var documentIds = claimRecords.Select(r => r.TellmaDocumentId).ToList();
                    await _service.CloseDocuments(tenantId, techDocDefinitionId, documentIds, cancellationToken);
                    await _repository.UpdateImportedWorksheets(tenantCode, claimRecords, cancellationToken);
                    _logger.LogInformation("Claims import finished for tenant {Tenant}", tenantCode);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occured while importing technicals for tenant {Tenant}", tenantCode);
                throw;
            }
        }

        private int CheckInsuranceAgentsCount(int insuranceAgentsCount, int updatedInsuranceAgentsCount, string agentCode)
        {
            if (insuranceAgentsCount == updatedInsuranceAgentsCount)
            {
                _logger.LogWarning("No new {agentCode} to sync", agentCode);
            }
            else
            {
                insuranceAgentsCount = updatedInsuranceAgentsCount;
            }

            return insuranceAgentsCount;
        }

        private List<Technical> MapTechnicalAccounts(List<Technical> tenantWorks, List<Technical> mappingAccounts)
        {
            foreach (var tw in tenantWorks)
            {
                var mapping = mappingAccounts.FirstOrDefault(ma => ma.AccountCode == tw.AccountCode && ma.IsInward == tw.IsInward);

                if (mapping == null)
                    _logger.LogWarning("WorksheetId {WorksheetId} has SICS account {acc} for is_insward = {isInward} technical which is not defined in mapping table.", tw.WorksheetId, tw.AccountCode, tw.IsInward);

                if (mapping != null)
                {
                    tw.AAccount = mapping.AAccount;
                    tw.BAccount = mapping.BAccount;
                    tw.APurposeConcept = mapping.APurposeConcept;
                    tw.BPurposeConcept = mapping.BPurposeConcept;
                    tw.ATaxAccount = mapping.ATaxAccount;
                    tw.BTaxAccount = mapping.BTaxAccount;
                    tw.AHasNotedDate = mapping.AHasNotedDate;
                    tw.BHasNotedDate = mapping.BHasNotedDate;
                }
            }
            return tenantWorks;
        }
        private List<Technical> ValidateWorksheets(List<Technical> worksheets)
        {
            var valid = worksheets.ToList();

            RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.AgentCode), "have an invalid insurance agent");
            RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.ContractCode), "have an invalid contract");
            RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.BusinessMainClassCode), "have an invalid business main class");
            RemoveIf(ref valid, w => Math.Abs(w.Direction) != 1, "have an invalid direction");

            // Keep only worksheets with supported prefixes
            var supportedPrefixes = new[] { "TW", "RT", "CW" };
            var invalidTypeIds = valid.Where(t => !supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).Select(t => t.WorksheetId).Distinct().ToList();
            if (invalidTypeIds.Any())
            {
                valid = valid.Where(t => supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).ToList();
                _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid technical type.", invalidTypeIds.Count, string.Join(", ", invalidTypeIds));
            }

            return valid;
        }

        private void RemoveIf(ref List<Technical> list, Func<Technical, bool> predicate, string errorMessage)
        {
            var invalid = list.Where(predicate).Select(t => t.WorksheetId).Distinct().ToList();
            if (!invalid.Any())
                return;

            list = list.Where(t => !invalid.Contains(t.WorksheetId)).ToList();
            _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] {Message}.", invalid.Count, string.Join(", ", invalid), errorMessage);
        }

        private async Task SyncAgentGroup<T>(List<Technical> tenantWorks,
            List<Agent> insList,
            Func<Technical, T> selector,
            Func<Technical, bool>? predicate = null,
            int tenantId =0,
            string definitionCode = "",
            CancellationToken cancellationToken = default) where T : class
        {
            predicate ??= (_ => true);
            var items = tenantWorks
                .Where(predicate)
                .Select(selector)
                .Where(a => a != null)
                .Distinct()
                .ToList();

            // Map anonymous objects to Agent
            var agents = items.Select(a =>
            {
                // Use reflection to map common properties Code/Name when selector returns anonymous type
                var codeProp = a.GetType().GetProperty("Code");
                var nameProp = a.GetType().GetProperty("Name");
                var code = codeProp?.GetValue(a)?.ToString();
                var name = nameProp?.GetValue(a)?.ToString();
                return new Agent { Code = code, Name = name, Name2 = name };
            })
                .Where(a => !string.IsNullOrWhiteSpace(a.Code))
                .GroupBy(a => a.Code)  // Group by Code
                .Select(g => new Agent
                {
                    Code = g.Key,
                    Name = g.First().Name,  // Take the first Name in the group
                    Name2 = g.First().Name
                })
                .ToList();

            if (!agents.Any()) return;

            var synced = await _service.SyncAgents(tenantId, definitionCode, agents, cancellationToken);
            insList.AddRange(synced);
        }

        private List<Agent> BuildPartnerList(
            IEnumerable<Technical> tenantWorks,
            IEnumerable<Agent> insuranceAgents,
            IEnumerable<Agent> contractsResult,
            IEnumerable<Lookup> partnershipTypesResult,
            Func<Technical, string> codeSelector,
            string lookupCode)
        {
            var lookupId = partnershipTypesResult
                .FirstOrDefault(p => p.Code == lookupCode)?.Id;

            return tenantWorks
                .Where(c => !string.IsNullOrWhiteSpace(codeSelector(c)))
                .GroupBy(tw => tw.ContractCode)
                .Select(g => g.OrderByDescending(c => c.PostingDate).First())
                .Select(c =>
                {
                    var code = codeSelector(c);

                    return new Agent
                    {
                        Code = "-",
                        Name = insuranceAgents.FirstOrDefault(ia => ia.Code == code)?.Name,
                        Agent1Id = contractsResult.FirstOrDefault(ct => ct.Code == c.ContractCode)?.Id,
                        Agent2Id = insuranceAgents.FirstOrDefault(ia => ia.Code == code)?.Id,
                        Lookup1Id = lookupId,
                    };
                })
                .ToList();
        }
    }
}