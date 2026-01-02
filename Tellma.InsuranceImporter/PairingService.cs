using MailKit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Enums;
using Tellma.InsuranceImporter.Repository;
using Tellma.Model.Application;

namespace Tellma.InsuranceImporter
{
    public class PairingService : IImportService<Pairing>
    {
        private readonly ITellmaService _service;
        private readonly IWorksheetRepository<Pairing> _pairingRepository;
        private readonly ILogger<PairingService> _logger;
        private readonly IOptions<TellmaOptions> _tellmaOptions;
        private readonly IOptionsMonitor<InsuranceOptions> _insuranceOptions;

        public PairingService(ITellmaService tellmaService,
                              IWorksheetRepository<Pairing> pairingRepository,
                              ILogger<PairingService> logger,
                              IOptionsMonitor<InsuranceOptions> insuranceOptions,
                              IOptions<TellmaOptions> tellmaOptions)
        {
            _service = tellmaService;
            _pairingRepository = pairingRepository;
            _logger = logger;
            _insuranceOptions = insuranceOptions;
            _tellmaOptions = tellmaOptions;
        }

        public async Task Import(string tenantCode, CancellationToken cancellationToken)
        {
            try
            {
                string pairingFilter = $"p.[IMPORT_DATE] IS NULL " +
                    $"AND t.[IMPORT_DATE] IS NOT NULL AND r.[IMPORT_DATE] IS NOT NULL " +
                    $"AND t.[TENANT_CODE] = '{tenantCode}' AND r.[TENANT_CODE] = '{tenantCode}' " +
                    $"AND ([TENANT_CODE1] = '{tenantCode}' OR [TENANT_CODE2] = '{tenantCode}') " +
                    $"AND (tmt.CanBePairing = 1 OR tmt.[B Account] is null)";

                var allWorksheets = await _pairingRepository.GetWorksheets(pairingFilter, cancellationToken);

                var excludedWorksheetsFilter = $"p.[IMPORT_DATE] IS NULL " +
                    $"AND (t.[IMPORT_DATE] IS NULL OR r.[IMPORT_DATE] IS NULL) " + // This will get pairing worksheet that have unimported remittance or technicals.
                    $"AND t.[TENANT_CODE] = '{tenantCode}' AND r.[TENANT_CODE] = '{tenantCode}' " +
                    $"AND ([TENANT_CODE1] = '{tenantCode}' OR [TENANT_CODE2] = '{tenantCode}') " +
                    $"AND (tmt.CanBePairing = 1 OR tmt.[B Account] is null)";

                var badWorksheets = (await _pairingRepository.GetWorksheets(excludedWorksheetsFilter, cancellationToken))
                                    .Select(bad => $"PK: {bad.Pk} has nonimported Remit: {bad.RemitWorksheet} or Tech: {bad.TechWorksheet}")
                                    .Distinct()
                                    .ToList();

                if (badWorksheets.Any())
                _logger.LogWarning("Import Warning: ({Count}) Pairing worksheets will not be imported! \n {BadWorsheets}", badWorksheets.Count, String.Join(" \n ", badWorksheets));


                if (!allWorksheets.Any())
                {
                    _logger.LogInformation("Sucess: Pairing is up to date for tenant {TenantCode}", tenantCode);
                    return;
                }

                await ProcessTenant(tenantCode, allWorksheets, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError("Processing tenant {TenantCode} Failed for the following reason: \n {Error}", tenantCode, ex.ToString());
                throw;
            }
        }

        private async Task ProcessTenant(string tenantCode, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            var tenantId = InsuranceHelper.GetTenantId(tenantCode, _tellmaOptions.Value.Tenants);
            var tenantProfile = await _service.GetTenantProfile(tenantId, cancellationToken);

            _logger.LogInformation("\nProcessing tenant {TenantCode} (ID: {TenantId}) with {Count} pairing worksheets\n",
                tenantCode, tenantId, tenantWorks.Count);

            tenantWorks = ValidateWorksheets(tenantWorks).ToList();

            // 1. Get tenant configuration
            var functionalCurrency = tenantProfile.FunctionalCurrencyId;

            // Remove pairings before freeze/archive dates
            RemoveIf(ref tenantWorks, r => r.TellmaDocumentId > 0 && r.PairingDate <= tenantProfile.ArchiveDate,
                "have a pairing date on or before the archive date for existing pairings");
            RemoveIf(ref tenantWorks, r => r.PairingDate <= tenantProfile.FreezeDate,
                "have a pairing date on or before the freeze date for new pairings");

            if (!tenantWorks.Any())
            {
                _logger.LogWarning("No valid pairing records after date filtering for tenant {Tenant}", tenantCode);
                return;
            }

            // 2. Get required Tellma entities
            var currencies = await GetCurrencies(tenantId, tenantWorks, cancellationToken);
            var insuranceAgents = await GetInsuranceAgents(tenantId, tenantWorks, cancellationToken);
            var contracts = await GetInsuranceContracts(tenantId, tenantWorks, cancellationToken);
            var customerAccounts = await GetCustomerAccounts(tenantId, tenantWorks, cancellationToken);
            var exchangeRates = await GetExchangeRates(tenantId, tenantWorks, functionalCurrency, cancellationToken);
            var accounts = await GetAccounts(tenantId, tenantWorks, cancellationToken);

            RemoveIf(ref tenantWorks,
                w => !currencies.Select(c => c.Id).Contains(w.TechCurrency) || !currencies.Select(c => c.Id).Contains(w.RemitCurrency),
                $"have currencies {string.Join(", ", tenantWorks.Select(w => w.TechCurrency).Union(tenantWorks.Select(w => w.RemitCurrency)).Except(currencies.Select(c => c.Id)))} not found in Tellma.");

            // Remove pairings with zero contract and sum values.
            tenantWorks = tenantWorks
                .Where(w => Math.Abs(w.SumMonetaryValue) > 0 && Math.Abs(w.SumValue) > 0)
                .ToList();

            // 3. Get definition IDs
            var definitions = await GetDefinitionIds(tenantId, cancellationToken);

            // 4. Create documents
            var pairingDocuments = await CreatePairingDocuments(
                tenantWorks, definitions, accounts, contracts,
                customerAccounts, insuranceAgents, exchangeRates,
                functionalCurrency, cancellationToken);

            // 5. Save to Tellma
            try
            {
                if (pairingDocuments.Any())
                {
                    var docResult = await _service.SaveDocuments(tenantId, definitions.PairingDocDefinitionId, pairingDocuments, cancellationToken);

                    var pairingRecords = docResult.Select(p => new Pairing
                    {
                        Pk = (int)p.SerialNumber,
                        TenantCode1 = tenantCode,
                        TellmaDocumentId = p.Id
                    }).ToList();

                    await _pairingRepository.UpdateDocumentIds(tenantCode, pairingRecords, cancellationToken);

                    var documentIds = pairingRecords.Select(r => (int)r.TellmaDocumentId).ToList();
                    await _service.CloseDocuments(tenantId, definitions.PairingDocDefinitionId, documentIds, cancellationToken);
                    await _pairingRepository.UpdateImportedWorksheets(tenantCode, pairingRecords, cancellationToken);

                    _logger.LogInformation("Successfully imported {Count} pairing documents for tenant {Tenant}",
                        pairingDocuments.Count, tenantCode);
                }
                else
                {
                    _logger.LogWarning("No pairing documents to import for tenant {Tenant}", tenantCode);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error saving pairing documents for tenant {Tenant}: {Error}",
                    tenantCode, ex.ToString());
                throw;
            }
        }

        private async Task<List<Currency>> GetCurrencies(int tenantId, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            var currenciesObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Currencies.AsString(), token: cancellationToken);
            var currencies = currenciesObjectResult.ConvertAll(c => (Currency)c);

            // Remove pairings with currencies not in Tellma
            var validCurrencies = currencies.Select(c => c.Id).ToList();
            var invalidCurrencies = tenantWorks
                .Where(w => !validCurrencies.Contains(w.TechCurrency) || !validCurrencies.Contains(w.RemitCurrency))
                .Select(w => new { w.TechCurrency, w.RemitCurrency })
                .Distinct()
                .ToList();

            return currencies;
        }

        private async Task<List<Agent>> GetInsuranceAgents(int tenantId, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            int insuranceAgentDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.AgentDefinitions.AsString(),
                TellmaEntityCode.InsuranceAgent.AsString(),
                token: cancellationToken);

            var insuranceAgentCodes = tenantWorks
                .SelectMany(t => new[] { t.AgentCode1, t.AgentCode2, t.RemitInsuranceAgent })
                .Where(c => !string.IsNullOrEmpty(c))
                .Distinct()
                .ToList();

            string batchFilter = insuranceAgentCodes.Any() ?
                string.Join(" OR ", insuranceAgentCodes.Select(c => $"Code='{c}'")) : null;
            
            batchFilter = batchFilter?.Length < 1024 ? batchFilter : null;

            var agentsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(),
                insuranceAgentDefinitionId, batchFilter, token: cancellationToken);

            var existingAgents = agentsObjectResult.ConvertAll(agent => (Agent)agent);

            // Create missing agents
            var missingAgentCodes = insuranceAgentCodes.Except(existingAgents.Select(a => a.Code)).ToList();
            var newAgents = missingAgentCodes.Select(code => new Agent
            {
                Code = code,
                Name = code // Use code as name if not available
            }).ToList();

            var syncedAgents = await _service.SyncAgents(tenantId, TellmaEntityCode.InsuranceAgent.AsString(), newAgents, cancellationToken);

            return existingAgents.Concat(syncedAgents).ToList();
        }

        private async Task<List<Agent>> GetInsuranceContracts(int tenantId, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            int contractDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.AgentDefinitions.AsString(),
                TellmaEntityCode.InsuranceContract.AsString(),
                token: cancellationToken);

            var contractCodes = tenantWorks
                .Select(t => t.ContractCode)
                .Where(c => !string.IsNullOrEmpty(c))
                .Distinct()
                .ToList();

            if (!contractCodes.Any())
                return new List<Agent>();

            string batchFilter = string.Join(" OR ", contractCodes.Select(c => $"Code='{c}'"));
            batchFilter = batchFilter?.Length < 1024 ? batchFilter : null;

            var agentsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(),
                contractDefinitionId, batchFilter, token: cancellationToken);

            var contracts = agentsObjectResult.ConvertAll(agent => (Agent)agent);

            // Remove pairings with missing contracts
            var missingContracts = contractCodes.Except(contracts.Select(a => a.Code)).ToList();
            if (missingContracts.Any())
            {
                RemoveIf(ref tenantWorks, t => missingContracts.Contains(t.ContractCode),
                    $"have missing insurance contracts[{string.Join(", ", missingContracts)}].");
            }

            return contracts;
        }

        private async Task<List<Agent>> GetCustomerAccounts(int tenantId, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            int customerAccDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.AgentDefinitions.AsString(),
                TellmaEntityCode.TradeReceivableAccount.AsString(),
                token: cancellationToken);

            var customerAccCodes = tenantWorks
                .Where(c => !string.IsNullOrEmpty(c.ContractCode) &&
                           !string.IsNullOrEmpty(c.BusinessMainClassCode) &&
                           !string.IsNullOrEmpty(c.TechInsuranceAgent))
                .Select(p => $"{p.ContractCode}-{p.BusinessMainClassCode}-{p.TechInsuranceAgent}")
                .Distinct()
                .ToList();

            if (!customerAccCodes.Any())
                return new List<Agent>();

            string batchFilter = string.Join(" OR ", customerAccCodes.Select(t => $"Code='{t}'"));
            batchFilter = batchFilter?.Length < 1024 ? batchFilter : null;

            var agentsObjectResult = await _service.GetClientEntities(tenantId, TellmaClientProperty.Agents.AsString(),
                customerAccDefinitionId, batchFilter, token: cancellationToken);

            var customerAccounts = agentsObjectResult.ConvertAll(agent => (Agent)agent);

            // Remove pairings with missing customer accounts
            var missingAccounts = customerAccCodes.Except(customerAccounts.Select(a => a.Code)).ToList();
            if (missingAccounts.Any())
            {
                RemoveIf(ref tenantWorks, t => missingAccounts.Contains(
                    $"{t.ContractCode}-{t.BusinessMainClassCode}-{t.TechInsuranceAgent}"),
                    "have missing customer accounts.");
            }

            return customerAccounts;
        }

        private async Task<List<Tellma.Model.Application.ExchangeRate>> GetExchangeRates(int tenantId, List<Pairing> tenantWorks,
            string functionalCurrency, CancellationToken cancellationToken)
        {
            if (!tenantWorks.Any())
                return new List<Tellma.Model.Application.ExchangeRate>();

            var minPaymentDate = tenantWorks.Min(t => t.RemittancePaymentDate);
            minPaymentDate = minPaymentDate.AddDays(1 - minPaymentDate.Day);
            var maxPaymentDate = tenantWorks.Max(t => t.RemittancePaymentDate);
            maxPaymentDate = maxPaymentDate.AddDays(1 - maxPaymentDate.Day);

            var currencyCodes = tenantWorks
                .SelectMany(t => new[] { t.TechCurrency, t.RemitCurrency, t.ContractCurrencyId })
                .Where(c => !string.IsNullOrEmpty(c) && !c.Equals(functionalCurrency, StringComparison.OrdinalIgnoreCase))
                .Distinct()
                .ToList();

            if (!currencyCodes.Any())
                return new List<Tellma.Model.Application.ExchangeRate>();

            var currencyFilter = $"({string.Join(" OR ", currencyCodes.Select(c => $"CurrencyId='{c}'"))})";
            var exchangeRatesFilter = $"ValidAsOf >= '{minPaymentDate:yyyy-MM-dd}' AND ValidAsOf <= '{maxPaymentDate:yyyy-MM-dd}'";

            exchangeRatesFilter = currencyFilter.Length + 55 < 1024 ? $"{exchangeRatesFilter} AND {currencyFilter}" : exchangeRatesFilter;
            var exchangeRatesObjectResult = await _service.GetClientEntities(tenantId,
                TellmaClientProperty.ExchangeRates.AsString(),
                filter: exchangeRatesFilter,
                token: cancellationToken);

            return exchangeRatesObjectResult.ConvertAll(er => (Tellma.Model.Application.ExchangeRate)er);
        }

        private async Task<List<Account>> GetAccounts(int tenantId, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            var accountCodes = tenantWorks
                .Select(w => w.AccountCode)
                .Append("16002") // Unallocated Insurance receivables
                .Append("4400050") // Net foreign exchange gain
                .Append("5212018") // Net foreign exchange loss
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct()
                .ToList();

            string accountsFilter = accountCodes.Any() ?
                string.Join(" OR ", accountCodes.Select(a => $"Code = '{a}'")) : null;

            var accountsObjectResult = await _service.GetClientEntities(tenantId,
                TellmaClientProperty.Accounts.AsString(),
                filter: accountsFilter,
                token: cancellationToken);

            return accountsObjectResult.ConvertAll(a => (Account)a);
        }

        private async Task<DocumentDefinitions> GetDefinitionIds(int tenantId, CancellationToken cancellationToken)
        {
            var definitions = new DocumentDefinitions
            {
                LineDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.LineDefinitions.AsString(),
                    TellmaEntityCode.ManualLine.AsString(),
                    token: cancellationToken),

                PairingDocDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.DocumentDefinitions.AsString(),
                    TellmaEntityCode.PairingWorksheet.AsString(),
                    token: cancellationToken),

                TaxDepartmentDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.AgentDefinitions.AsString(),
                    TellmaEntityCode.TaxDepartment.AsString(),
                    token: cancellationToken),

                InwardOutwardDefinitionId = await _service.GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.LookupDefinitions.AsString(),
                    TellmaEntityCode.TechnicalInOutward.AsString(),
                    token: cancellationToken),

                OtherGainsLossesId = await _service.GetIdByCodeAsync(tenantId,
                    TellmaClientProperty.EntryTypes.AsString(),
                    "OtherGainsLosses",
                    token: cancellationToken)
            };

            definitions.VatDeptId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.Agents.AsString(),
                TellmaEntityCode.ValueAddedTax.AsString(),
                definitions.TaxDepartmentDefinitionId,
                token: cancellationToken);

            definitions.OperationCenterId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.Centers.AsString(),
                TellmaEntityCode.OperationCenter.AsString(),
                token: cancellationToken);

            definitions.InwardLookupId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.Lookups.AsString(),
                TellmaEntityCode.Inward.AsString(),
                definitions.InwardOutwardDefinitionId,
                token: cancellationToken);

            definitions.OutwardLookupId = await _service.GetIdByCodeAsync(tenantId,
                TellmaClientProperty.Lookups.AsString(),
                TellmaEntityCode.Outward.AsString(),
                definitions.InwardOutwardDefinitionId,
                token: cancellationToken);

            return definitions;
        }

        private async Task<List<DocumentForSave>> CreatePairingDocuments(
            List<Pairing> tenantWorks,
            DocumentDefinitions definitions,
            List<Account> accounts,
            List<Agent> contracts,
            List<Agent> customerAccounts,
            List<Agent> insuranceAgents,
            List<Tellma.Model.Application.ExchangeRate> exchangeRates,
            string functionalCurrency,
            CancellationToken cancellationToken)
        {
            var pairingDocuments = new List<DocumentForSave>();
            var pairingGroups = tenantWorks.GroupBy(p => p.Pk);

            int remittanceAccId = accounts.FirstOrDefault(a => a.Code == "16002")?.Id ?? 0;
            int gainAccId = accounts.FirstOrDefault(a => a.Code == "4400050")?.Id ?? 0;
            int lossAccId = accounts.FirstOrDefault(a => a.Code == "5212018")?.Id ?? 0;

            if (remittanceAccId == 0 || gainAccId == 0 || lossAccId == 0)
            {
                _logger.LogError("Required accounts not found in Tellma");
                return pairingDocuments;
            }

            foreach (var pairingGroup in pairingGroups)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var firstPairing = pairingGroup.First();

                var originalTechSign = pairingGroup.Sum(g => g.SumValue) > 0 ? 1 : -1;

                // Determine pairing type
                bool isNormalPairing = firstPairing.RemitWsId.StartsWith("RW");
                bool isReversePairing = firstPairing.TechWsId.StartsWith("RW");

                if (!isNormalPairing && !isReversePairing)
                {
                    _logger.LogWarning("Invalid pairing type for PK {Pk}", firstPairing.Pk);
                    continue;
                }

                // Get pairing amounts
                var (remittanceAmount, remittanceCurrency, remittanceAgentCode) = isNormalPairing ?
                    (firstPairing.RemitAmount, firstPairing.RemitCurrency, firstPairing.RemitInsuranceAgent) :
                    (firstPairing.TechAmount, firstPairing.TechCurrency, firstPairing.TechInsuranceAgent);

                // Get exchange rate
                decimal exchangeRate = 1;
                if (remittanceCurrency != functionalCurrency)
                {
                    var rate = exchangeRates
                        .Where(er => er.CurrencyId == remittanceCurrency &&
                                     er.ValidAsOf <= firstPairing.RemittancePaymentDate)
                        .OrderByDescending(er => er.ValidAsOf)
                        .FirstOrDefault();

                    if (rate == null)
                    {
                        _logger.LogError("No exchange rate found for currency {Currency} on or before {Date} for PK {Pk}",
                            remittanceCurrency, firstPairing.RemittancePaymentDate.ToString("yyyy-MM-dd"), firstPairing.Pk);
                        continue;
                    }

                    exchangeRate = (decimal)rate.Rate;
                }

                // Calculate total technical amounts from all lines
                decimal totalMonetarySum = pairingGroup.Sum(p => p.SumMonetaryValue);
                decimal totalValueSum = pairingGroup.Sum(p => p.SumValue);

                // Remove if total sums are zero
                if (Math.Abs(totalMonetarySum) == 0 || Math.Abs(totalValueSum) == 0)
                {
                    _logger.LogError("Total monetary or value sum is zero for PK {Pk}, Remittance {Remittance}, Technical {Technical}", firstPairing.Pk, firstPairing.RemitWorksheet, firstPairing.TechWorksheet);
                    continue;
                }

                // Calculate scaling factor if needed
                decimal scalingFactor = 1;
                decimal technicalAmountInPairing = isNormalPairing ? firstPairing.TechAmount : firstPairing.RemitAmount;

                if (Math.Abs(totalMonetarySum) > 0 &&
                    Math.Abs(Math.Abs(technicalAmountInPairing) - Math.Abs(totalMonetarySum)) > 0)
                {
                    scalingFactor = Math.Abs(technicalAmountInPairing) / Math.Abs(totalMonetarySum);
                    _logger.LogDebug("PK {Pk}: Applying scaling factor {Factor}", firstPairing.Pk, scalingFactor);
                }

                // Create entries
                var entries = new List<EntryForSave>();

                // 1. Remittance entry
                var remittanceAgent = insuranceAgents.FirstOrDefault(a => a.Code == remittanceAgentCode);
                if (remittanceAgent == null)
                {
                    _logger.LogError("Remittance agent {AgentCode} not found for PK {Pk}", remittanceAgentCode, firstPairing.Pk);
                    continue;
                }

                entries.Add(new EntryForSave
                {
                    AccountId = remittanceAccId,
                    AgentId = remittanceAgent.Id,
                    CenterId = definitions.OperationCenterId,
                    NotedDate = firstPairing.RemittancePaymentDate,
                    CurrencyId = remittanceCurrency,
                    Direction = remittanceAmount > 0 ? (short)-1 : (short)1, // Opposite sign to technical
                    MonetaryValue = Math.Abs(Math.Round(remittanceAmount, 2)),
                    Value = Math.Round(Math.Abs(remittanceAmount * exchangeRate), 2)
                });

                // 2. Technical entries
                decimal totalTechnicalValue = 0;

                foreach (var pairingLine in pairingGroup)
                {
                    // Calculate line amounts
                    decimal lineMonetaryValue = Math.Abs(Math.Round(pairingLine.SumMonetaryValue * scalingFactor, 2));
                    decimal lineValue = Math.Abs(Math.Round(pairingLine.SumValue * scalingFactor, 2));

                    // Calculate direction
                    var techAmount = isNormalPairing ? pairingLine.TechAmount : pairingLine.RemitAmount;
                    short direction = GetDirection(techAmount > 0 ? 1 : -1,
                        originalTechSign,
                        pairingLine.TechDirection);

                    // throw error if direction is 0
                    if (direction == 0)
                    {
                        _logger.LogError("Invalid direction calculation for PK {Pk}", firstPairing.Pk);
                        throw new Exception($"Invalid direction calculation for PK {firstPairing.Pk}");
                    }

                    // Get customer account
                    var customerAccCode = $"{pairingLine.ContractCode}-{pairingLine.BusinessMainClassCode}-{pairingLine.TechInsuranceAgent}";
                    var customerAcc = customerAccounts.FirstOrDefault(a => a.Code == customerAccCode);

                    if (customerAcc == null)
                    {
                        _logger.LogError("Customer account {AccountCode} not found for PK {Pk}", customerAccCode, firstPairing.Pk);
                        continue;
                    }

                    // Get contract
                    var contract = contracts.FirstOrDefault(c => c.Code == pairingLine.ContractCode);

                    // Get account
                    var account = accounts.FirstOrDefault(a => a.Code == pairingLine.AccountCode);
                    if (account == null)
                    {
                        _logger.LogError("Account {AccountCode} not found for PK {Pk}", pairingLine.AccountCode, firstPairing.Pk);
                        continue;
                    }

                    entries.Add(new EntryForSave
                    {
                        AccountId = account.Id,
                        AgentId = !pairingLine.BTaxAccount ? customerAcc.Id : definitions.VatDeptId,
                        NotedAgentId = pairingLine.BTaxAccount ? customerAcc.Id : null,
                        CenterId = definitions.OperationCenterId,
                        CurrencyId = pairingLine.ContractCurrencyId,
                        Direction = direction,
                        MonetaryValue = Math.Round(lineMonetaryValue, 2),
                        Value = Math.Round(lineValue, 2),
                        Time1 = pairingLine.EffectiveDate,
                        Time2 = pairingLine.ExpiryDate,
                        NotedDate = pairingLine.TechNotedDate
                    });

                    totalTechnicalValue += direction > 0 ? lineValue : -lineValue;
                }

                // 3. Exchange rate difference entry if needed
                decimal remittanceEntryValue = entries[0].Direction > 0 ?
                    entries[0].Value ?? 0 : -(entries[0].Value ?? 0);

                remittanceEntryValue = Math.Round(remittanceEntryValue, 2);

                decimal balanceDifference = Math.Round(totalTechnicalValue + remittanceEntryValue, 2);

                var totalOffBalance = Math.Abs((decimal)entries.Sum(e => e.Value * e.Direction)) - Math.Abs(balanceDifference);

                if (Math.Abs(balanceDifference) > 0.00m)
                {
                    // Determine which currency is not usd
                    var nonUsdCurrency = firstPairing.RemitCurrency != "USD" ? firstPairing.RemitCurrency : firstPairing.TechCurrency;
                    exchangeRate = nonUsdCurrency == "USD" ? 1 : exchangeRates
                            .Where(er => er.CurrencyId == nonUsdCurrency &&
                                         er.ValidAsOf <= firstPairing.PairingDate)
                            .OrderByDescending(er => er.ValidAsOf)
                            .FirstOrDefault()?.Rate ?? 1;

                    decimal adjustmentValue = Math.Round(Math.Abs(balanceDifference) + totalOffBalance, 2);
                    decimal adjustmentMonetaryValue = Math.Round(adjustmentValue / exchangeRate, 2);

                    // check if exchange rate is greater than 3% to log a warning
                    if (adjustmentValue / totalTechnicalValue > 0.03m)
                        _logger.LogWarning("High exchange rate difference of {Difference:P2} for PK {Pk}", adjustmentValue / totalTechnicalValue, firstPairing.Pk);

                    entries.Add(new EntryForSave
                    {
                        AccountId = balanceDifference < 0 ? lossAccId : gainAccId,
                        EntryTypeId = definitions.OtherGainsLossesId,
                        CenterId = definitions.OperationCenterId,
                        CurrencyId = nonUsdCurrency,
                        Direction = balanceDifference < 0 ? (short)1 : (short)-1,
                        MonetaryValue = adjustmentMonetaryValue,
                        Value = adjustmentValue
                    });
                }

                // Create fixed date for 2025/05/16
                DateTime previousTransactionsDate = DateTime.ParseExact(_insuranceOptions.CurrentValue.PrvsPairingTransactionsDate ?? "2025/05/16", "yyyy-MM-dd", CultureInfo.InvariantCulture);

                // Create document
                pairingDocuments.Add(new DocumentForSave
                {
                    Id = (int)firstPairing.TellmaDocumentId,
                    SerialNumber = firstPairing.Pk,
                    Lookup1Id = firstPairing.TechIsInward ? definitions.InwardLookupId : definitions.OutwardLookupId,
                    PostingDate = firstPairing.PairingDate >= previousTransactionsDate ? firstPairing.PairingDate : firstPairing.RemittancePaymentDate,
                    Memo = $"Pairing {firstPairing.TechWorksheet} and {firstPairing.RemitWorksheet}, Remit orginal sign = {(remittanceAmount > 0 ? "Receipt" : "Reverse")}, Pairing type = {(isNormalPairing ? "Normal" : "Reverse")}",
                    Lines = new List<LineForSave>
                    {
                        new LineForSave
                        {
                            DefinitionId = definitions.LineDefinitionId,
                            Entries = entries
                        }
                    }
                });
            }

            return pairingDocuments;
        }

        private IEnumerable<Pairing> ValidateWorksheets(List<Pairing> allWorksheets)
        {
            var valid = allWorksheets.ToList();

            // Basic validations
            RemoveIf(ref valid, w => w.TenantCode1 != w.TenantCode2, "Tenant codes are not the same!");

            string[] validTenants = ["IR1", "IR160"];
            RemoveIf(ref valid, w => !validTenants.Contains(w.TenantCode1) || !validTenants.Contains(w.TenantCode2),
                "have invalid tenant codes.");

            RemoveIf(ref valid, w => string.IsNullOrEmpty(w.ContractCode),
                "have null technical contract codes.");

            // Cannot have same type on both sides
            RemoveIf(ref valid, w =>
                (w.RemitWorksheet.StartsWith("RW") && w.TechWorksheet.StartsWith("RW")) ||
                (w.RemitWorksheet.StartsWith("TW") && w.TechWorksheet.StartsWith("TW")) ||
                (w.RemitWorksheet.StartsWith("CW") && w.TechWorksheet.StartsWith("CW")) ||
                (w.RemitWorksheet.StartsWith("CW") && w.TechWorksheet.StartsWith("TW")) || // Might remove in the future.
                (w.RemitWorksheet.StartsWith("TW") && w.TechWorksheet.StartsWith("CW")),
                "have same worksheet type on both sides.");

            // Worksheet IDs cannot be null or empty
            RemoveIf(ref valid, w => string.IsNullOrEmpty(w.RemitWsId) || string.IsNullOrEmpty(w.TechWsId),
                "have null or empty worksheet IDs.");

            // Remove zero amounts in technical
            valid = valid.Where(w => Math.Abs(w.SumMonetaryValue) > 0 && Math.Abs(w.SumValue) > 0).ToList();

            // Tech and Remit direction must be either 1 or -1
            RemoveIf(ref valid, w => Math.Abs(w.TechDirection) != 1 && Math.Abs(w.TechDirection) != -1,
                "have invalid technical direction.");

            // Validate worksheet types
            List<string> supportedWorksheets = _insuranceOptions.CurrentValue.PairingSupportedPrefixes
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList() ?? ["RW", "TW", "CW"];

            RemoveIf(ref valid, w =>
                !supportedWorksheets.Any(p => w.RemitWorksheet.StartsWith(p)) ||
                !supportedWorksheets.Any(p => w.TechWorksheet.StartsWith(p)),
                "have unsupported worksheet types.");

            RemoveIf(ref valid, w => w.RemittancePaymentDate == DateTime.MinValue, "have null remittance payment date.");

            // Validate amounts
            RemoveIf(ref valid, w => w.TechCurrency == w.RemitCurrency &&
                w.TechAmount + w.RemitAmount != 0,
                "have same currency but different amounts on both sides.");

            // Remittance amount cannot be zero and Technical amount cannot be zero
            RemoveIf(ref valid, w => w.RemitAmount == 0, "have zero remittance amount.");
            RemoveIf(ref valid, w => w.TechAmount == 0, "have zero technical amount.");



            return valid;
        }

        private void RemoveIf(ref List<Pairing> list, Func<Pairing, bool> predicate, string errorMessage)
        {
            var invalid = list.Where(predicate).Select(t => t.Pk).Distinct().ToList();

            if (!invalid.Any())
                return;

            list = list.Where(t => !invalid.Contains(t.Pk)).ToList();
            _logger.LogError("Validation Error: ({Count}) Pairing PK [{Ids}] {Message}.",
                invalid.Count, string.Join(", ", invalid), errorMessage);
        }

        public short GetDirection(int pairingTechSign, int originalTechSign, int direction)
        {
            if (pairingTechSign == -1)
            {
                if (originalTechSign > 0 && direction == -1) return -1;
                if (originalTechSign > 0 && direction == 1) return 1;
                if (originalTechSign < 0 && direction == 1) return -1;
                if (originalTechSign < 0 && direction == -1) return 1;
            }
            else
            {
                if (originalTechSign > 0 && direction == 1) return -1;
                if (originalTechSign > 0 && direction == -1) return 1;
                if (originalTechSign < 0 && direction == -1) return -1;
                if (originalTechSign < 0 && direction == 1) return 1;
            }

            throw new InvalidOperationException("No matching condition found for adjustment direction");
        }
    }
}