using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using Tellma.InsuranceImporter.Contract;
using Tellma.Utilities.EmailLogger;

namespace Tellma.InsuranceImporter
{
    public class TellmaInsuranceImporter
    {
        private readonly IOptions<TellmaOptions> _options;
        private readonly IOptionsMonitor<InsuranceOptions> _insuranceOptions;
        private readonly ILogger<TellmaInsuranceImporter> _logger;
        private readonly IImportService<Remittance> _remittanceService;
        private readonly IImportService<Technical> _technicalService;
        private readonly IImportService<Pairing> _pairingService;
        private readonly IImportService<Contract.ExchangeRate> _exchangeRateService;
        private readonly EmailLogger _emailLogger;
        private readonly List<string> _tenantCodes;

        public TellmaInsuranceImporter(
            ILogger<TellmaInsuranceImporter> logger,
            EmailLogger emailLogger,
            IImportService<Remittance> remittanceService,
            IImportService<Technical> technicalService,
            IImportService<Pairing> pairingService,
            IImportService<Contract.ExchangeRate> exchangeRateService,
            IOptions<TellmaOptions> options,
            IOptionsMonitor<InsuranceOptions> insuranceOptions)
        {
            _logger = logger;
            _emailLogger = emailLogger;
            _remittanceService = remittanceService;
            _technicalService = technicalService;
            _pairingService = pairingService;
            _exchangeRateService = exchangeRateService;
            _options = options;
            _insuranceOptions = insuranceOptions;

            // Simplified Parsing logic
            var rawCodes = options.Value.TenantCodes;

            if (string.IsNullOrWhiteSpace(rawCodes))
            {
                throw new ArgumentException("Error reading TenantCodes config value: The list is empty or secrets are missing.");
            }

            _tenantCodes = rawCodes
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();
        }

        public async Task ImportToTellma(CancellationToken stoppingToken)
        {
            foreach (var tenantCode in _tenantCodes)
            {
                try
                {
                    _logger.LogInformation("Starting full import process for tenant {TenantCode}", tenantCode);

                    await RunImportStepAsync(_insuranceOptions.CurrentValue.EnableExchangeRate, _exchangeRateService, tenantCode, "Exchange Rates", stoppingToken);
                    await RunImportStepAsync(_insuranceOptions.CurrentValue.EnableRemittance, _remittanceService, tenantCode, "Remittances", stoppingToken);
                    await RunImportStepAsync(_insuranceOptions.CurrentValue.EnableTechnical, _technicalService, tenantCode, "Technical Data", stoppingToken);
                    await RunImportStepAsync(_insuranceOptions.CurrentValue.EnablePairing, _pairingService, tenantCode, "Pairing", stoppingToken);

                    _logger.LogInformation("Finished import for tenant {TenantCode}", tenantCode);

                    _emailLogger.SendActivityReport($"Tellma insurance importer report for {tenantCode}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Critical error importing data for tenant {TenantCode}. Terminating process.", tenantCode);
                    throw;
                }
            }
        }

        private async Task RunImportStepAsync<T>(
            bool isEnabled,
            IImportService<T> service,
            string tenantCode,
            string stepName,
            CancellationToken token) where T : class
        {
            if (!isEnabled)
            {
                _logger.LogInformation("{StepName} import is disabled. Skipping for tenant {TenantCode}.", stepName, tenantCode);
                return;
            }
            var stopwatch = Stopwatch.StartNew();

            _logger.LogInformation("Starting {StepName} import for tenant {TenantCode}...", stepName, tenantCode);

            await service.Import(tenantCode, token);

            stopwatch.Stop();
            _logger.LogInformation("{StepName} took {ElapsedSeconds} seconds.", stepName, stopwatch.Elapsed.TotalSeconds);
        }
    }
}