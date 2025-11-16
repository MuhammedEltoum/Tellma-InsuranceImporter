using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using Tellma.InsuranceImporter.Contract;
using Tellma.Utilities.EmailLogger;

namespace Tellma.InsuranceImporter
{
    public class TellmaInsuranceImporter
    {
        private readonly ILogger<TellmaInsuranceImporter> _logger;
        private readonly IImportService<Remittance> _remittanceService;
        private readonly IImportService<Technical> _technicalService;
        private readonly IImportService<Contract.ExchangeRate> _exchangeRateService;
        private readonly List<string> _tenantCodes;
        private readonly EmailLogger _emailLogger;

        public TellmaInsuranceImporter(ILogger<TellmaInsuranceImporter> logger,
            EmailLogger emailLogger,
            IImportService<Remittance> remittanceService,
            IImportService<Technical> technicalService,
            IImportService<Contract.ExchangeRate> exchangeRateService,
            IOptions<TellmaOptions> options)
        {
            _logger = logger;
            _emailLogger = emailLogger;
            _exchangeRateService = exchangeRateService;
            _remittanceService = remittanceService;
            _technicalService = technicalService;

            _tenantCodes = (options.Value.TenantCodes ?? "")
                .Split(",")
                .Select(s =>
                {
                    if (s.GetType() == typeof(string))
                        return s;
                    else if (string.IsNullOrWhiteSpace(s))
                        throw new ArgumentException($"Error reading TenantCodes config value, the TenantCodes list is empty or the service account is unable to see the secrets file..");
                    else
                        throw new ArgumentException($"Error reading TenantCodes config value, {s} is not a valid string.");
                })
                .ToList();
        }

        public async Task ImportToTellma(CancellationToken stoppingToken)
        {
            foreach (var tenantCode in _tenantCodes)
            {
                // Only process IR160 tenant for now
                if (tenantCode != "IR160") continue;

                var time = new Stopwatch();
                time.Start();
                _logger.LogInformation($"Starting remittance import for tenant {tenantCode}...at {DateTime.Now}");
                await _remittanceService.Import(tenantCode, stoppingToken);
                time.Stop();
                _logger.LogInformation($"remittance took {time.ElapsedMilliseconds / 1000} seconds!");

                time.Restart();
                _logger.LogInformation($"Starting technical import for tenant {tenantCode}...at {DateTime.Now}");
                await _technicalService.Import(tenantCode, stoppingToken);
                time.Stop();
                _logger.LogInformation($"technical took {time.ElapsedMilliseconds / 1000} seconds!");

                _logger.LogInformation($"Finished import for tenant {tenantCode} at {DateTime.Now}.");
                _emailLogger.SendActivityReport("Tellma insurance importer report");
            }
        }
    }
}