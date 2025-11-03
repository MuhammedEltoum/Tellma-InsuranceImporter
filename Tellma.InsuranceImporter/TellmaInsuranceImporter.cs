using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter
{
    public class TellmaInsuranceImporter
    {
        private readonly ILogger<TellmaInsuranceImporter> _logger;
        private readonly IImportService<Remittance> _remittanceService;
        private readonly IImportService<Technical> _technicalService;
        private readonly IImportService<Contract.ExchangeRate> _exchangeRateService;

        public TellmaInsuranceImporter(ILogger<TellmaInsuranceImporter> logger,
            IImportService<Remittance> remittanceService,
            IImportService<Technical> technicalService,
            IImportService<Contract.ExchangeRate> exchangeRateService,
            IOptions<TellmaOptions> options)
        {
            _logger = logger;
            _exchangeRateService = exchangeRateService;
            _remittanceService = remittanceService;
            _technicalService = technicalService;
        }

        public async Task ImportToTellma(CancellationToken stoppingToken)
        {
            var time = new Stopwatch();
            time.Start();
            //await _remittanceService.Import(stoppingToken);
            //time.Stop();
            //_logger.LogInformation($"remittance took {time.ElapsedMilliseconds / 1000} seconds!");
            //time.Restart();
            await _technicalService.Import(stoppingToken);
            time.Stop();
            _logger.LogInformation($"technical took {time.ElapsedMilliseconds / 1000} seconds!");
        }
    }
}