using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tellma.InsuranceImporter.Enums;
using Tellma.InsuranceImporter.Repository;
using Tellma.Model.Application;

namespace Tellma.InsuranceImporter
{
    public class ExchangeRatesService : IImportService<Contract.ExchangeRate>
    {
        private readonly ITellmaService _service;
        private readonly IExchangeRatesRepository _repository;
        private readonly ILogger<TellmaService> _logger;
        private readonly IOptions<TellmaOptions> _options;

        public ExchangeRatesService(IExchangeRatesRepository repository,
            ITellmaService tellmaService,
            ILogger<TellmaService> logger, 
            IOptions<TellmaOptions> options)
        {
            _service = tellmaService;
            _repository = repository;
            _logger = logger;
            _options = options;
        }

        public async Task Import(string tenantCode, CancellationToken cancellationToken)
        {
            var tenantId = InsuranceHelper.GetTenantId(tenantCode, _options.Value.Tenants);

            var tenantProfile = await _service.GetTenantProfile(tenantId, cancellationToken);

            // Get latest exchange rates from Tellma
            var startOfCurrentMonth = DateTime.Now.AddDays(1 - DateTime.Now.Day).ToString("yyyy-MM-dd");
            string filter = $"ValidAsOf >= '{startOfCurrentMonth}'";
            var exchangeObjectRatesResult = await _service
                .GetClientEntities(tenantId, TellmaClientProperty.ExchangeRates.AsString(), null, filter, cancellationToken);
            var exchangeRatesResult = exchangeObjectRatesResult
                .ConvertAll(o => (Tellma.Model.Application.ExchangeRate)o);
            
            // Log tenant info
            _logger.LogInformation("\n \n Processing tenant {TenantCode} (ID: {TenantId}, Name: {TenantName}) with {Count} exchange rates. \n \n",
                tenantCode, tenantId, tenantProfile.CompanyName, exchangeRatesResult.Count);

            var tellmaExchangeRates = exchangeRatesResult
                .Select(e => new ExchangeRateForSave
                {
                    Id = e.Id,
                    CurrencyId = e.CurrencyId,
                    ValidAsOf = e.ValidAsOf,
                    AmountInCurrency = e.AmountInCurrency,
                    AmountInFunctional = e.AmountInFunctional
                }).ToList();

            // Get latest exchange rates from SICS database
            var dbExchangeRates = await _repository.GetLatestExchangeRatesFromDB();
            var dbExchangeRatesList = dbExchangeRates
                .Select(e => new ExchangeRateForSave
                {
                    CurrencyId = e.CurrencyId,
                    ValidAsOf = e.ValidAsOf,
                    AmountInCurrency = e.AmountInCurrency,
                    AmountInFunctional = e.AmountInFunctional
                }).ToList();

            var tellmaExchangeRatesCompare = tellmaExchangeRates
                .Select(r => r.CurrencyId + '-' + r.ValidAsOf + '-' + r.AmountInCurrency + '-' + r.AmountInFunctional);

            //Exclude existing exchange rates in tellma from dbExchangeRates.
            var newRates = dbExchangeRatesList
                .Where(r => !tellmaExchangeRatesCompare.Contains(r.CurrencyId + '-' + r.ValidAsOf + '-' + r.AmountInCurrency + '-' + r.AmountInFunctional))
                .ToList();

            if (!newRates.Any())
            {
                _logger.LogInformation($"\n For tenant: {tenantId} Exchange rates are upto date! \n");
                return;
            }

            //Get ids from existing currencies on both tellma & SICS
            var tellmaExchangeRatesId = tellmaExchangeRates.ToDictionary(r => r.ValidAsOf + "-" + r.CurrencyId, r => r.Id);

            foreach (var rate in newRates)
            {
                if (tellmaExchangeRatesId.TryGetValue(rate.ValidAsOf + "-" + rate.CurrencyId, out int Id))
                {
                    rate.Id = Id;
                }
            }
            _logger.LogInformation($"For Tenant: {tenantId} Found {newRates.Where(r => r.Id == 0).Count()} new exchange rates, and {newRates.Where(r => r.Id > 0).Count()} edited exchange rates.");

            try
            {
                await _service.SaveExchangeRates(tenantId, newRates, cancellationToken);
                _logger.LogInformation($"For tenant: {tenantId} exchange rates are imported!");
            }
            catch (Exception ex)
            {
                _service.LogTellmaError(ex);
            }
        }
    }
}