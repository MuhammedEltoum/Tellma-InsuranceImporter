using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tellma.Api.Dto;
using Tellma.Client;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Enums;
using Tellma.InsuranceImporter.Repository;
using Tellma.Model.Application;

namespace Tellma.InsuranceImporter
{
    public class ExchangeRatesService : IImportService<Contract.ExchangeRate>
    {
        private readonly ITellmaService _service;
        private readonly IExchangeRatesRepository _repository;
        private readonly IEnumerable<int> tenantIds;
        private readonly ILogger<TellmaService> _logger;

        public ExchangeRatesService(IExchangeRatesRepository repository,
            ILogger<TellmaService> logger, 
            IOptions<TellmaOptions> options)
        {
            _service = new TellmaService(logger, options);
            _repository = repository;
            _logger = logger;

            tenantIds = (options.Value.TenantIds ?? "")
                .Split(",")
                .Select(s =>
                {
                    if (int.TryParse(s, out int result))
                        return result;
                    else if (string.IsNullOrWhiteSpace(s))
                        throw new ArgumentException($"Error parsing TenantIds config value, the TenantIds list is empty or the service account is unable to see the secrets file..");
                    else
                        throw new ArgumentException($"Error parsing TenantIds config value, {s} is not a valid integer.");
                })
                .ToList();
        }

        public async Task Import(CancellationToken cancellationToken)
        {
            foreach (var tenantId in tenantIds)
            {
                //start of validation
                var startOfCurrentMonth = DateTime.Now.AddDays(1 - DateTime.Now.Day).ToString("yyyy-MM-dd");
                string filter = $"ValidAsOf >= '{startOfCurrentMonth}'";
                var exchangeObjectRatesResult = await _service
                    .GetClientEntities(tenantId, TellmaClientProperty.ExchangeRates.AsString(), null, filter, cancellationToken);
                var exchangeRatesResult = exchangeObjectRatesResult
                    .ConvertAll(o => (Contract.ExchangeRate)o);
                var tellmaExchangeRates = exchangeRatesResult
                    .Select(e => new ExchangeRateForSave
                    {
                        Id = e.Id,
                        CurrencyId = e.CurrencyId,
                        ValidAsOf = e.ValidAsOf,
                        AmountInCurrency = e.AmountInCurrency,
                        AmountInFunctional = e.AmountInFunctional
                    }).ToList();
                var worksheets = await _repository.GetLatestExchangeRatesFromDB();
                var dbExchangeRatesList = worksheets
                    .Select(e => new ExchangeRateForSave
                    {
                        CurrencyId = e.CurrencyId,
                        ValidAsOf = e.ValidAsOf,
                        AmountInCurrency = e.AmountInCurrency,
                        AmountInFunctional = e.AmountInFunctional
                    }).ToList();

                var tellmaExchangeRatesCompare = dbExchangeRatesList
                    .Select(r => r.CurrencyId + '-' + r.ValidAsOf + '-' + r.AmountInCurrency + '-' + r.AmountInFunctional);

                //Exclude existing exchange rates in tellma from dbExchangeRates.
                var newRates = dbExchangeRatesList
                    .Where(r => !tellmaExchangeRatesCompare.Contains(r.CurrencyId + '-' + r.ValidAsOf + '-' + r.AmountInCurrency + '-' + r.AmountInFunctional))
                    .ToList();

                if (!newRates.Any())
                {
                    _logger.LogInformation($"\n For tenant: {tenantId} Exchange rates are upto date! \n");
                    continue;
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
}
