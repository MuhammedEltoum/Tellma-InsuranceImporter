using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public interface IExchangeRatesRepository
    {
        Task<List<ExchangeRate>> GetLatestExchangeRatesFromDB();
        Task InsertNewExchangeRates(IEnumerable<ExchangeRate> exchangeRates);
    }
}
