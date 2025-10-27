using Microsoft.Extensions.Options;
using System.Diagnostics;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public class ExchangeRatesRepository : RepositoryBase, IExchangeRatesRepository
    {
        public ExchangeRatesRepository(IOptions<InsuranceDBOptions> dbOptions) : base(dbOptions.Value.ConnectionString) { }

        public async Task<List<ExchangeRate>> GetLatestExchangeRatesFromDB()
        {
            var exchangeRates = new List<ExchangeRate>();

            using (var reader = await ExecuteReaderAsync($"" +
                $"SELECT CurrencyId, ValidAsOf, AmountInFunctional" +
                $" FROM ExchangeRates" +
                $" WHERE CurrencyId <> 'USD'" +
                $" AND Year(ValidAsOf) = {DateTime.Now.Year} " +
                $" AND Month(ValidAsOf) = {DateTime.Now.Month} " +
                $" ORDER BY ValidAsOf DESC"))
            {
                while (await reader.ReadAsync())
                {
                    exchangeRates.Add(new ExchangeRate
                    {
                        CurrencyId = reader.GetString(0),
                        ValidAsOf = reader.GetDateTime(1),
                        AmountInCurrency = Math.Round(1 / reader.GetDecimal(2), 6),
                        AmountInFunctional = 1
                    });
                }
            }
            return exchangeRates;
        }

        public async Task InsertNewExchangeRates(IEnumerable<ExchangeRate> exchangeRates)
        {
            foreach (var exchangeRate in exchangeRates)
            {
                var insertStatement = "INSERT INTO ExchangeRates " +
                    "(CurrencyId, AmountInCurrency, AmountInFunctional, Rate, ValidAsOf, ValidTill, Created_Date)" +
                    " VALUES ('"
                    + exchangeRate.CurrencyId.ToString() + "', " 
                    + exchangeRate.AmountInCurrency + ", " 
                    + exchangeRate.AmountInFunctional + ", "
                    + exchangeRate.ValidAsOf.ToString("yyyy-MM-dd") + "', '"
                    + exchangeRate.Created_Date.ToString() + "');";

               await ExecuteNonQueryAsync(insertStatement);
            }
        }
    }
}
