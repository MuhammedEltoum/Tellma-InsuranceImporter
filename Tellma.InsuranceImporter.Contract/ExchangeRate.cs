using System.Xml.Linq;

namespace Tellma.InsuranceImporter.Contract
{
    public class ExchangeRate
    {
        public int Id { get; } = 0;
        public string CurrencyId { get; set; } = string.Empty;
        public DateTime ValidAsOf { get; set; }
        public decimal AmountInCurrency { get; set; }
        public decimal AmountInFunctional { get; set; }
        public DateTimeOffset Created_Date { get; set; }
    }
}