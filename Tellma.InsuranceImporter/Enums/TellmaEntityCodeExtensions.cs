using System;

namespace Tellma.InsuranceImporter.Enums
{
    public static class TellmaEntityCodeExtensions
    {
        public static string AsString(this TellmaEntityCode code)
        {
            return code switch
            {
                TellmaEntityCode.InsuranceAgent => "InsuranceAgent",
                TellmaEntityCode.BankAccount => "BankAccount",
                TellmaEntityCode.Inward => "Inward",
                TellmaEntityCode.Outward => "Outward",
                TellmaEntityCode.TechnicalInOutward => "TechnicalInOutward",
                TellmaEntityCode.RemittanceWorksheet => "RemittanceWorksheet",
                TellmaEntityCode.ManualLine => "ManualLine",
                TellmaEntityCode.OperationCenter => "20",
                TellmaEntityCode.MainBusinessClass => "MainBusinessClass",
                TellmaEntityCode.Citizenship => "Citizenship",
                TellmaEntityCode.TradeReceivableAccount => "TradeReceivableAccount",
                TellmaEntityCode.BusinessType => "BusinessType",
                TellmaEntityCode.ClaimWorksheet => "ClaimWorksheet",
                TellmaEntityCode.TechnicalWorksheet => "TechnicalWorksheet",
                TellmaEntityCode.InsuranceContract => "InsuranceContract",
                TellmaEntityCode.PartnershipTypes => "PartnershipTypes",
                TellmaEntityCode.BusinessPartner => "BusinessPartner",
                _ => throw new ArgumentOutOfRangeException(nameof(code), code, null)
            };
        }
    }
}
