namespace Tellma.InsuranceImporter.Contract
{
    public class Pairing
    {
        public int Pk { get; set; }
        public DateTime PairingDate { get; set; }
        public string TechWsId { get; set; } = String.Empty;
        public decimal TechAmount { get; set; }
        public string TechCurrency { get; set; } = String.Empty;
        public string RemitWsId { get; set; } = String.Empty;
        public decimal RemitAmount { get; set; }
        public string RemitCurrency { get; set; } = String.Empty;
        public int? TellmaDocumentId { get; set; }
        public int BatchId { get; set; }
        public string PObjectId { get; set; } = String.Empty;
        public string Bal1ObjectId { get; set; } = String.Empty;
        public string WorksheetId1 { get; set; } = String.Empty;
        public string AgentCode1 { get; set; } = String.Empty;
        public string AgentName1 { get; set; } = String.Empty;
        public string TenantCode1 { get; set; } = String.Empty;
        public string TenantName1 { get; set; } = String.Empty;
        public string Bal2ObjectId { get; set; } = String.Empty;
        public string WorksheetId2 { get; set; } = String.Empty;
        public string AgentCode2 { get; set; } = String.Empty;
        public string AgentName2 { get; set; } = String.Empty;
        public string TenantCode2 { get; set; } = String.Empty;
        public string TenantName2 { get; set; } = String.Empty;
        public string ContractCode { get; set; } = String.Empty;
        public DateTime EffectiveDate { get; set; }
        public DateTime ExpiryDate { get; set; }
        public string BrokerCode { get; set; } = String.Empty;
        public short RemitDirection { get; set; }
        public DateTime RemittancePaymentDate { get; set; }
        public string ContractCurrencyId { get; set; } = String.Empty;
        public decimal SumMonetaryValue { get; set; } = 0;
        public decimal SumValue { get; set; }
        public bool TechIsInward { get; set; }
        public short TechDirection { get; set; }
        public string BusinessMainClassCode { get; set; } = String.Empty;
        public string TechWorksheet { get; set; } = String.Empty;
        public string RemitWorksheet { get; set; } = String.Empty;
        public string AccountCode { get; set; } = String.Empty;
        public bool BTaxAccount { get; set; }
        public bool BHasNotedDate { get; set; }
        public string RemitInsuranceAgent { get; set; } = String.Empty;
        public string TechInsuranceAgent { get; set; } = String.Empty;
        public DateTime TechNotedDate { get; set; }
        public short BDirection { get; set; }
    }
}
