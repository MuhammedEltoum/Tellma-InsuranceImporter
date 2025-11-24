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
        public DateTime? ImportDate { get; set; }
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

    }
}
