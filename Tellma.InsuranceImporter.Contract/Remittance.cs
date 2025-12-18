namespace Tellma.InsuranceImporter.Contract
{
    public class Remittance
    {
        public int PK { get; set; }
        public string? WorksheetId { get; set; }
        public DateTime PostingDate { get; set; }
        public bool IsPosted { get; set; }
        public int TellmaDocumentId { get; set; } = 0;
        public string TransferToTellma { get; set; }
        public string? Reference { get; set; } = "-";
        public string? AgentCode { get; set; } = String.Empty;
        public string? AgentName { get; set; }
        public string? TenantCode { get; set; }
        public sbyte Direction { get; set; }
        public Decimal TransferAmount { get; set; }
        public string TransferCurrencyId { get; set; } = String.Empty;
        public string BankAccountCode { get; set; } = String.Empty;
        public string BankAccountName { get; set; } = String.Empty;
        public Decimal BankAccountAmount { get; set; }
        public Decimal BankAccountFee { get; set; }
        public string BankAccountCurrencyId { get; set; } = String.Empty;
        public Decimal ValueFC2 { get; set; }
        public String RemittanceType { get; set; } = String.Empty;
        public String RemittanceTypeName { get; set; } = String.Empty;
        public String? RemittanceNotes { get; set; }
        public string BalObjectId {  get; set; } = String.Empty;
        public string? AAccount { get; set; }
        public int? ANotedAgentId { get; set; } = 0;
        public int? AResourceId { get; set; } = 0;
        public int? ANotedResourceId { get; set; } = 0;
        public string? APurposeConcept { get; set; }
        public int APurposeId { get; set; } = 0;
        public sbyte ADirection { get; set; }
        public byte? AQuantity { get; set; } = 0;
        public bool AHasNOTEDDATE { get; set; } = false;
        public bool AIsBankAcc { get; set; } = false;
        public string? BAccount { get; set; }
        public int? BNotedAgentId { get; set; } = 0;
        public int? BResourceId { get; set; } = 0;
        public int? BNotedResourceId { get; set; } = 0;
        public string? BPurposeConcept { get; set; }
        public int BPurposeId { get; set; } = 0;
        public sbyte BDirection { get; set; }
        public byte? BQuantity { get; set; } = 0;
        public bool BHasNOTEDDATE { get; set; } = false;
        public bool BIsBankAcc { get; set; } = false;
    }
}
