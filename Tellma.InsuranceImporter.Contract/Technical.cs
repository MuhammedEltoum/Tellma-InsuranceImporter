using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tellma.InsuranceImporter.Contract
{
    public class Technical
    {
        public int PK {get; set;}
        public string WorksheetId {get; set;}
        public string Description {get; set;}
        public DateTime PostingDate {get; set;}
        public DateTime ClosingDate {get; set;}
        public bool IsInward {get; set;}
        public string ContractName {get; set;}
        public string ContractCode {get; set;}
        public string BusinessTypeCode {get; set;}
        public string BusinessMainClassCode {get; set;}
        public string BusinessMainClassName {get; set;}
        public string AgentCode {get; set;}
        public string AgentName {get; set;}
        public string BrokerCode {get; set;}
        public string BrokerName {get; set;}
        public string CedantCode {get; set;}
        public string CedantName {get; set;}
        public string ReinsurerCode {get; set;}
        public string ReinsurerName {get; set;}
        public string RiskCountry {get; set;}
        public string InsuredCode { get; set; }
        public string InsuredName { get; set; }
        public DateTime EffectiveDate {get; set;}
        public DateTime ExpiryDate {get; set;}
        public Int16 Direction {get; set;}
        public decimal ContractAmount {get; set;}
        public string ContractCurrencyId {get; set;}
        public decimal ValueFc2 {get; set;}
        public string ChannelCode {get; set;}
        public string ChannelName {get; set;}
        public DateTime NotedDate {get; set;}
        public string TenantCode {get; set;}
        public string TenantName {get; set;}
        public Int32 TellmaDocumentId { get; set; } = 0;
        public string TransferToTellma {get; set;}
        public string AccountCode {get; set;}
        public DateTime ImportDate {get; set;}
        public string TechnicalNotes {get; set;}
        public string AAccount {get; set;}
        public string ASign {get; set;}
        public bool ATaxAccount {get; set;}
        public string APurposeConcept {get; set;}
        public bool AHasNotedDate {get; set;}
        public string BAccount {get; set;}
        public string BSign {get; set;}
        public bool BTaxAccount {get; set;}
        public string BPurposeConcept {get; set;}
        public bool BHasNotedDate {get; set;}
        public string? BalObjectId { get; set; } = String.Empty;
        public string? DObjectId { get; set; } = String.Empty;
        public bool IsPairingAccount { get; set; } = false;
    }
}
