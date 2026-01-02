using System;
using System.Collections.Generic;
using System.Text;

namespace Tellma.InsuranceImporter
{
    public class InsuranceOptions
    {
        // Service Enablers
        public bool EnableExchangeRate { get; set; }
        public bool EnableRemittance { get; set; }
        public bool EnableTechnical { get; set; }
        public bool EnablePairing { get; set; }
        
        // Remittance Options
        public string RemittanceSupportedPrefixes { get; set; }
        
        // Technical Options
        public string TechnicalSupportedPrefixes { get; set; }
        
        // Pairing Options
        public string PairingSupportedPrefixes { get; set; }
        public string PrvsPairingTransactionsDate { get; set; }

        // Daily schedule Options
        public int Hour { get; set; }
        public int Minute { get; set; }
        public int Second { get; set; }
    }
}
