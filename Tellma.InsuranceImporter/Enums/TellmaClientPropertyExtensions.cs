using System;

namespace Tellma.InsuranceImporter.Enums
{
    public static class TellmaClientPropertyExtensions
    {
        public static string AsString(this TellmaClientProperty property)
        {
            return property switch
            {
                TellmaClientProperty.AgentDefinitions => "AgentDefinitions",
                TellmaClientProperty.Agents => "Agents",
                TellmaClientProperty.Resources => "Resources",
                TellmaClientProperty.Lookups => "Lookups",
                TellmaClientProperty.Documents => "Documents",
                TellmaClientProperty.DocumentDefinitions => "DocumentDefinitions",
                TellmaClientProperty.LineDefinitions => "LineDefinitions",
                TellmaClientProperty.EntryTypes => "EntryTypes",
                TellmaClientProperty.Accounts => "Accounts",
                TellmaClientProperty.Centers => "Centers",
                TellmaClientProperty.LookupDefinitions => "LookupDefinitions",
                TellmaClientProperty.ExchangeRates => "ExchangeRates",
                TellmaClientProperty.Users => "Users",
                TellmaClientProperty.Roles => "Roles",
                TellmaClientProperty.Units => "Units",
                TellmaClientProperty.Currencies => "Currencies",
                TellmaClientProperty.Emails => "Emails",
                TellmaClientProperty.SmsMessages => "SmsMessages",
                TellmaClientProperty.Outbox => "Outbox",
                TellmaClientProperty.DashboardDefinitions => "DashboardDefinitions",
                TellmaClientProperty.ReportDefinitions => "ReportDefinitions",
                TellmaClientProperty.PrintingTemplates => "PrintingTemplates",
                TellmaClientProperty.ResourceDefinitions => "ResourceDefinitions",
                _ => throw new ArgumentOutOfRangeException(nameof(property), property, null)
            };
        }
    }
}
