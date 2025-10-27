using Microsoft.Extensions.Options;
using System;
using System.Security.Principal;
using Tellma.InsuranceImporter.Contract;
using static System.Net.Mime.MediaTypeNames;

namespace Tellma.InsuranceImporter.Repository
{
    public class TechnicalRepository : RepositoryBase, IWorksheetRepository<Technical>
    {
        public TechnicalRepository(IOptions<InsuranceDBOptions> dbOptions) : base(dbOptions.Value.ConnectionString) { }
        public async Task<List<Technical>> GetWorksheets(CancellationToken token)
        {
            List<Technical> technicalsList = new List<Technical>();
            string selectQuery = "SELECT [PK]" +
                                  ",[WORKSHEET_ID]" +
                                  ",[DESCRIPTION]" +
                                  ",[POSTING_DATE]" +
                                  ",[CLOSING_DATE]" +
                                  ", T.[IS_INWARD]" +
                                  ",[CONTRACT_CODE]" +
                                  ",[CONTRACT_NAME]" +
                                  ",[BUSINESS_TYPE_CODE]" +
                                  ",[BUSINESS_MAIN_CLASS_CODE]" +
                                  ",[BUSINESS_MAIN_CLASS_NAME]" +
                                  ",[BUSINESS_CLASS_CODE]" +
                                  ",[BUSINESS_CLASS_NAME]" +
                                  ",[BUSINESS_SUB_CLASS_CODE]" +
                                  ",[BUSINESS_SUB_CLASSNAME]" +
                                  ",[AGENT_CODE]" +
                                  ",[AGENT_NAME]" +
                                  ",[BROKER_CODE]" +
                                  ",[BROKER_NAME]" +
                                  ",[CEDANT_CODE]" +
                                  ",[CEDANT_NAME]" +
                                  ",[REINSURER_CODE]" +
                                  ",[REINSURER_NAME]" +
                                  ",[RISK_COUNTRY]" +
                                  ",[EFFECTIVE_DATE]" +
                                  ",[EXPIRY_DATE]" +
                                  ",[DIRECTION]" +
                                  ",[CONTRACT_AMOUNT]" +
                                  ",[CONTRACT_CURRENCY_ID]" +
                                  ",[VALUE_FC2]" +
                                  ",[CHANNEL_CODE]" +
                                  ",[CHANNEL_NAME]" +
                                  ",[NOTED_DATE]" +
                                  ",[TENANT_CODE]" +
                                  ",[TENANT_NAME]" +
                                  ",[TELLMA_DOCUMENT_ID]" +
                                  ",[ACCOUNT_CODE]" +
                                  ",[Technical_Notes]" +
                                  ",TMT.[A Account]" +
                                  ",TMT.[A sign]" +
                                  ",TMT.[A TAX Account]" +
                                  ",TMT.[A Purpose - Concept]" +
                                  ",TMT.[A Has NOTED_DATE]" +
                                  ",TMT.[B Account]" +
                                  ",TMT.[B sign]" +
                                  ",TMT.[B TAX Account]" +
                                  ",TMT.[B Purpose - Concept]" +
                                  ",TMT.[B Has NOTED_DATE]" +
                              "FROM [dbo].[Technicals] T " +
                                "INNER JOIN Tellma_Mapping_Technical TMT " +
                                "ON T.IS_INWARD = TMT.IS_INWARD AND T.ACCOUNT_CODE = TMT.SICS_Account " +
                              "WHERE [TRANSFER_TO_TELLMA] = N'N' AND [IMPORT_DATE] IS NULL AND VALUE_FC2 <> 0;";
            using (var reader = await ExecuteReaderAsync(selectQuery))
            {
                while (await reader.ReadAsync())
                {
                    technicalsList.Add(new Technical
                    {
                        PK = reader.GetInt32(0),
                        WorksheetId = reader.GetString(1),
                        Description = reader.GetString(2),
                        PostingDate = reader.GetDateTime(3),
                        ClosingDate = reader.GetDateTime(4),
                        IsInward = reader.GetBoolean(5),
                        ContractCode = !reader.IsDBNull(6) ? reader.GetString(6) : null,
                        ContractName = !reader.IsDBNull(7) ? reader.GetString(7) : null,
                        BusinessTypeCode = !reader.IsDBNull(8) ? reader.GetString(8) : null,
                        BusinessMainClassCode = !reader.IsDBNull(9) ? reader.GetString(9) : null,
                        BusinessMainClassName = !reader.IsDBNull(10) ? reader.GetString(10) : null,
                        BusinessClassCode = !reader.IsDBNull(11) ? reader.GetString(11) : null,
                        BusinessClassName = !reader.IsDBNull(12) ? reader.GetString(12) : null,
                        BusinessSubClassCode = !reader.IsDBNull(13) ? reader.GetString(13) : null,
                        BusinessSubClassName = !reader.IsDBNull(14) ? reader.GetString(14) : null,
                        AgentCode = !reader.IsDBNull(15) ? reader.GetString(15) : null,
                        AgentName = !reader.IsDBNull(16) ? reader.GetString(16) : null,
                        BrokerCode = !reader.IsDBNull(17) ? reader.GetString(17) : null,
                        BrokerName = !reader.IsDBNull(18) ? reader.GetString(18) : null,
                        CedantCode = !reader.IsDBNull(19) ? reader.GetString(19) : null,
                        CedantName = !reader.IsDBNull(20) ? reader.GetString(20) : null,
                        ReinsurerCode = !reader.IsDBNull(21) ? reader.GetString(21) : null,
                        ReinsurerName = !reader.IsDBNull(22) ? reader.GetString(22) : null,
                        RiskCountry = !reader.IsDBNull(23) ? reader.GetString(23) : null,
                        EffectiveDate = reader.GetDateTime(24),
                        ExpiryDate = reader.GetDateTime(25),
                        Direction = reader.GetInt16(26),
                        ContractAmount = reader.GetDecimal(27),
                        ContractCurrencyId = reader.GetString(28),
                        ValueFc2 = reader.GetDecimal(29),
                        ChannelCode = !reader.IsDBNull(30) ? reader.GetString(30) : null,
                        ChannelName = !reader.IsDBNull(31) ? reader.GetString(31) : null,
                        NotedDate = reader.GetDateTime(32),
                        TenantCode = reader.GetString(33),
                        TenantName = reader.GetString(34),
                        TellmaDocumentId = !reader.IsDBNull(35) ? reader.GetInt32(35) : 0,
                        AccountCode = reader.GetString(36),
                        TechnicalNotes = !reader.IsDBNull(37) ? reader.GetString(37) : null,
                        AAccount = !reader.IsDBNull(38) ? reader.GetString(38) : null,
                        ASign = !reader.IsDBNull(39) ? reader.GetString(39) : null,
                        ATaxAccount = !reader.IsDBNull(40) ? reader.GetBoolean(40) : false,
                        APurposeConcept = !reader.IsDBNull(41) ? reader.GetString(41) : null,
                        AHasNotedDate = !reader.IsDBNull(42) ? reader.GetBoolean(42) : false,
                        BAccount = !reader.IsDBNull(43) ? reader.GetString(43) : null,
                        BSign = !reader.IsDBNull(44) ? reader.GetString(44) : null,
                        BTaxAccount = !reader.IsDBNull(45) ? reader.GetBoolean(45) : false,
                        BPurposeConcept = !reader.IsDBNull(46) ? reader.GetString(46) : null,
                        BHasNotedDate = !reader.IsDBNull(47) ? reader.GetBoolean(47) : false
                    });
                }
            }
            return technicalsList;
        }

        public async Task UpdateDocumentIds(string tenantCode, IEnumerable<Technical> worksheets, CancellationToken token)
        {
            if (!worksheets.Any())
                return;
            
            var query = new List<string>();
            foreach (var technical in worksheets)
            {
                string updateStatement = "UPDATE [dbo].[Technicals] " +
                                         $"SET TELLMA_DOCUMENT_ID = {technical.TellmaDocumentId} " +
                                      $"WHERE TENANT_CODE = '{tenantCode}' AND WORKSHEET_ID = '{technical.WorksheetId}';";
                query.Add(updateStatement);
            }

            await ExecuteNonQueryAsync(string.Join(" ", query));
        }

        public async Task UpdateImportedWorksheets(string tenantCode, IEnumerable<Technical> worksheets, CancellationToken token)
        {
            if (!worksheets.Any())
                return;

            var query = new List<string>();
            foreach (var technical in worksheets)
            {
                string updateStatement = "UPDATE [dbo].[Technicals] " +
                                          "SET TRANSFER_TO_TELLMA = 'Y', " +
                                          $"IMPORT_DATE = '{DateTime.Now.ToString("yyyy-MM-dd")}'" +
                                      $"WHERE TENANT_CODE = '{tenantCode}' AND WORKSHEET_ID = '{technical.WorksheetId}';";
                query.Add(updateStatement);
            }
            await ExecuteNonQueryAsync(string.Join(" ", query));
        }
    }
}