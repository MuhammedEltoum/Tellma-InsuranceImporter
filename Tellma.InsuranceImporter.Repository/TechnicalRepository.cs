using Microsoft.Extensions.Options;
using System;
using System.Data;
using System.Security.Principal;
using Tellma.InsuranceImporter.Contract;
using static System.Net.Mime.MediaTypeNames;

namespace Tellma.InsuranceImporter.Repository
{
    public class TechnicalRepository : RepositoryBase, IWorksheetRepository<Technical>
    {
        public TechnicalRepository(IOptions<InsuranceDBOptions> dbOptions) : base(dbOptions.Value.ConnectionString) { }

        public async Task<List<Technical>> GetMappingAccounts(CancellationToken token)
        {
            List<Technical> technicalsList = new List<Technical>();

            string selectQuery = "SELECT [SICS_Account], " +
                                  "[IS_INWARD], " +
                                  "[A Account], " +
                                  "[A TAX Account], " +
                                  "[A Purpose - Concept], " +
                                  "[A Has NOTED_DATE], " +
                                  "[B Account], " +
                                  "[B TAX Account], " +
                                  "[B Purpose - Concept], " +
                                  "[B Has NOTED_DATE]," +
                                  "[CanBePairing] " +
                              "FROM [dbo].[Tellma_Mapping_Technical];";

            using (var reader = await ExecuteReaderAsync(selectQuery))
            {
                while (await reader.ReadAsync())
                {
                    technicalsList.Add(new Technical
                    {
                        AccountCode = reader.GetString(0),
                        IsInward = reader.GetByte(1) > 0 ? true : false,
                        AAccount = reader.GetString(2),
                        ATaxAccount = !reader.IsDBNull(3) ? reader.GetBoolean(3) : false,
                        APurposeConcept = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                        AHasNotedDate = !reader.IsDBNull(5) ? reader.GetBoolean(5) : false,
                        BAccount = !reader.IsDBNull(6) ? reader.GetString(6) : null,
                        BTaxAccount = !reader.IsDBNull(7) ? reader.GetBoolean(7) : false,
                        BPurposeConcept = !reader.IsDBNull(8) ? reader.GetString(8) : null,
                        BHasNotedDate = !reader.IsDBNull(9) ? reader.GetBoolean(9) : false,
                        IsPairingAccount = !reader.IsDBNull(10) ? reader.GetBoolean(10) : false
                    });
                }
            }
            return technicalsList;
        }

        public async Task<List<Technical>> GetWorksheets(bool includeImported, string filter, CancellationToken token)
        {
            List<Technical> technicalsList = new List<Technical>();
            string selectQuery = "SELECT [PK], " +
                                  "[WORKSHEET_ID], " +
                                  "[DESCRIPTION], " +
                                  "[POSTING_DATE], " +
                                  "[CLOSING_DATE], " +
                                  "[IS_INWARD], " +
                                  "[CONTRACT_CODE], " +
                                  "[CONTRACT_NAME], " +
                                  "[BUSINESS_TYPE_CODE], " +
                                  "[BUSINESS_MAIN_CLASS_CODE], " +
                                  "[BUSINESS_MAIN_CLASS_NAME], " +
                                  "[AGENT_CODE], " +
                                  "[AGENT_NAME], " +
                                  "[BROKER_CODE], " +
                                  "[BROKER_NAME], " +
                                  "[CEDANT_CODE], " +
                                  "[CEDANT_NAME], " +
                                  "[REINSURER_CODE], " +
                                  "[REINSURER_NAME], " +
                                  "[INSURED_CODE], " +
                                  "[INSURED_NAME], " +
                                  "[RISK_COUNTRY], " +
                                  "[EFFECTIVE_DATE], " +
                                  "[EXPIRY_DATE], " +
                                  "[DIRECTION], " +
                                  "[CONTRACT_AMOUNT], " +
                                  "[CONTRACT_CURRENCY_ID], " +
                                  "[VALUE_FC2], " +
                                  "[CHANNEL_CODE], " +
                                  "[CHANNEL_NAME], " +
                                  "[NOTED_DATE], " +
                                  "[TENANT_CODE], " +
                                  "[TENANT_NAME], " +
                                  "[TELLMA_DOCUMENT_ID], " +
                                  "[ACCOUNT_CODE], " +
                                  "[Technical_Notes], " +
                                  "[BAL_OBJECT_ID], " +
                                  "[D_OBJECT_ID], " +
                                  "[TRANSFER_TO_TELLMA] " +
                              "FROM [dbo].[Technicals] " +
                              "WHERE 1=1 " + filter + " " + (includeImported ? " " : " AND [TRANSFER_TO_TELLMA] = N'N' ");
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
                        AgentCode = !reader.IsDBNull(11) ? reader.GetString(11) : null,
                        AgentName = !reader.IsDBNull(12) ? reader.GetString(12) : null,
                        BrokerCode = !reader.IsDBNull(13) ? reader.GetString(13) : null,
                        BrokerName = !reader.IsDBNull(14) ? reader.GetString(14) : null,
                        CedantCode = !reader.IsDBNull(15) ? reader.GetString(15) : null,
                        CedantName = !reader.IsDBNull(16) ? reader.GetString(16) : null,
                        ReinsurerCode = !reader.IsDBNull(17) ? reader.GetString(17) : null,
                        ReinsurerName = !reader.IsDBNull(18) ? reader.GetString(18) : null,
                        InsuredCode = !reader.IsDBNull(19) ? reader.GetString(19) : null,
                        InsuredName = !reader.IsDBNull(20) ? reader.GetString(20) : null,
                        RiskCountry = !reader.IsDBNull(21) ? reader.GetString(21) : null,
                        EffectiveDate = reader.GetDateTime(22),
                        ExpiryDate = reader.GetDateTime(23),
                        Direction = reader.GetInt16(24),
                        ContractAmount = reader.GetDecimal(25),
                        ContractCurrencyId = reader.GetString(26),
                        ValueFc2 = reader.GetDecimal(27),
                        ChannelCode = !reader.IsDBNull(28) ? reader.GetString(28) : null,
                        ChannelName = !reader.IsDBNull(29) ? reader.GetString(29) : null,
                        NotedDate = reader.GetDateTime(30),
                        TenantCode = reader.GetString(31),
                        TenantName = reader.GetString(32),
                        TellmaDocumentId = !reader.IsDBNull(33) ? reader.GetInt32(33) : 0,
                        AccountCode = reader.GetString(34),
                        TechnicalNotes = !reader.IsDBNull(35) ? reader.GetString(35) : null,
                        BalObjectId = !reader.IsDBNull(36) ? reader.GetString(36) : null,
                        DObjectId = !reader.IsDBNull(37) ? reader.GetString(37) : null,
                        TransferToTellma = !reader.IsDBNull(38) ? reader.GetString(38) : "N"
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