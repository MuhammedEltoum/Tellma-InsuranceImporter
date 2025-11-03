using Microsoft.Extensions.Options;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public class RemittanceRepository : RepositoryBase, IWorksheetRepository<Remittance>
    {
        public RemittanceRepository(IOptions<InsuranceDBOptions> dbOptions) : base(dbOptions.Value.ConnectionString) { }

        public async Task<List<Remittance>> GetWorksheets(CancellationToken token)
        {
            List<Remittance> remittancesList = new List<Remittance>();
            string selectQuery = "SELECT [PK]" +
                                    ",[WORKSHEET_ID]" +
                                    ",[PAYMENT_DATE]" +
                                    ",[REFERENCE]" +
                                    ",[AGENT_CODE]" +
                                    ",[AGENT_Name]" +
                                    ",[TENANT_CODE]" +
                                    ",R.[DIRECTION]" +
                                    ",[TRANSFER_AMOUNT]" +
                                    ",[TRANSFER_CURRENCY_ID]" +
                                    ",[BANK_ACCOUNT_AMOUNT]" +
                                    ",[BANK_ACCOUNT_FEE]" +
                                    ",[BANK_ACCOUNT_CURRENCY_ID]" +
                                    ",[BANK_ACCOUNT_CODE]" +
                                    ",[BANK_ACCOUNT_NAME]" +
                                    ",[VALUE_FC2]" +
                                    ",[RemitType]" +
                                    ",TMR.[RemittanceTypeName]" +
                                    ",[Remittance_Notes]" +
                                    ",TMR.[A Account]" +
                                    ",TMR.[ANotedAgentId]" +
                                    ",TMR.[AResourceId]" +
                                    ",TMR.[ANotedResourceId]" +
                                    ",TMR.[A Purpose - Concept]" +
                                    ",TMR.[A Purpose - Id]" +
                                    ",TMR.[A Direction]" +
                                    ",TMR.[A Quantity]" +
                                    ",TMR.[A Has NOTED_DATE]" +
                                    ",TMR.[A Is Bank_Account?]" +
                                    ",TMR.[B Account]" +
                                    ",TMR.[BNotedAgentId]" +
                                    ",TMR.[BResourceId]" +
                                    ",TMR.[BNotedResourceId]" +
                                    ",TMR.[B Purpose - Concept]" +
                                    ",TMR.[B Purpose - Id]" +
                                    ",TMR.[B Direction]" +
                                    ",TMR.[B Quantity]" +
                                    ",TMR.[B Has NOTED_DATE] " +
                                    ",TMR.[B Is Bank_Account?]" +
                                    ",[TELLMA_DOCUMENT_ID]" +
                                "FROM [Remittances] R " +
                                "LEFT JOIN Tellma_Mapping_Remittance TMR " +
                                "ON R.RemitType = TMR.RemittanceTypeCode AND R.DIRECTION = TMR.Direction " +
                                "WHERE TRANSFER_TO_TELLMA = N'N' AND R.IMPORT_DATE IS NULL AND VALUE_FC2 <> 0;";
            using (var reader = await ExecuteReaderAsync(selectQuery))
            {
                while (await reader.ReadAsync())
                {
                    remittancesList.Add(new Remittance
                    {
                        PK = reader.GetInt32(0),
                        WorksheetId = reader.GetString(1),
                        PostingDate = reader.GetDateTime(2),
                        Reference = reader.GetString(3),
                        AgentCode = reader.GetString(4),
                        AgentName = reader.GetString(5),
                        TenantCode = reader.GetString(6),
                        Direction = Convert.ToSByte(reader.GetInt16(7)),
                        TransferAmount = Math.Round(reader.GetDecimal(8), 6),
                        TransferCurrencyId = reader.GetString(9),
                        BankAccountAmount = Math.Round(reader.GetDecimal(10), 6),
                        BankAccountFee = Math.Round(reader.GetDecimal(11), 6),
                        BankAccountCurrencyId = reader.GetString(12),
                        BankAccountCode = reader.GetString(13),
                        BankAccountName = reader.GetString(14),
                        ValueFC2 = Math.Round(reader.GetDecimal(15), 6),
                        RemittanceType = reader.GetString(16),
                        RemittanceTypeName = reader.GetString(17),
                        RemittanceNotes = !reader.IsDBNull(18) ? reader.GetString(18) : null,
                        AAccount = reader.GetString(19),
                        ANotedAgentId = !reader.IsDBNull(20) ? reader.GetInt32(20) : null,
                        AResourceId = !reader.IsDBNull(21) ? reader.GetInt32(21) : null,
                        ANotedResourceId = !reader.IsDBNull(22) ? reader.GetInt32(22) : null,
                        APurposeConcept = reader.GetString(23),
                        APurposeId = reader.GetInt32(24),
                        ADirection = Convert.ToSByte(reader.GetByte(25)),
                        AQuantity = !reader.IsDBNull(26) ? Convert.ToByte(reader.GetBoolean(26)) : null,
                        AHasNOTEDDATE = reader.GetBoolean(27),
                        AIsBankAcc = reader.GetBoolean(28),
                        BAccount = reader.GetString(29),
                        BNotedAgentId = !reader.IsDBNull(30) ? reader.GetInt32(30) : null,
                        BResourceId = !reader.IsDBNull(31) ? reader.GetInt32(31) : null,
                        BNotedResourceId = !reader.IsDBNull(32) ? reader.GetInt32(32) : null,
                        BPurposeConcept = reader.GetString(33),
                        BPurposeId = reader.GetInt32(34),
                        BDirection = Convert.ToSByte(reader.GetInt32(35)),
                        BQuantity = !reader.IsDBNull(36) ? Convert.ToByte(reader.GetBoolean(36)) : null,
                        BHasNOTEDDATE = reader.GetBoolean(37),
                        BIsBankAcc = reader.GetBoolean(38),
                        DocumentId = !reader.IsDBNull(39) ? reader.GetInt32(39) : 0
                    });
                }
            }
            return remittancesList;
        }

        public async Task UpdateDocumentIds(string tenantCode, IEnumerable<Remittance> remittances, CancellationToken token)
        {
            var query = new List<string>();
            foreach (var remittance in remittances)
            {
                string updateStatement = "UPDATE [dbo].[Remittances] " +
                                         $"SET TELLMA_DOCUMENT_ID = {remittance.DocumentId} " +
                                      $"WHERE TENANT_CODE = '{tenantCode}' AND WORKSHEET_ID = '{remittance.WorksheetId}';";
                query.Add(updateStatement);
            }

            await ExecuteNonQueryAsync(string.Join(" ", query));
        }

        public async Task UpdateImportedWorksheets(string tenantCode, IEnumerable<Remittance> remittances, CancellationToken token)
        {
            var query = new List<string>();
            foreach (var remittance in remittances)
            {
                string updateStatement = "UPDATE [dbo].[Remittances] " +
                                          "SET TRANSFER_TO_TELLMA = 'Y', " +
                                          $"IMPORT_DATE = '{DateTime.Now.ToString("yyyy-MM-dd")}'" +
                                      $"WHERE TENANT_CODE = '{tenantCode}' AND WORKSHEET_ID = '{remittance.WorksheetId}';";
                query.Add(updateStatement);
            }
            await ExecuteNonQueryAsync(string.Join(" ", query));
        }
    }
}
