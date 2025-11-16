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
            string selectQuery = "SELECT [PK], " +
                                    "[WORKSHEET_ID], " +
                                    "[PAYMENT_DATE], " +
                                    "[REFERENCE], " +
                                    "[AGENT_CODE], " +
                                    "[AGENT_Name], " +
                                    "[TENANT_CODE], " +
                                    "[DIRECTION], " +
                                    "[TRANSFER_AMOUNT], " +
                                    "[TRANSFER_CURRENCY_ID], " +
                                    "[BANK_ACCOUNT_AMOUNT], " +
                                    "[BANK_ACCOUNT_FEE], " +
                                    "[BANK_ACCOUNT_CURRENCY_ID], " +
                                    "[BANK_ACCOUNT_CODE], " +
                                    "[BANK_ACCOUNT_NAME], " +
                                    "[VALUE_FC2], " +
                                    "[RemitType], " +
                                    "[Remittance_Notes], " +
                                    "[TELLMA_DOCUMENT_ID] " +
                                "FROM [Remittances] " +
                                "WHERE TRANSFER_TO_TELLMA = N'N' AND IMPORT_DATE IS NULL AND VALUE_FC2 <> 0;";
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
                        RemittanceNotes = !reader.IsDBNull(17) ? reader.GetString(17) : null,
                        DocumentId = !reader.IsDBNull(18) ? reader.GetInt32(18) : 0
                    });
                }
            }
            return remittancesList;
        }

        public async Task<List<Remittance>> GetMappingAccounts(CancellationToken token)
        {
            List<Remittance> remittancesList = new List<Remittance>();
            string selectQuery = "SELECT [RemittanceTypeCode], " +
                                    "[RemittanceTypeName], " +
                                    "[Direction], " +
                                    "[A Account], " +
                                    "[ANotedAgentId], " +
                                    "[AResourceId], " +
                                    "[ANotedResourceId], " +
                                    "[A Purpose - Concept], " +
                                    "[A Purpose - Id], " +
                                    "[A Direction], " +
                                    "[A Quantity], " +
                                    "[A Has NOTED_DATE], " +
                                    "[A Is Bank_Account?], " +
                                    "[B Account], " +
                                    "[BNotedAgentId], " +
                                    "[BResourceId], " +
                                    "[BNotedResourceId], " +
                                    "[B Purpose - Concept], " +
                                    "[B Purpose - Id], " +
                                    "[B Direction], " +
                                    "[B Quantity], " +
                                    "[B Has NOTED_DATE] , " +
                                    "[B Is Bank_Account?] " +
                                "FROM [Tellma_Mapping_Remittance];";
            using (var reader = await ExecuteReaderAsync(selectQuery))
            {
                while (await reader.ReadAsync())
                {
                    remittancesList.Add(new Remittance
                    {
                        RemittanceType = reader.GetString(0),
                        RemittanceTypeName = reader.GetString(1),
                        Direction = Convert.ToSByte(reader.GetInt16(2)),
                        AAccount = reader.GetString(3),
                        ANotedAgentId = !reader.IsDBNull(4) ? reader.GetInt32(4) : null,
                        AResourceId = !reader.IsDBNull(5) ? reader.GetInt32(5) : null,
                        ANotedResourceId = !reader.IsDBNull(6) ? reader.GetInt32(6) : null,
                        APurposeConcept = reader.GetString(7),
                        APurposeId = reader.GetInt32(8),
                        ADirection = Convert.ToSByte(reader.GetByte(9)),
                        AQuantity = !reader.IsDBNull(10) ? Convert.ToByte(reader.GetBoolean(10)) : null,
                        AHasNOTEDDATE = reader.GetBoolean(11),
                        AIsBankAcc = reader.GetBoolean(12),
                        BAccount = reader.GetString(13),
                        BNotedAgentId = !reader.IsDBNull(14) ? reader.GetInt32(14) : null,
                        BResourceId = !reader.IsDBNull(15) ? reader.GetInt32(15) : null,
                        BNotedResourceId = !reader.IsDBNull(16) ? reader.GetInt32(16) : null,
                        BPurposeConcept = reader.GetString(17),
                        BPurposeId = reader.GetInt32(18),
                        BDirection = Convert.ToSByte(reader.GetInt32(19)),
                        BQuantity = !reader.IsDBNull(20) ? Convert.ToByte(reader.GetBoolean(20)) : null,
                        BHasNOTEDDATE = reader.GetBoolean(21),
                        BIsBankAcc = reader.GetBoolean(22),
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


                                