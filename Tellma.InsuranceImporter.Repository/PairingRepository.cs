using System.Data;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public class PairingRepository : RepositoryBase, IWorksheetRepository<Pairing>
    {
        public PairingRepository(string connectionString) : base(connectionString){}

        public Task<List<Pairing>> GetMappingAccounts(CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task<List<Pairing>> GetWorksheets(CancellationToken token)
        {
            var pairingsList = new List<Pairing>();

            string selectQuery = "SELECT [PK], " +
                                        "[PAIRING_DATE], " +
                                        "[TECH_WS_ID], " +
                                        "[TECH_AMOUNT], " +
                                        "[TECH_CURRENCY], " +
                                        "[REMIT_WS_ID], " +
                                        "[REMIT_AMOUNT], " +
                                        "[REMIT_CURRENCY], " +
                                        "[TELLMA_DOCUMENT_ID], " +
                                        "[BATCH_ID], " +
                                        "[TRANSFER_TO_TELLMA], " +
                                        "[CREATION_DATE], " +
                                        "[P_OBJECT_ID], " +
                                        "[Bal1_OBJECT_ID], " +
                                        "[WORKSHEET_ID1], " +
                                        "[AGENT_CODE1], " +
                                        "[AGENT_NAME1], " +
                                        "[TENANT_CODE1], " +
                                        "[TENANT_NAME1], " +
                                        "[Bal2_OBJECT_ID], " +
                                        "[WORKSHEET_ID2], " +
                                        "[AGENT_CODE2], " +
                                        "[AGENT_NAME2], " +
                                        "[TENANT_CODE2], " +
                                        "[TENANT_NAME2], " +
                                "FROM [Test].[dbo].[Pairing] " +
                                "WHERE [TRANSFER_TO_TELLMA] = 'N'";

            using (var reader = await ExecuteReaderAsync(selectQuery))
            {
                while (await reader.ReadAsync())
                {
                    pairingsList.Add(new Pairing
                    {
                        Pk = reader.GetInt32(0),
                        PairingDate = reader.GetDateTime(1),
                        TechWsId = !reader.IsDBNull(2) ? reader.GetString(2) : String.Empty,
                        TechAmount = reader.GetDecimal(3),
                        TechCurrency = !reader.IsDBNull(4) ? reader.GetString(4) : String.Empty,
                        RemitWsId = !reader.IsDBNull(5) ? reader.GetString(5) : String.Empty,
                        RemitAmount = reader.GetDecimal(6),
                        RemitCurrency = !reader.IsDBNull(7) ? reader.GetString(7) : String.Empty,
                        TellmaDocumentId = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                        BatchId = reader.GetInt32(9),
                        PObjectId = !reader.IsDBNull(10) ? reader.GetString(10) : String.Empty,
                        Bal1ObjectId = !reader.IsDBNull(11) ? reader.GetString(11) : String.Empty,
                        WorksheetId1 = !reader.IsDBNull(12) ? reader.GetString(12) : String.Empty,
                        AgentCode1 = !reader.IsDBNull(13) ? reader.GetString(13) : String.Empty,
                        AgentName1 = !reader.IsDBNull(14) ? reader.GetString(14) : String.Empty,
                        TenantCode1 = !reader.IsDBNull(15) ? reader.GetString(15) : String.Empty,
                        TenantName1 = !reader.IsDBNull(16) ? reader.GetString(16) : String.Empty,
                        Bal2ObjectId = !reader.IsDBNull(17) ? reader.GetString(17) : String.Empty,
                        WorksheetId2 = !reader.IsDBNull(18) ? reader.GetString(18) : String.Empty,
                        AgentCode2 = !reader.IsDBNull(19) ? reader.GetString(19) : String.Empty,
                        AgentName2 = !reader.IsDBNull(20) ? reader.GetString(20) : String.Empty,
                        TenantCode2 = !reader.IsDBNull(21) ? reader.GetString(21) : String.Empty,
                        TenantName2 = !reader.IsDBNull(22) ? reader.GetString(22) : String.Empty
                    });
                }
            }
                return pairingsList;
        }

        public async Task UpdateDocumentIds(string tenantCode, IEnumerable<Pairing> worksheets, CancellationToken token)
        {
            if (!worksheets.Any())
                return;

            var query = new List<string>();
            foreach (var pairing in worksheets)
            {
                string updateStatement = "UPDATE [dbo].[Technicals] " +
                                         $"SET TELLMA_DOCUMENT_ID = {pairing.TellmaDocumentId} " +
                                      $"WHERE TENANT_CODE1 = '{tenantCode}' AND PK = '{pairing.Pk}';";
                query.Add(updateStatement);
            }

            await ExecuteNonQueryAsync(string.Join(" ", query));
        }

        public async Task UpdateImportedWorksheets(string tenantCode, IEnumerable<Pairing> worksheets, CancellationToken token)
        {
            if (!worksheets.Any())
                return;

            var query = new List<string>();
            foreach (var pairing in worksheets)
            {
                string updateStatement = "UPDATE [dbo].[Pairing] " +
                                         $"SET TRANSFER_TO_TELLMA = 'Y', " +
                                         $"IMPORT_DATE = GETDATE() " +
                                      $"WHERE TENANT_CODE1 = '{tenantCode}' AND PK = '{pairing.Pk}';";
                query.Add(updateStatement);
            }
            await ExecuteNonQueryAsync(string.Join(" ", query));
        }
    }
}
