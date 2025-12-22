using Microsoft.Extensions.Options;
using System.Data;
using System.Security.Principal;
using Tellma.InsuranceImporter.Contract;

namespace Tellma.InsuranceImporter.Repository
{
    public class PairingRepository : RepositoryBase, IWorksheetRepository<Pairing>
    {
        public PairingRepository(IOptions<InsuranceDBOptions> dbOptions) : base(dbOptions.Value.ConnectionString) {}

        public Task<List<Pairing>> GetMappingAccounts(CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task<List<Pairing>> GetWorksheets(string filter, CancellationToken token)
        {
            var pairingsList = new List<Pairing>();

            string selectQuery = "SELECT p.[PK]" +
                ",[PAIRING_DATE]" +
                ",[TECH_WS_ID]" +
                ",[TECH_AMOUNT]" +
                ",[TECH_CURRENCY]" +
                ",[REMIT_WS_ID]" +
                ",[REMIT_AMOUNT]" +
                ",[REMIT_CURRENCY]" +
                ",p.[TELLMA_DOCUMENT_ID]" +
                ",p.[BATCH_ID]" +
                ",[P_OBJECT_ID]" +
                ",[Bal1_OBJECT_ID]" +
                ",[WORKSHEET_ID1]" +
                ",[AGENT_CODE1]" +
                ",[AGENT_NAME1]" +
                ",[TENANT_CODE1]" +
                ",[TENANT_NAME1]" +
                ",[Bal2_OBJECT_ID]" +
                ",[WORKSHEET_ID2]" +
                ",[AGENT_CODE2]" +
                ",[AGENT_NAME2]" +
                ",[TENANT_CODE2]" +
                ",[TENANT_NAME2]" +
                ",t.[CONTRACT_CODE]" +
                ",r.[AGENT_CODE]" +
                ",r.[Direction] as 'RemitDirection'" +
                ",r.[PAYMENT_DATE]" +
                ",t.CONTRACT_CURRENCY_ID" +
                ",SUM(t.DIRECTION * t.CONTRACT_AMOUNT) as 'ContractMonetarySum'" +
                ",SUM(t.DIRECTION * t.VALUE_FC2) as 'ContractValueSum'" +
                ",t.[IS_INWARD]" +
                ",t.[DIRECTION] as 'TechDirection'" +
                ",t.[BROKER_CODE]" +
                ",t.[BUSINESS_MAIN_CLASS_CODE]" +
                ",t.[WORKSHEET_ID] as 'TechWorksheet'" +
                ",r.[WORKSHEET_ID] as 'RemitWorksheet'" +
                ",t.[AGENT_CODE]" +
                ",t.[NOTED_DATE]" +
                ",COALESCE(tmt.[B Account], '06001') as 'ACCOUNT_CODE'" +
                ",COALESCE(tmt.[B TAX Account], 0) as 'B_TAX_ACCOUNT'" +
                ",CASE tmt.[B sign] WHEN 'Debit' THEN 1 ELSE - 1 END as 'B_ACCOUNT_SIGN'" +
                ",tmt.[B Has NOTED_DATE] " +
            "FROM [Test].[dbo].[Pairing] p " +
            "left join Technicals t on (p.TECH_WS_ID = t.WORKSHEET_ID AND p.Bal2_OBJECT_ID = t.BAL_OBJECT_ID) OR (p.REMIT_WS_ID = t.WORKSHEET_ID AND p.Bal1_OBJECT_ID = t.BAL_OBJECT_ID) " +
            "left join Remittances r on (p.REMIT_WS_ID = r.WORKSHEET_ID AND p.Bal1_OBJECT_ID = r.BAL_OBJECT_ID) OR (p.TECH_WS_ID = r.WORKSHEET_ID AND p.Bal2_OBJECT_ID = r.BAL_OBJECT_ID) " +
            "left join Tellma_Mapping_Technical tmt on t.[ACCOUNT_CODE] = tmt.[SICS_Account] AND t.[IS_INWARD] = tmt.[IS_INWARD] " +
            $"WHERE {filter ?? "1=1"}" +
            "group by p.[PK]" +
                ",[PAIRING_DATE]" +
                ",[TECH_WS_ID]" +
                ",[TECH_AMOUNT]" +
                ",[TECH_CURRENCY]" +
                ",[REMIT_WS_ID]" +
                ",[REMIT_AMOUNT]" +
                ",[REMIT_CURRENCY]" +
                ",[P_OBJECT_ID]" +
                ",[Bal1_OBJECT_ID]" +
                ",[WORKSHEET_ID1]" +
                ",[AGENT_CODE1]" +
                ",[AGENT_NAME1]" +
                ",[TENANT_CODE1]" +
                ",[TENANT_NAME1]" +
                ",[Bal2_OBJECT_ID]" +
                ",[WORKSHEET_ID2]" +
                ",[AGENT_CODE2]" +
                ",[AGENT_NAME2]" +
                ",[TENANT_CODE2]" +
                ",[TENANT_NAME2]" +
                ",t.[CONTRACT_CODE]" +
                ",r.[AGENT_CODE]" +
                ",r.[Direction]" +
                ",r.[PAYMENT_DATE]" +
                ",t.[IS_INWARD]" +
                ",t.[BROKER_CODE]" +
                ",t.[BUSINESS_MAIN_CLASS_CODE]" +
                ",t.CONTRACT_CURRENCY_ID" +
                ",t.[WORKSHEET_ID]" +
                ",r.[WORKSHEET_ID]" +
                ",p.[TELLMA_DOCUMENT_ID]" +
                ",p.[BATCH_ID]" +
                ",t.[DIRECTION]" +
                ",t.[AGENT_CODE]" +
                ",t.[NOTED_DATE]" +
                ",tmt.[B sign]" +
                ",tmt.[B Account]" +
                ",tmt.[B Has NOTED_DATE]" +
                ",tmt.[B TAX Account];";

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
                        TellmaDocumentId = !reader.IsDBNull(8) ? reader.GetInt32(8) : 0,
                        BatchId = !reader.IsDBNull(9) ? reader.GetInt32(9) : 0,
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
                        TenantName2 = !reader.IsDBNull(22) ? reader.GetString(22) : String.Empty,
                        ContractCode = !reader.IsDBNull(23) ? reader.GetString(23) : String.Empty,
                        RemitInsuranceAgent = !reader.IsDBNull(24) ? reader.GetString(24) : String.Empty,
                        RemitDirection = !reader.IsDBNull(25) ? reader.GetInt16(25) : (short)0,
                        RemittancePaymentDate = !reader.IsDBNull(26) ? reader.GetDateTime(26) : DateTime.MinValue,
                        ContractCurrencyId = !reader.IsDBNull(27) ? reader.GetString(27) : String.Empty,
                        SumMonetaryValue = !reader.IsDBNull(28) ? reader.GetDecimal(28) : 0,
                        SumValue = !reader.IsDBNull(29) ? reader.GetDecimal(29) : 0,
                        TechIsInward = !reader.IsDBNull(30) ? Convert.ToBoolean(reader.GetValue(30)) : false,
                        TechDirection = !reader.IsDBNull(31) ? reader.GetInt16(31) : (short)0,
                        BrokerCode = !reader.IsDBNull(32) ? reader.GetString(32) : String.Empty,
                        BusinessMainClassCode = !reader.IsDBNull(33) ? reader.GetString(33) : String.Empty,
                        TechWorksheet = !reader.IsDBNull(34) ? reader.GetString(34) : String.Empty,
                        RemitWorksheet = !reader.IsDBNull(35) ? reader.GetString(35) : String.Empty,
                        TechInsuranceAgent = !reader.IsDBNull(36) ? reader.GetString(36) : String.Empty,
                        TechNotedDate = !reader.IsDBNull(37) ? reader.GetDateTime(37) : DateTime.MinValue,
                        AccountCode = !reader.IsDBNull(38) ? reader.GetString(38) : String.Empty,
                        BTaxAccount = !reader.IsDBNull(39) ? Convert.ToBoolean(reader.GetValue(39)) : false,
                        BDirection = !reader.IsDBNull(40) ? Convert.ToInt16(reader.GetValue(40)) : (short)0,
                        BHasNotedDate = !reader.IsDBNull(41) ? Convert.ToBoolean(reader.GetValue(41)) : false
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
                string updateStatement = "UPDATE [dbo].[Pairing] " +
                                         $"SET TELLMA_DOCUMENT_ID = {pairing.TellmaDocumentId} " +
                                      $"WHERE TENANT_CODE1 = '{tenantCode}' AND PK = {pairing.Pk};";
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
                                      $"WHERE TENANT_CODE1 = '{tenantCode}' AND PK = {pairing.Pk};";
                query.Add(updateStatement);
            }
            await ExecuteNonQueryAsync(string.Join(" ", query));
        }
    }
}
