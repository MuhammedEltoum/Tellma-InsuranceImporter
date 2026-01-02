using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tellma.InsuranceImporter
{
    public static class InsuranceHelper
    {
        public static int GetTenantId(string tenantCode, Dictionary<string, string> tenants)
        {
            if (tenants.TryGetValue(tenantCode, out var tenantId))
            {
                return Convert.ToInt32(tenantId);
            }

            throw new InvalidOperationException(
                $"Tenant code '{tenantCode}' is not configured. " +
                $"Available tenants: {string.Join(", ", tenants.Keys)}");
        }
    }
}
