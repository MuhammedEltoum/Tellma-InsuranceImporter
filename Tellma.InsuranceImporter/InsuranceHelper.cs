using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tellma.InsuranceImporter
{
    public static class InsuranceHelper
    {
        public static int GetTenantId(string tenantCode)
        {
            return 1303;
            //return tenantCode switch
            //{
            //    "IR1" => 601,
            //    "IR160" => 602,
            //    _ => 1303
            //};
        }
    }
}
