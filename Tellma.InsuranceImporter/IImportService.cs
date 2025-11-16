using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tellma.InsuranceImporter
{
    public  interface IImportService<T> where T : class
    {
        Task Import(string tenantCode, CancellationToken cancellationToken);
    }
}
