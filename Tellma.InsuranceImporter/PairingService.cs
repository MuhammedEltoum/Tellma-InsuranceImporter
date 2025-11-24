using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Repository;

namespace Tellma.InsuranceImporter
{
    public class PairingService : IImportService<Pairing>
    {
        private readonly ITellmaService _tellmaService;
        private readonly IWorksheetRepository<Pairing> _pairingRepository;
        private readonly IWorksheetRepository<Technical> _technicalRepository;
        private readonly IWorksheetRepository<Remittance> _remittanceRepository;
        private readonly ILogger<PairingService> _logger;

        public PairingService(ITellmaService tellmaService,
                              IWorksheetRepository<Pairing> pairingRepository,
                              IWorksheetRepository<Technical> technicalRepository,
                              IWorksheetRepository<Remittance> remittanceRepository,
                              ILogger<PairingService> logger)
        {
            _tellmaService = tellmaService;
            _pairingRepository = pairingRepository;
            _technicalRepository = technicalRepository;
            _remittanceRepository = remittanceRepository;
            _logger = logger;
        }

        public async Task Import(string tenantCode, CancellationToken cancellationToken)
        {
            var allWorksheets = (await _pairingRepository.GetWorksheets(cancellationToken)).ToList();

            var validWorksheets = ValidateWorksheets(allWorksheets);

            cancellationToken.ThrowIfCancellationRequested();

            var tenantWorks = validWorksheets.Where(t => t.TenantCode1 == tenantCode).ToList();

            try
            {
                await ProcessTenant(tenantCode, tenantWorks, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while processing tenant {TenantCode}", tenantCode);
            }
        }

        private async Task ProcessTenant(string tenantCode, List<Pairing> tenantWorks, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private IEnumerable<Pairing> ValidateWorksheets(List<Pairing> allWorksheets)
        {
            var valid = allWorksheets.ToList();

            //RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.AgentCode), "have an invalid insurance agent");
            //RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.ContractCode), "have an invalid contract");
            //RemoveIf(ref valid, w => string.IsNullOrWhiteSpace(w.BusinessMainClassCode), "have an invalid business main class");
            //RemoveIf(ref valid, w => Math.Abs(w.Direction) != 1, "have an invalid direction");

            //// Keep only worksheets with supported prefixes
            //var supportedPrefixes = new[] { "TW", "RT", "CW" };
            //var invalidTypeIds = valid.Where(t => !supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).Select(t => t.WorksheetId).Distinct().ToList();
            //if (invalidTypeIds.Any())
            //{
            //    valid = valid.Where(t => supportedPrefixes.Any(p => t.WorksheetId.StartsWith(p))).ToList();
            //    _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] have an invalid technical type.", invalidTypeIds.Count, string.Join(", ", invalidTypeIds));
            //}

            return valid;
        }

        private void RemoveIf(ref List<Pairing> list, Func<Pairing, bool> predicate, string errorMessage)
        {
            var invalid = list.Where(predicate).Select(t => t.WorksheetId).Distinct().ToList();
            if (!invalid.Any())
                return;

            list = list.Where(t => !invalid.Contains(t.WorksheetId)).ToList();
            _logger.LogError("Validation Error: ({Count}) WorksheetId [{Ids}] {Message}.", invalid.Count, string.Join(", ", invalid), errorMessage);
        }
    }
}
