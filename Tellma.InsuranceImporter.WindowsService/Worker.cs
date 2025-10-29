using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Configuration;
using Tellma.InsuranceImporter.Repository;

namespace Tellma.InsuranceImporter.WindowsService
{
    public class Worker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ImporterOptions _options;

        public Worker(IServiceProvider serviceProvider, IOptions<ImporterOptions> options)
        {
            _serviceProvider = serviceProvider;
            _options = options.Value;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var scope = _serviceProvider.CreateScope();
                var logger = scope.ServiceProvider.GetRequiredService<ILogger<Worker>>();
                logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                try
                {
                    var reader = scope.ServiceProvider.GetRequiredService<TellmaInsuranceImporter>();
                    await reader.ImportToTellma(stoppingToken);
                    //var reader = scope.ServiceProvider.GetRequiredService<ITellmaService>();
                    //await reader.DeleteDocumentsByDefinitionId(1303, 90, stoppingToken);
                    //await reader.DeleteDocumentsByDefinitionId(1303, 93, stoppingToken);

                    //await reader.DeleteAgentsByDefinition(1303, 103, stoppingToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unhandled Error");
                    // Don't throw. Instead, wait for period then try again
                }
                await Task.Delay(_options.PeriodInHours * 360 * 1000, stoppingToken);
            }
        }
    }
}