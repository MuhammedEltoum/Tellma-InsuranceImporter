using Microsoft.Extensions.Options;

namespace Tellma.InsuranceImporter.WindowsService
{
    public class Worker : BackgroundService, IHostedService, IDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private InsuranceOptions _options;
        private IDisposable _changeListener;

        public Worker(IServiceProvider serviceProvider, IOptionsMonitor<InsuranceOptions> options)
        {
            _serviceProvider = serviceProvider;
            _options = options.CurrentValue;
            
            _changeListener = options.OnChange(config =>
            {
                Console.WriteLine("Configuration updated in the background worker!");
                _options = config;
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            while (!stoppingToken.IsCancellationRequested)
            {
                using var scope = _serviceProvider.CreateScope();
                var logger = scope.ServiceProvider.GetRequiredService<ILogger<Worker>>();
                
                try
                {
                    var reader = scope.ServiceProvider.GetRequiredService<TellmaInsuranceImporter>();
                    await reader.ImportToTellma(stoppingToken);
                    //var reader = scope.ServiceProvider.GetRequiredService<ITellmaService>();
                    //await reader.DeleteDocumentsByDefinitionId(1303, 91, stoppingToken); //Pairing
                    //await reader.DeleteDocumentsByDefinitionId(1303, 93, stoppingToken); //Technicals
                    //await reader.DeleteDocumentsByDefinitionId(1303, 92, stoppingToken); //Remittance
                    //await reader.DeleteDocumentsByDefinitionId(1303, 90, stoppingToken); //Claims

                    //await reader.DeleteAgentsByDefinition(1303, 81, stoppingToken);    //BP
                    //await reader.DeleteAgentsByDefinition(1303, 103, stoppingToken);    //CussAcc
                    //await reader.DeleteAgentsByDefinition(1303, 102, stoppingToken);    //Contract
                    //await reader.DeleteAgentsByDefinition(1303, 100, stoppingToken);    //IA
                }
                catch (Exception ex)
                {
                    logger.LogError("Unhandled Error {ex}", ex.ToString());
                    // Don't throw. Instead, wait for period then try again
                }
                var now = DateTime.Now;
                var nextRun = new DateTime(now.Year, now.Month, now.Day, 8, 0, 0, 0);

                // If it's already past 8:00 AM today, schedule for tomorrow
                if (now > nextRun)
                {
                    nextRun = nextRun.AddDays(1);
                }

                var delay = nextRun - now;
                logger.LogDebug($"Next execution at: {nextRun}");
                await Task.Delay(delay, stoppingToken);
                //await Task.Delay(_options.PeriodInHours * 360 * 1000, stoppingToken);
            }
        }
    }
}