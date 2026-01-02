using Microsoft.Extensions.Options;
using Tellma.Utilities.EmailLogger;

namespace Tellma.InsuranceImporter.WindowsService
{
    public class Worker : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IOptionsMonitor<InsuranceOptions> _optionsMonitor;
        private readonly ILogger<Worker> _logger;
        private readonly EmailLogger _emailLogger;
        private InsuranceOptions _currentOptions;
        private readonly object _optionsLock = new object();

        public Worker(
            IServiceProvider serviceProvider,
            IOptionsMonitor<InsuranceOptions> optionsMonitor,
            ILogger<Worker> logger,
            EmailLogger emailLogger)
        {
            _serviceProvider = serviceProvider;
            _optionsMonitor = optionsMonitor;
            _logger = logger;
            _emailLogger = emailLogger;

            // Initialize current options
            _currentOptions = optionsMonitor.CurrentValue;

            // Subscribe to changes with thread-safe update
            optionsMonitor.OnChange(newOptions =>
            {
                lock (_optionsLock)
                {
                    _currentOptions = newOptions;
                    _logger.LogInformation("Configuration updated at {Time}", DateTime.Now);
                }
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RunSingleIterationAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Graceful shutdown
                    _logger.LogWarning("Worker is stopping due to cancellation request, please restart service!");
                    break;
                }
                catch (Exception ex)
                {
                    // Log but don't crash the worker
                    _logger.LogError(ex, "Unhandled error in worker iteration at {Time}", DateTime.Now);

                    // Wait before retrying to avoid tight error loops
                    await SafeDelay(TimeSpan.FromMinutes(5), stoppingToken);
                }
            }

            _logger.LogError(new TaskCanceledException("Worker is stopping due to cancellation request"), "Please restart service!");
        }

        private async Task RunSingleIterationAsync(CancellationToken stoppingToken)
        {
            // Get a snapshot of options to ensure consistency during the iteration
            InsuranceOptions optionsSnapshot;
            lock (_optionsLock)
            {
                optionsSnapshot = _currentOptions;
            }

            // Create a scope for this iteration
            using var scope = _serviceProvider.CreateScope();
            var iterationLogger = scope.ServiceProvider.GetRequiredService<ILogger<Worker>>();

            try
            {
                // Execute the import operation
                var reader = scope.ServiceProvider.GetRequiredService<TellmaInsuranceImporter>();
                await reader.ImportToTellma(stoppingToken);

                iterationLogger.LogInformation("Import completed successfully at {Time}", DateTime.Now);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                iterationLogger.LogWarning("Import was cancelled");
                throw; // Re-throw to be handled by ExecuteAsync
            }
            catch (Exception ex)
            {
                iterationLogger.LogError(ex, "Import failed at {Time}", DateTime.Now);
                // Don't throw - let the iteration complete and schedule next run
            }

            // Calculate next run time
            var now = DateTime.Now;
            var nextRun = new DateTime(
                now.Year,
                now.Month,
                now.Day,
                optionsSnapshot.Hour,
                optionsSnapshot.Minute,
                optionsSnapshot.Second,
                0);

            // If it's already past the scheduled time today, schedule for tomorrow
            if (now >= nextRun)
            {
                nextRun = nextRun.AddDays(1);
            }

            var delay = nextRun - now;

            iterationLogger.LogWarning(
                "Next execution in {DelayHours} hours and {DelayMinutes} minutes at {NextRunTime}",
                delay.Hours,
                delay.Minutes,
                nextRun);

            // Clear logs if needed
            _emailLogger.ClearLogs();

            // Wait until next scheduled time or cancellation
            await SafeDelay(delay, stoppingToken);
        }

        private async Task SafeDelay(TimeSpan delay, CancellationToken stoppingToken)
        {
            try
            {
                // Delay with cancellation support
                await Task.Delay(delay, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // This is expected during shutdown
                throw new OperationCanceledException(stoppingToken);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker stopping at: {Time}", DateTime.Now);
            await base.StopAsync(cancellationToken);
        }
    }
}