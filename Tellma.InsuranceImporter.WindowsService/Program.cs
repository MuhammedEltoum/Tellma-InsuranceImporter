using Tellma.InsuranceImporter;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Repository;
using Tellma.InsuranceImporter.WindowsService;
using Tellma.Utilities.EmailLogger;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(config =>
    {
        config.ServiceName = "Tellma Insurance Importer";
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<Worker>(); // automatically defines Ilogger
        services.Configure<ImporterOptions>(hostContext.Configuration);
        services.Configure<InsuranceDBOptions>(hostContext.Configuration.GetSection("InsuranceDB"));
        services.Configure<TellmaOptions>(hostContext.Configuration.GetSection("Tellma"));
        services.Configure<InsuranceOptions>(hostContext.Configuration.GetSection("Insurance"));
        services.Configure<EmailOptions>(hostContext.Configuration.GetSection("Email"));
        services.AddSingleton<EmailLogger>();
        services.AddScoped<IExchangeRatesRepository, ExchangeRatesRepository>();
        services.AddScoped<IWorksheetRepository<Remittance>, RemittanceRepository>();
        services.AddScoped<IWorksheetRepository<Technical>, TechnicalRepository>();
        services.AddScoped<IWorksheetRepository<Pairing>, PairingRepository>();
        services.AddScoped<ITellmaService,TellmaService>();
        services.AddScoped<IImportService<ExchangeRate>, ExchangeRatesService>();
        services.AddScoped<IImportService<Remittance>, RemittanceService>();
        services.AddScoped<IImportService<Technical>, TechnicalService>();
        services.AddScoped<IImportService<Pairing>, PairingService>();
        services.AddScoped<TellmaInsuranceImporter>();
    })
    .ConfigureLogging((hostContext, loggingBuilder) =>
    {
        // Clear default providers if needed
        loggingBuilder.ClearProviders();

        // Add EventLog with proper configuration
        loggingBuilder.AddEventLog(eventLogSettings =>
        {
            eventLogSettings.SourceName = "Tellma Insurance Importer";
            eventLogSettings.LogName = "Application";
            // Optional: Filter by log level
            eventLogSettings.Filter = (source, level) =>
                level >= LogLevel.Information; // Adjust as needed
        });
        loggingBuilder.AddDebug();
        loggingBuilder.AddEmail(hostContext.Configuration);
    })
    .Build();

host.Run();