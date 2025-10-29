using Tellma.InsuranceImporter;
using Tellma.InsuranceImporter.Contract;
using Tellma.InsuranceImporter.Repository;
using Tellma.InsuranceImporter.WindowsService;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(config =>
    {
        config.ServiceName = "Tellma Attendance Importer";
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<Worker>(); // automatically defines Ilogger
        services.Configure<ImporterOptions>(hostContext.Configuration);
        services.Configure<InsuranceDBOptions>(hostContext.Configuration.GetSection("InsuranceDB"));
        services.Configure<TellmaOptions>(hostContext.Configuration.GetSection("Tellma"));
        services.AddScoped<IExchangeRatesRepository, ExchangeRatesRepository>();
        services.AddScoped<IWorksheetRepository<Remittance>, RemittanceRepository>();
        services.AddScoped<IWorksheetRepository<Technical>, TechnicalRepository>();
        services.AddScoped<ITellmaService,TellmaService>();
        services.AddScoped<IImportService<ExchangeRate>, ExchangeRatesService>();
        services.AddScoped<IImportService<Remittance>, RemittanceService>();
        services.AddScoped<IImportService<Technical>, TechnicalService>();
        services.AddScoped<TellmaInsuranceImporter>();
    })
    .ConfigureLogging((hostContext, loggingBuilder) =>
    {
        loggingBuilder.AddDebug();
        loggingBuilder.AddEmail(hostContext.Configuration);
    })
    .Build();

host.Run();