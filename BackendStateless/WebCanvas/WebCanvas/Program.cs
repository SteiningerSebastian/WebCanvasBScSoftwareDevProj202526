using Microsoft.Extensions.Options;
using NoredbPartitioningControllerClient;
using VeritasClient;
using VeritasClient.ServiceRegistration;
using WebCanvas.Configuration;
using WebCanvas.Hubs;
using WebCanvas.Services;

var builder = WebApplication.CreateBuilder(args);

// Configure host options to prevent BackgroundService failures from stopping the host
builder.Services.Configure<HostOptions>(options =>
{
    options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore;
});

// Configure logging for Kubernetes
builder.Logging.ClearProviders();
builder.Logging.AddSystemdConsole(options =>
{
    options.IncludeScopes = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});

// Configure log level from environment variable or appsettings
var logLevel = builder.Configuration.GetValue<string>("Logging:LogLevel:Default");
if (!string.IsNullOrEmpty(logLevel) && Enum.TryParse<LogLevel>(logLevel, true, out var level))
{
    builder.Logging.SetMinimumLevel(level);
}

// Configure service registration options
builder.Services.Configure<ServiceRegistrationOptions>(
    builder.Configuration.GetSection("ServiceRegistration"));

// Add the VeritasClient as a singleton service
builder.Services.AddSingleton<IVeritasClient>(sp =>
{
    // There are two ways to get the endpoints:
    // - During local development, we use a fixed list of endpoints
    // - In production, we use the environment variable VERITAS_ENDPOINTS
    var endpoints = new List<string>();

    var logger = sp.GetRequiredService<ILogger<ServiceRegistrationService>>();

    // Production endpoints from environment variable
    var endpointsEnv = Environment.GetEnvironmentVariable("VERITAS_NODES");

    logger.LogInformation("Configuring Veritas endpoints from environment variable VERITAS_NODES.");
    logger.LogDebug("VERITAS_NODES value: {VeritasNodes}", endpointsEnv ?? "(not set)");

    if (!string.IsNullOrEmpty(endpointsEnv))
    {
        var endpointsList = endpointsEnv.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
        logger.LogInformation("Found {Count} endpoints in VERITAS_NODES", endpointsList.Length);
        
        foreach (var endpoint in endpointsList)
        {
            var trimmedEndpoint = endpoint.Trim();
            logger.LogDebug("Adding Veritas endpoint from environment: {Endpoint}", trimmedEndpoint);
            endpoints.Add(trimmedEndpoint);
        }
    }
    else
    {
        logger.LogError("VERITAS_NODES environment variable is not set or empty! No Veritas endpoints configured.");
    }

    if (endpoints.Count == 0)
    {
        logger.LogCritical("No Veritas endpoints configured! Service registration will fail.");
        throw new InvalidOperationException("No Veritas endpoints configured. Set VERITAS_NODES environment variable.");
    }

    logger.LogInformation(
        "Creating VeritasClient with {Count} endpoint(s): [{Endpoints}] (timeout: 5s)",
        endpoints.Count,
        string.Join(", ", endpoints));

    return new VeritasClient.VeritasClient(
        endpoints.ToArray(),
        TimeSpan.FromSeconds(5),
        logDebug: msg => logger.LogDebug("[VeritasClient] {Message}", msg),
        logWarning: msg => logger.LogWarning("[VeritasClient] {Message}", msg));
});

// Add services for API controllers
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();

// Add SignalR
builder.Services.AddSignalR();

builder.Services.AddPartitioningControllerClientWithLogging();

// Add service registration handler as a singleton (needed by PeerConnectionService)
builder.Services.AddSingleton<IServiceRegistration>(sp =>
{
    var veritasClient = sp.GetRequiredService<IVeritasClient>();
    var logger = sp.GetRequiredService<ILogger<ServiceRegistrationService>>();
    var options = sp.GetRequiredService<IOptions<ServiceRegistrationOptions>>();

    logger.LogInformation("Creating ServiceRegistrationHandler for {ServiceName}.", options.Value.ServiceName);

    return new VeritasClient.ServiceRegistration.ServiceRegistrationHandler(
        veritasClient,
        $"service-{options.Value.ServiceName}",
        logError: msg => logger.LogError(msg));
});

// Add peer connection service as a hosted service (connects to other backend instances)
builder.Services.AddSingleton<IPeerConnectionService, PeerConnectionService>();
builder.Services.AddHostedService(sp => (PeerConnectionService)sp.GetRequiredService<IPeerConnectionService>());

// Add cache invalidation service as a singleton (uses peer connections)
builder.Services.AddSingleton<ICacheInvalidationService, CacheInvalidationService>();

// Add canvas cache service as a singleton
builder.Services.AddSingleton<ICanvasCacheService, CanvasCacheService>();

// Add service registration as a hosted background service
builder.Services.AddHostedService<ServiceRegistrationService>();

// Add SignalR notification bridge service
builder.Services.AddHostedService<SignalRNotificationService>();

// Add HTTP logging services
builder.Services.AddHttpLogging();

var app = builder.Build();

// Enable HTTP logging
app.UseHttpLogging();

app.UseStaticFiles();

// Map controllers
app.MapControllers();

// Map SignalR hubs
app.MapHub<CanvasHub>("/canvas");  // For end-user clients
app.MapHub<InvalidationHub>("/invalidation");  // For inter-service cache invalidation

// Health check endpoint
app.MapGet("/health", () => "WebCanvas is running.");
app.MapGet("/", () => Results.Redirect("/index.html"));

app.Run();
