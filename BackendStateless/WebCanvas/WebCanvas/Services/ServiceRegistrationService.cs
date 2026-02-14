using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Options;
using VeritasClient;
using VeritasClient.ServiceRegistration;
using WebCanvas.Configuration;

namespace WebCanvas.Services;

/// <summary>
/// Background service that manages the lifecycle of service registration with Veritas.
/// Registers the WebCanvas instance on startup, periodically updates the endpoint to keep it alive,
/// and handles graceful shutdown.
/// </summary>
public class ServiceRegistrationService : BackgroundService
{
    private readonly IVeritasClient _veritasClient;
    private readonly ServiceRegistrationOptions _options;
    private readonly ILogger<ServiceRegistrationService> _logger;
    private readonly IHostApplicationLifetime _lifetime;
    private IServiceRegistrationHandler? _registrationHandler;
    private ServiceEndpoint? _currentEndpoint;
    private string? _serverAddress;
    private int _serverPort;

    public ServiceRegistrationService(
        IVeritasClient veritasClient,
        IOptions<ServiceRegistrationOptions> options,
        ILogger<ServiceRegistrationService> logger,
        IHostApplicationLifetime lifetime,
        IServiceRegistrationHandler serviceRegistration,
        IConfiguration configuration)
    {
        _veritasClient = veritasClient ?? throw new ArgumentNullException(nameof(veritasClient));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));
        _registrationHandler = serviceRegistration ?? throw new ArgumentNullException(nameof(serviceRegistration));

        // Extract port from configuration
        var urls = configuration.GetValue<string>("ASPNETCORE_URLS") 
                   ?? configuration.GetValue<string>("urls") 
                   ?? "http://localhost:5272";
        
        _serverPort = ExtractPortFromUrl(urls);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation("ServiceRegistrationService starting up...");
            
            // Wait for the application to be fully started
            _logger.LogDebug("Waiting for application to start...");
            await WaitForApplicationStartAsync(stoppingToken);
            _logger.LogDebug("Application started, proceeding with service registration");

            // Initialize service registration handler
            _logger.LogDebug(
                "Creating ServiceRegistrationHandler for {ServiceName}.",
                _options.ServiceName);

            // Determine the endpoint address
            _logger.LogDebug("Determining endpoint address...");
            _serverAddress = await GetEndpointAddressAsync();
            
            // Generate endpoint ID if not specified
            var endpointId = string.IsNullOrWhiteSpace(_options.EndpointId)
                ? Guid.NewGuid().ToString()
                : _options.EndpointId;

            _currentEndpoint = new ServiceEndpoint
            {
                Id = endpointId,
                Address = _serverAddress,
                Port = (ushort)_serverPort,
                Timestamp = DateTime.UtcNow
            };

            _logger.LogInformation(
                "Registering WebCanvas endpoint: {EndpointId} at {Address}:{Port} (Detected port from config: {ConfigPort})",
                _currentEndpoint.Id,
                _currentEndpoint.Address,
                _currentEndpoint.Port,
                _serverPort);

            // Initial registration with retry logic
            _logger.LogDebug("Starting initial endpoint registration with retry logic...");
            await RegisterEndpointWithRetryAsync(stoppingToken);

            _logger.LogInformation("Service registration completed successfully");

            // Start keep-alive loop
            _logger.LogDebug("Starting keep-alive loop...");
            await KeepAliveLoopAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
            _logger.LogInformation("Service registration service is shutting down");
        }
        catch (Exception ex)
        {
            // Log the error but don't crash the application
            // The service will continue to retry via the keep-alive loop
            _logger.LogError(ex, "Critical error in service registration service. Service will continue running.");
        }
    }

    private async Task WaitForApplicationStartAsync(CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<bool>();
        
        using var registration = _lifetime.ApplicationStarted.Register(() => tcs.TrySetResult(true));
        using var cancelRegistration = cancellationToken.Register(() => tcs.TrySetCanceled());
        
        await tcs.Task;
    }

    private async Task<string> GetEndpointAddressAsync()
    {
        if (_options.UsePodHostname)
        {
            _logger.LogDebug("UsePodHostname is enabled, checking for Kubernetes environment");
            
            // In Kubernetes, use the pod IP address for direct pod-to-pod communication
            var podIp = Environment.GetEnvironmentVariable("POD_IP");
            var podName = Environment.GetEnvironmentVariable("POD_NAME");
            
            _logger.LogDebug(
                "Environment variables: POD_IP={PodIp}, POD_NAME={PodName}",
                podIp ?? "(not set)",
                podName ?? "(not set)");
            
            if (!string.IsNullOrWhiteSpace(podIp))
            {
                _logger.LogInformation(
                    "Using Kubernetes Pod IP: {PodIp} (pod name: {PodName})",
                    podIp,
                    podName ?? "unknown");
                return podIp;
            }
            
            _logger.LogWarning("POD_IP environment variable not found, falling back to local IP detection");
            
            // Fallback: try to detect local IP
            try
            {
                var localIp = GetLocalIpAddress();
                if (localIp != null)
                {
                    _logger.LogInformation("Using detected local IP: {LocalIp}", localIp);
                    return localIp;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to detect local IP address");
            }
        }
        else
        {
            _logger.LogDebug("UsePodHostname is disabled, using local hostname");
        }

        // Final fallback to local hostname
        var localHostname = System.Net.Dns.GetHostName();
        _logger.LogInformation("Using hostname: {Hostname}", localHostname);
        return localHostname;
    }

    private string? GetLocalIpAddress()
    {
        try
        {
            var host = System.Net.Dns.GetHostEntry(System.Net.Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
        }
        catch
        {
            // Ignore errors
        }
        return null;
    }

    private int ExtractPortFromUrl(string urls)
    {
        // Parse the first URL to extract the port
        var firstUrl = urls.Split(';').FirstOrDefault() ?? urls;
        
        if (Uri.TryCreate(firstUrl, UriKind.Absolute, out var uri))
        {
            if (uri.Port > 0)
            {
                return uri.Port;
            }
            
            // Use default ports if not specified
            return uri.Scheme.ToLowerInvariant() == "https" ? 443 : 80;
        }

        // Check for environment variable that might override
        var envPort = Environment.GetEnvironmentVariable("ASPNETCORE_HTTP_PORTS");
        if (!string.IsNullOrWhiteSpace(envPort) && int.TryParse(envPort, out var port))
        {
            return port;
        }

        // Default to 8080 for containers, 5272 for local development
        return _options.UsePodHostname ? 8080 : 8765;
    }

    private async Task RegisterEndpointAsync(CancellationToken cancellationToken)
    {
        if (_registrationHandler == null || _currentEndpoint == null)
        {
            throw new InvalidOperationException("Registration handler or endpoint not initialized");
        }

        try
        {
            _logger.LogDebug(
                "Attempting to register endpoint {EndpointId} at {Address}:{Port}",
                _currentEndpoint.Id,
                _currentEndpoint.Address,
                _currentEndpoint.Port);

            // Update timestamp
            _currentEndpoint.Timestamp = DateTime.UtcNow;

            // Register or update the endpoint
            await _registrationHandler.RegisterOrUpdateEndpointAsync(_currentEndpoint, cancellationToken);

            _logger.LogInformation(
                "Successfully registered endpoint {EndpointId} at {Address}:{Port}",
                _currentEndpoint.Id,
                _currentEndpoint.Address,
                _currentEndpoint.Port);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Failed to register endpoint {EndpointId} at {Address}:{Port}. Error type: {ErrorType}, Message: {ErrorMessage}",
                _currentEndpoint.Id,
                _currentEndpoint.Address,
                _currentEndpoint.Port,
                ex.GetType().Name,
                ex.Message);
            throw;
        }
    }

    private async Task RegisterEndpointWithRetryAsync(CancellationToken cancellationToken)
    {
        const int MAX_RETRIES = 5;
        var retryDelay = TimeSpan.FromSeconds(2);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++)
        {
            try
            {
                await RegisterEndpointAsync(cancellationToken);
                return; // Success
            }
            catch (Exception ex) when (attempt < MAX_RETRIES)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to register endpoint (attempt {Attempt}/{MaxRetries}). Retrying in {Delay} seconds...",
                    attempt,
                    MAX_RETRIES,
                    retryDelay.TotalSeconds);

                await Task.Delay(retryDelay, cancellationToken);
                retryDelay = TimeSpan.FromSeconds(retryDelay.TotalSeconds * 2); // Exponential backoff
            }
            catch (Exception ex)
            {
                // Last attempt failed
                _logger.LogError(
                    ex,
                    "Failed to register endpoint after {MaxRetries} attempts. Will retry during keep-alive loop.",
                    MAX_RETRIES);
                // Don't throw - let the keep-alive loop retry
                return;
            }
        }
    }

    private async Task KeepAliveLoopAsync(CancellationToken cancellationToken)
    {
        var updateInterval = TimeSpan.FromSeconds(_options.UpdateIntervalSeconds);
        
        _logger.LogInformation(
            "Starting keep-alive loop with interval: {Interval} seconds",
            _options.UpdateIntervalSeconds);

        int iterationCount = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                iterationCount++;
                _logger.LogDebug("Keep-alive loop iteration {Iteration} starting", iterationCount);
                
                var task = _registrationHandler?.TryCleanupOldEndpointsAsync(TimeSpan.FromSeconds(_options.UpdateIntervalSeconds*5), cancellationToken);
                if(task is not null)
                {
                    _logger.LogDebug("Running cleanup of old endpoints (timeout: {Timeout}s)", _options.UpdateIntervalSeconds * 5);
                    await task;
                }

                _logger.LogDebug("Waiting {Delay}s before next keep-alive update...", updateInterval.TotalSeconds);
                await Task.Delay(updateInterval, cancellationToken);
                
                if (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug("Sending keep-alive update for endpoint {EndpointId}", _currentEndpoint?.Id);
                    await RegisterEndpointAsync(cancellationToken);
                    _logger.LogDebug("Keep-alive update sent successfully for endpoint {EndpointId}", _currentEndpoint?.Id);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
                _logger.LogDebug("Keep-alive loop cancelled");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, 
                    "Error during keep-alive update iteration {Iteration}: {ErrorType}: {ErrorMessage}. Will retry on next interval.",
                    iterationCount,
                    ex.GetType().Name,
                    ex.Message);
                // Continue the loop to retry on next interval
            }
        }
        
        _logger.LogInformation("Keep-alive loop has exited");
    }

    public override void Dispose()
    {
        _logger.LogInformation("Disposing service registration service");
                
        base.Dispose();
    }
}
