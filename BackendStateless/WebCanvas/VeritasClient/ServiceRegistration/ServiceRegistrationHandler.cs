// Translated from Go implementation using Claude Sonnet

using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Implements service registration operations using Veritas as the backend.
/// </summary>
public class ServiceRegistrationHandler : IServiceRegistrationHandler, IDisposable
{
    private readonly IVeritasClient _client;
    private readonly string _serviceName;
    private readonly List<Action<ServiceRegistration>> _listeners = new();
    private readonly List<Action<ServiceEndpointsUpdate>> _endpointListeners = new();
    private ServiceRegistration? _currentRegistration;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ILogger<ServiceRegistrationHandler> _logger;
    private bool _disposed;

    /// <summary>
    /// Creates a new ServiceRegistrationHandler.
    /// </summary>
    /// <param name="client">The Veritas client to use</param>
    /// <param name="serviceName">The name of the service to manage</param>
    /// <param name="logger">Logger instance</param>
    public ServiceRegistrationHandler(
        IVeritasClient client,
        string serviceName,
        ILogger<ServiceRegistrationHandler> logger)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _serviceName = serviceName ?? throw new ArgumentNullException(nameof(serviceName));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _cancellationTokenSource = new CancellationTokenSource();

        _logger.LogInformation("Initializing ServiceRegistrationHandler for service: {ServiceName}", _serviceName);

        // Start watching for updates
        var (updates, errors) = _client.WatchVariablesAutoReconnect(
            new[] { _serviceName },
            _cancellationTokenSource.Token);

        // Handle updates and errors in the background
        var _ = HandleWatchUpdatesAsync(updates, errors);

        _logger.LogDebug("ServiceRegistrationHandler watch task started for service: {ServiceName}", _serviceName);
    }

    private async Task HandleWatchUpdatesAsync(
        IAsyncEnumerable<Models.UpdateNotification> updates,
        IAsyncEnumerable<Exception> errors)
    {
        var updateTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var update in updates.WithCancellation(_cancellationTokenSource.Token))
                {
                    try
                    {
                        // Skip empty updates (service not registered yet)
                        if (string.IsNullOrWhiteSpace(update.NewValue))
                        {
                            _logger.LogDebug("Received empty service registration for key '{ServiceName}', skipping", _serviceName);
                            continue;
                        }

                        _logger.LogTrace("Processing service registration update for '{ServiceName}'", _serviceName);
                        var serviceRegistration = JsonSerializer.Deserialize<ServiceRegistration>(update.NewValue);
                        if (serviceRegistration == null)
                        {
                            _logger.LogError("Failed to parse service registration JSON for key '{ServiceName}': {Json}",
                                _serviceName, update.NewValue);
                            continue;
                        }

                        ServiceRegistration? oldRegistration;
                        Action<ServiceRegistration>[] listenersCopy;
                        Action<ServiceEndpointsUpdate>[] endpointListenersCopy;

                        lock (_lock)
                        {
                            oldRegistration = _currentRegistration;
                            _currentRegistration = serviceRegistration;
                            listenersCopy = _listeners.ToArray();
                            endpointListenersCopy = _endpointListeners.ToArray();
                        }

                        _logger.LogDebug("Updated service registration for '{ServiceName}'. Endpoint count: {EndpointCount}, Listener count: {ListenerCount}, Endpoint listener count: {EndpointListenerCount}",
                            _serviceName, serviceRegistration.Endpoints.Count, listenersCopy.Length, endpointListenersCopy.Length);

                        // Notify all listeners about the update
                        foreach (var listener in listenersCopy)
                        {
                            _ = Task.Run(() => listener(serviceRegistration));
                        }

                        // Determine added and removed endpoints
                        List<ServiceEndpoint> addedEndpoints;
                        List<ServiceEndpoint> removedEndpoints;
                        List<ServiceEndpoint> updatedEndpoints;

                        if (oldRegistration != null)
                        {
                            var oldEndpointsMap = oldRegistration.Endpoints
                                .ToDictionary(ep => ep.Id, ep => ep);

                            var newEndpointsMap = serviceRegistration.Endpoints
                                .ToDictionary(ep => ep.Id, ep => ep);

                            addedEndpoints = newEndpointsMap
                                .Where(kvp => !oldEndpointsMap.ContainsKey(kvp.Key))
                                .Select(kvp => kvp.Value)
                                .ToList();

                            removedEndpoints = oldEndpointsMap
                                .Where(kvp => !newEndpointsMap.ContainsKey(kvp.Key))
                                .Select(kvp => kvp.Value)
                                .ToList();

                            updatedEndpoints = newEndpointsMap.Where(kvp =>
                                oldEndpointsMap.ContainsKey(kvp.Key) &&
                                !kvp.Value.Equals(oldEndpointsMap[kvp.Key]))
                                .Select(kvp => kvp.Value)
                                .ToList();
                        }
                        else
                        {
                            // First update: all current endpoints are "added"
                            addedEndpoints = serviceRegistration.Endpoints.ToList();
                            removedEndpoints = new List<ServiceEndpoint>();
                            updatedEndpoints = new List<ServiceEndpoint>();
                        }

                        // Notify endpoint listeners if there are any changes
                        if (addedEndpoints.Count > 0 || removedEndpoints.Count > 0)
                        {
                            _logger.LogInformation("Service '{ServiceName}' endpoints changed. Added: {AddedCount}, Removed: {RemovedCount}, Updated: {UpdatedCount}",
                                _serviceName, addedEndpoints.Count, removedEndpoints.Count, updatedEndpoints.Count);
                            
                            var endpointUpdate = new ServiceEndpointsUpdate
                            {
                                AddedEndpoints = addedEndpoints,
                                RemovedEndpoints = removedEndpoints,
                                UpdatedEndpoints = updatedEndpoints
                            };

                            foreach (var endpointListener in endpointListenersCopy)
                            {
                                _ = Task.Run(() => endpointListener(endpointUpdate));
                            }
                        }
                        else if (updatedEndpoints.Count > 0)
                        {
                            _logger.LogDebug("Service '{ServiceName}' has {UpdatedCount} updated endpoints",
                                _serviceName, updatedEndpoints.Count);
                        }
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogError(ex, "Failed to parse service registration JSON for key '{ServiceName}'", _serviceName);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error processing service registration update for '{ServiceName}'", _serviceName);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Update handling task cancelled for service '{ServiceName}'", _serviceName);
            }
        });

        var errorTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var error in errors.WithCancellation(_cancellationTokenSource.Token))
                {
                    _logger.LogError(error, "Error watching service registration for '{ServiceName}'", _serviceName);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogDebug("Error task cancelled for service '{ServiceName}'", _serviceName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in error handling task for service '{ServiceName}'", _serviceName);
            }
        });

        await Task.WhenAll(updateTask, errorTask);
    }

    public async Task RegisterOrUpdateServiceAsync(
        ServiceRegistration service,
        ServiceRegistration? oldService = null,
        CancellationToken cancellationToken = default)
    {
        if (service == null)
        {
            throw new ArgumentNullException(nameof(service));
        }

        _logger.LogInformation("Registering or updating service '{ServiceName}' with {EndpointCount} endpoints",
            _serviceName, service.Endpoints.Count);

        try
        {
            // Convert ServiceRegistration to JSON
            var jsonStr = JsonSerializer.Serialize(service);
            
            // For first-time registration (oldService is null), use empty string
            // This allows creating the key if it doesn't exist
            var oldJson = oldService != null ? JsonSerializer.Serialize(oldService) : "";

            _logger.LogDebug("Attempting compare-and-set for service '{ServiceName}'. IsFirstRegistration: {IsFirst}",
                _serviceName, oldService == null);

            // Use CompareAndSetVariable to update the service registration atomically
            var success = await _client.CompareAndSetVariableAsync(
                _serviceName,
                oldJson,
                jsonStr,
                cancellationToken);

            if (!success)
            {
                _logger.LogWarning("Service registration compare-and-set failed for '{ServiceName}' - service was modified concurrently", _serviceName);
                throw new ServiceRegistrationChangedException();
            }

            lock (_lock)
            {
                _currentRegistration = service;
            }
            
            _logger.LogInformation("Successfully registered/updated service '{ServiceName}'", _serviceName);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Failed to serialize service registration for '{ServiceName}'", _serviceName);
            throw new ServiceRegistrationFailedException(ex);
        }
        catch (Exception ex) when (ex is not ServiceRegistrationChangedException)
        {
            _logger.LogError(ex, "Failed to register/update service '{ServiceName}'", _serviceName);
            throw new ServiceRegistrationFailedException(ex);
        }
    }

    public async Task RegisterOrUpdateEndpointAsync(
        ServiceEndpoint endpoint,
        CancellationToken cancellationToken = default)
    {
        if (endpoint == null)
        {
            throw new ArgumentNullException(nameof(endpoint));
        }

        _logger.LogInformation("Registering or updating endpoint '{EndpointId}' for service '{ServiceName}'",
            endpoint.Id, _serviceName);

        const int MAX_CAS_RETRIES = 10;
        var random = new Random();

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++)
        {
            _logger.LogTrace("Endpoint registration attempt {Attempt}/{MaxAttempts} for endpoint '{EndpointId}'",
                attempt + 1, MAX_CAS_RETRIES, endpoint.Id);

            // Get current service registration, or create a new one if it doesn't exist
            ServiceRegistration? current;
            try
            {
                current = await ResolveServiceAsync(cancellationToken);
                _logger.LogTrace("Retrieved current service registration with {EndpointCount} endpoints",
                    current.Endpoints.Count);
            }
            catch (ServiceNotFoundException)
            {
                _logger.LogDebug("Service '{ServiceName}' doesn't exist yet, will create new registration", _serviceName);
                // Service doesn't exist yet, create a new registration
                current = null;
            }

            // Create updated service registration
            ServiceRegistration updated;
            if (current == null)
            {
                updated = new ServiceRegistration
                {
                    Id = _serviceName,
                    Endpoints = new List<ServiceEndpoint>(),
                    Meta = new Dictionary<string, string>()
                };
                _logger.LogDebug("Created new service registration for '{ServiceName}'", _serviceName);
            }
            else
            {
                updated = current.Clone();
            }

            // Check if endpoint already exists
            var existingIndex = updated.Endpoints.FindIndex(ep => ep.Id == endpoint.Id);
            if (existingIndex >= 0)
            {
                _logger.LogDebug("Updating existing endpoint '{EndpointId}' at index {Index}", endpoint.Id, existingIndex);
                updated.Endpoints[existingIndex] = endpoint.Clone();
            }
            else
            {
                _logger.LogDebug("Adding new endpoint '{EndpointId}' to service '{ServiceName}'", endpoint.Id, _serviceName);
                updated.Endpoints.Add(endpoint.Clone());
            }

            try
            {
                // Convert to JSON
                var jsonStr = JsonSerializer.Serialize(updated);
                var oldJson = current is null ? "" : JsonSerializer.Serialize(current);

                // Use CompareAndSetVariable to update the service registration atomically
                var success = await _client.CompareAndSetVariableAsync(
                    _serviceName,
                    oldJson,
                    jsonStr,
                    cancellationToken);

                if (success)
                {
                    // Success! Update local cache and return
                    lock (_lock)
                    {
                        _currentRegistration = updated;
                    }
                    _logger.LogInformation("Successfully registered/updated endpoint '{EndpointId}' for service '{ServiceName}' on attempt {Attempt}",
                        endpoint.Id, _serviceName, attempt + 1);
                    return;
                }

                // CAS failed, retry with exponential backoff and jitter
                if (attempt < MAX_CAS_RETRIES - 1)
                {
                    var backoffMs = (int)Math.Min(1000, Math.Pow(2, attempt) * 10);
                    var jitterMs = random.Next(0, backoffMs / 2);
                    var totalDelayMs = backoffMs + jitterMs;
                    _logger.LogDebug("Endpoint registration CAS failed for '{EndpointId}', retrying after {DelayMs}ms",
                        endpoint.Id, totalDelayMs);
                    await Task.Delay(totalDelayMs, cancellationToken);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Failed to serialize service registration for endpoint '{EndpointId}'", endpoint.Id);
                throw new EndpointRegistrationFailedException(ex);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Failed to register endpoint '{EndpointId}' on attempt {Attempt}",
                    endpoint.Id, attempt + 1);
                throw new EndpointRegistrationFailedException(ex);
            }
        }

        // All retries exhausted
        _logger.LogError("Failed to register endpoint '{EndpointId}' after {MaxRetries} attempts",
            endpoint.Id, MAX_CAS_RETRIES);
        throw new ServiceRegistrationChangedException();
    }

    public void AddListener(Action<ServiceRegistration> callback)
    {
        if (callback == null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        lock (_lock)
        {
            _listeners.Add(callback);
            _logger.LogDebug("Added service registration listener for '{ServiceName}'. Total listeners: {ListenerCount}",
                _serviceName, _listeners.Count);
        }
    }

    public async Task<ServiceRegistration> ResolveServiceAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Resolving service registration for '{ServiceName}'", _serviceName);
        try
        {
            var jsonStr = await _client.GetVariableAsync(_serviceName, cancellationToken);
            try
            {
                // Handle empty or non-existent keys
                if (string.IsNullOrWhiteSpace(jsonStr))
                {
                    _logger.LogWarning("Service '{ServiceName}' not found (empty response)", _serviceName);
                    throw new ServiceNotFoundException();
                }

                var registration = JsonSerializer.Deserialize<ServiceRegistration>(jsonStr);

                if (registration == null)
                {
                    _logger.LogError("Failed to parse service registration JSON for key '{ServiceName}': {Json}",
                        _serviceName, jsonStr);
                    throw new ServiceNotFoundException();
                }

                _logger.LogDebug("Successfully resolved service '{ServiceName}' with {EndpointCount} endpoints",
                    _serviceName, registration.Endpoints.Count);
                return registration;
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Failed to parse service registration JSON for key '{ServiceName}': {Json}",
                    _serviceName, jsonStr);
                throw new ServiceNotFoundException();
            }
        }
        catch (Exception ex) when (ex is not ServiceNotFoundException)
        {
            _logger.LogError(ex, "Error resolving service registration for '{ServiceName}'", _serviceName);
            throw new ServiceNotFoundException();
        }
    }

    public void AddEndpointListener(Action<ServiceEndpointsUpdate> callback)
    {
        if (callback == null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        lock (_lock)
        {
            _endpointListeners.Add(callback);
            _logger.LogDebug("Added endpoint listener for '{ServiceName}'. Total endpoint listeners: {ListenerCount}",
                _serviceName, _endpointListeners.Count);
        }
    }

    public async Task TryCleanupOldEndpointsAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _logger.LogDebug("Attempting to cleanup old endpoints for service '{ServiceName}' with timeout {TimeoutSeconds}s",
            _serviceName, timeout.TotalSeconds);
        
        var reg = await ResolveServiceAsync(cancellationToken);
        var oldReg = reg.Clone();

        // Filter out endpoints that have timed out
        var now = DateTime.UtcNow;
        reg.Endpoints = reg.Endpoints
            .Where(ep => ep.Timestamp.Add(timeout) > now)
            .ToList();

        var removedCount = oldReg.Endpoints.Count - reg.Endpoints.Count;

        // Update the service if any endpoints were removed
        if (removedCount > 0)
        {
            _logger.LogInformation("Cleaning up {RemovedCount} old endpoints from service '{ServiceName}'",
                removedCount, _serviceName);
            await RegisterOrUpdateServiceAsync(reg, oldReg, cancellationToken);
        }
        else
        {
            _logger.LogDebug("No old endpoints to cleanup for service '{ServiceName}'", _serviceName);
        }
    }

    /// <summary>
    /// Gets the current cached registration (may be stale).
    /// </summary>
    public ServiceRegistration? GetCurrentRegistration()
    {
        lock (_lock)
        {
            return _currentRegistration?.Clone();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _logger.LogInformation("Disposing ServiceRegistrationHandler for service '{ServiceName}'", _serviceName);
        _cancellationTokenSource.Cancel();
       
        _cancellationTokenSource.Dispose();
        _disposed = true;
        _logger.LogDebug("ServiceRegistrationHandler disposed for service '{ServiceName}'", _serviceName);
    }
}
