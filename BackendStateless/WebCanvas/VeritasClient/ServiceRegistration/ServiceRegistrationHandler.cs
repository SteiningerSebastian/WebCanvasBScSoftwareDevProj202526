// Translated from Go implementation using Claude Sonnet

using System.Text.Json;

namespace VeritasClient.ServiceRegistration;

/// <summary>
/// Implements service registration operations using Veritas as the backend.
/// </summary>
public class ServiceRegistrationHandler : IServiceRegistration, IDisposable
{
    private readonly IVeritasClient _client;
    private readonly string _serviceName;
    private readonly List<Action<ServiceRegistration>> _listeners = new();
    private readonly List<Action<ServiceEndpointsUpdate>> _endpointListeners = new();
    private ServiceRegistration? _currentRegistration;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _watchTask;
    private readonly Action<string>? _logError;
    private bool _disposed;

    /// <summary>
    /// Creates a new ServiceRegistrationHandler.
    /// </summary>
    /// <param name="client">The Veritas client to use</param>
    /// <param name="serviceName">The name of the service to manage</param>
    /// <param name="logError">Optional error logger</param>
    public ServiceRegistrationHandler(
        IVeritasClient client,
        string serviceName,
        Action<string>? logError = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _serviceName = serviceName ?? throw new ArgumentNullException(nameof(serviceName));
        _logError = logError;
        _cancellationTokenSource = new CancellationTokenSource();

        // Start watching for updates
        var (updates, errors) = _client.WatchVariablesAutoReconnect(
            new[] { _serviceName },
            _cancellationTokenSource.Token);

        _watchTask = Task.Run(async () => await HandleWatchUpdatesAsync(updates, errors));
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
                            continue;
                        }

                        var serviceRegistration = JsonSerializer.Deserialize<ServiceRegistration>(update.NewValue);
                        if (serviceRegistration == null)
                        {
                            _logError?.Invoke($"Failed to parse service registration JSON for key '{_serviceName}': {update.NewValue}");
                            continue;
                        }

                        ServiceRegistration? oldRegistration;
                        Action<ServiceRegistration>[] listenersCopy;
                        Action<ServiceEndpointsUpdate>[] endpointListenersCopy;

                        lock (_lock)
                        {
                            oldRegistration = _currentRegistration;
                            listenersCopy = _listeners.ToArray();
                            endpointListenersCopy = _endpointListeners.ToArray();
                        }

                        // Notify all listeners about the update
                        foreach (var listener in listenersCopy)
                        {
                            _ = Task.Run(() => listener(serviceRegistration));
                        }

                        // Determine added and removed endpoints
                        List<ServiceEndpoint> addedEndpoints;
                        List<ServiceEndpoint> removedEndpoints;

                        if (oldRegistration != null)
                        {
                            var oldEndpointsMap = oldRegistration.Endpoints
                                .ToDictionary(ep => $"{ep.Address}:{ep.Port}", ep => ep);

                            var newEndpointsMap = serviceRegistration.Endpoints
                                .ToDictionary(ep => $"{ep.Address}:{ep.Port}", ep => ep);

                            addedEndpoints = newEndpointsMap
                                .Where(kvp => !oldEndpointsMap.ContainsKey(kvp.Key))
                                .Select(kvp => kvp.Value)
                                .ToList();

                            removedEndpoints = oldEndpointsMap
                                .Where(kvp => !newEndpointsMap.ContainsKey(kvp.Key))
                                .Select(kvp => kvp.Value)
                                .ToList();
                        }
                        else
                        {
                            // First update: all current endpoints are "added"
                            addedEndpoints = serviceRegistration.Endpoints.ToList();
                            removedEndpoints = new List<ServiceEndpoint>();
                        }

                        // Notify endpoint listeners if there are any changes
                        if (addedEndpoints.Count > 0 || removedEndpoints.Count > 0)
                        {
                            var endpointUpdate = new ServiceEndpointsUpdate
                            {
                                AddedEndpoints = addedEndpoints,
                                RemovedEndpoints = removedEndpoints
                            };

                            foreach (var endpointListener in endpointListenersCopy)
                            {
                                _ = Task.Run(() => endpointListener(endpointUpdate));
                            }
                        }

                        lock (_lock)
                        {
                            _currentRegistration = serviceRegistration;
                        }
                    }
                    catch (JsonException ex)
                    {
                        _logError?.Invoke($"Failed to parse service registration JSON for key '{_serviceName}': {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancelled
            }
        });

        var errorTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var error in errors.WithCancellation(_cancellationTokenSource.Token))
                {
                    _logError?.Invoke($"Error watching service registration for '{_serviceName}': {error.Message}");
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancelled
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

        try
        {
            // Convert ServiceRegistration to JSON
            var jsonStr = JsonSerializer.Serialize(service);
            
            // For first-time registration (oldService is null), use empty string
            // This allows creating the key if it doesn't exist
            var oldJson = oldService != null ? JsonSerializer.Serialize(oldService) : "";

            // Use CompareAndSetVariable to update the service registration atomically
            var success = await _client.CompareAndSetVariableAsync(
                _serviceName,
                oldJson,
                jsonStr,
                cancellationToken);

            if (!success)
            {
                throw new ServiceRegistrationChangedException();
            }

            lock (_lock)
            {
                _currentRegistration = service;
            }
        }
        catch (JsonException ex)
        {
            throw new ServiceRegistrationFailedException(ex);
        }
        catch (Exception ex) when (ex is not ServiceRegistrationChangedException)
        {
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

        const int MAX_CAS_RETRIES = 10;
        var random = new Random();

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++)
        {
            // Get current service registration, or create a new one if it doesn't exist
            ServiceRegistration? current;
            try
            {
                current = await ResolveServiceAsync(cancellationToken);
            }
            catch (ServiceNotFoundException)
            {
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
            }
            else
            {
                updated = current.Clone();
            }

            // Check if endpoint already exists
            var existingIndex = updated.Endpoints.FindIndex(ep => ep.Id == endpoint.Id);
            if (existingIndex >= 0)
            {
                updated.Endpoints[existingIndex] = endpoint.Clone();
            }
            else
            {
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
                    return;
                }

                // CAS failed, retry with exponential backoff and jitter
                if (attempt < MAX_CAS_RETRIES - 1)
                {
                    var backoffMs = (int)Math.Min(1000, Math.Pow(2, attempt) * 10);
                    var jitterMs = random.Next(0, backoffMs / 2);
                    await Task.Delay(backoffMs + jitterMs, cancellationToken);
                }
            }
            catch (JsonException ex)
            {
                throw new EndpointRegistrationFailedException(ex);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new EndpointRegistrationFailedException(ex);
            }
        }

        // All retries exhausted
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
        }
    }

    public async Task<ServiceRegistration> ResolveServiceAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var jsonStr = await _client.GetVariableAsync(_serviceName, cancellationToken);
            try
            {
                // Handle empty or non-existent keys
                if (string.IsNullOrWhiteSpace(jsonStr))
                {
                    throw new ServiceNotFoundException();
                }

                var registration = JsonSerializer.Deserialize<ServiceRegistration>(jsonStr);

                if (registration == null)
                {
                    _logError?.Invoke($"Failed to parse service registration JSON for key '{_serviceName}': {jsonStr}");
                    throw new ServiceNotFoundException();
                }

                return registration;
            }
            catch (JsonException)
            {
                _logError?.Invoke($"Failed to parse service registration JSON for key '{_serviceName}': {jsonStr}");
                throw new ServiceNotFoundException();
            }
        }
        catch (Exception ex) when (ex is not ServiceNotFoundException)
        {
            _logError?.Invoke($"Error resolving service registration for '{_serviceName}': {ex.Message}");
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
        }
    }

    public async Task TryCleanupOldEndpointsAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var reg = await ResolveServiceAsync(cancellationToken);
        var oldReg = reg.Clone();

        // Filter out endpoints that have timed out
        var now = DateTime.UtcNow;
        reg.Endpoints = reg.Endpoints
            .Where(ep => ep.Timestamp.Add(timeout) > now)
            .ToList();

        // Update the service if any endpoints were removed
        if (reg.Endpoints.Count != oldReg.Endpoints.Count)
        {
            await RegisterOrUpdateServiceAsync(reg, oldReg, cancellationToken);
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

        _cancellationTokenSource.Cancel();
        
        try
        {
            _watchTask.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException)
        {
            // Ignore cancellation exceptions
        }

        _cancellationTokenSource.Dispose();
        _disposed = true;
    }
}
