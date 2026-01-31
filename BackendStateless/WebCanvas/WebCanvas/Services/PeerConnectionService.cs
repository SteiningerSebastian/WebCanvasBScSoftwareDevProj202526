using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR.Client;
using VeritasClient.ServiceRegistration;

namespace WebCanvas.Services;

public class PeerConnectionService : IPeerConnectionService, IHostedService, IDisposable
{
    private readonly IServiceRegistration _serviceRegistration;
    private readonly ILogger<PeerConnectionService> _logger;
    private readonly string _instanceId;
    private readonly ConcurrentDictionary<string, HubConnection> _peerConnections = new();
    private readonly ConcurrentBag<Func<uint, string, Task>> _subscribers = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private bool _disposed = false;

    public PeerConnectionService(
        IServiceRegistration serviceRegistration,
        ILogger<PeerConnectionService> logger)
    {
        _serviceRegistration = serviceRegistration ?? throw new ArgumentNullException(nameof(serviceRegistration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _instanceId = Guid.NewGuid().ToString();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting peer connection service with instance ID: {InstanceId}", _instanceId);

        // Subscribe to service endpoint updates
        _logger.LogDebug("Subscribing to service endpoint updates");
        _serviceRegistration.AddEndpointListener(update => _ = OnEndpointsUpdatedAsync(update));

        // Get initial list of endpoints and connect
        try
        {
            _logger.LogDebug("Attempting to resolve initial service endpoints...");
            var serviceRegistration = await _serviceRegistration.ResolveServiceAsync(cancellationToken);
            _logger.LogInformation(
                "Resolved initial service registration with {Count} endpoint(s)",
                serviceRegistration.Endpoints.Count);
            await ProcessEndpointsAsync(serviceRegistration.Endpoints, new List<ServiceEndpoint>());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, 
                "Failed to get initial service endpoints (error: {ErrorType}: {ErrorMessage}), will retry on updates",
                ex.GetType().Name,
                ex.Message);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping peer connection service");
        _cancellationTokenSource.Cancel();

        // Disconnect all peer connections
        var disconnectTasks = _peerConnections.Values.Select(conn => DisconnectAsync(conn));
        return Task.WhenAll(disconnectTasks);
    }

    private async Task OnEndpointsUpdatedAsync(ServiceEndpointsUpdate update)
    {
        _logger.LogInformation(
            "Service endpoints updated: {AddedCount} added, {RemovedCount} removed",
            update.AddedEndpoints.Count,
            update.RemovedEndpoints.Count);

        await ProcessEndpointsAsync(update.AddedEndpoints, update.RemovedEndpoints);
    }

    private async Task ProcessEndpointsAsync(
        List<ServiceEndpoint> addedEndpoints,
        List<ServiceEndpoint> removedEndpoints)
    {
        await _connectionLock.WaitAsync();
        try
        {
            // Filter out stale endpoints (older than 60 seconds)
            var staleThreshold = DateTime.UtcNow - TimeSpan.FromSeconds(60);
            var validEndpoints = addedEndpoints
                .Where(ep => ep.Timestamp > staleThreshold)
                .ToList();

            if (addedEndpoints.Count > validEndpoints.Count)
            {
                _logger.LogWarning(
                    "Filtered out {StaleCount} stale endpoints (older than 60 seconds)",
                    addedEndpoints.Count - validEndpoints.Count);
            }

            // Remove connections to endpoints that are no longer available
            foreach (var endpoint in removedEndpoints)
            {
                var endpointKey = GetEndpointKey(endpoint);
                if (_peerConnections.TryRemove(endpointKey, out var connection))
                {
                    _logger.LogInformation($"Removing peer connection to {endpointKey}");
                    await DisconnectAsync(connection);
                }
            }

            // Add connections to new valid endpoints
            foreach (var endpoint in validEndpoints)
            {
                var endpointKey = GetEndpointKey(endpoint);
                
                // Skip if we already have a connection
                if (_peerConnections.ContainsKey(endpointKey))
                {
                    continue;
                }

                _logger.LogInformation("Connecting to peer instance at {Endpoint}", endpointKey);
                await ConnectToPeerAsync(endpoint);
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task ConnectToPeerAsync(ServiceEndpoint endpoint)
    {
        var endpointKey = GetEndpointKey(endpoint);
        var url = $"http://{endpoint.Address}:{endpoint.Port}/invalidation";

        try
        {
            var connection = new HubConnectionBuilder()
                .WithUrl(url)
                .WithAutomaticReconnect(new[] { TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10) })
                .Build();

            // Register handler for incoming invalidations
            connection.On<uint, string>("InvalidateCache", async (key, instanceId) =>
            {
                _logger.LogDebug(
                    "Received cache invalidation from peer {InstanceId} for key {Key}",
                    instanceId,
                    key);

                // Don't process our own invalidations
                if (instanceId == _instanceId)
                {
                    return;
                }

                // Notify all subscribers
                var notifyTasks = _subscribers.Select(subscriber =>
                    Task.Run(async () =>
                    {
                        try
                        {
                            await subscriber(key, instanceId);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error in invalidation subscriber");
                        }
                    }));

                await Task.WhenAll(notifyTasks);
            });

            connection.Reconnecting += error =>
            {
                _logger.LogWarning("Reconnecting to peer at {Endpoint}: {Error}", endpointKey, error?.Message);
                return Task.CompletedTask;
            };

            connection.Reconnected += connectionId =>
            {
                _logger.LogInformation("Reconnected to peer at {Endpoint}", endpointKey);
                return Task.CompletedTask;
            };

            connection.Closed += async error =>
            {
                _logger.LogWarning("Connection closed to peer at {Endpoint}: {Error}", endpointKey, error?.Message);
                
                // Remove from our connection dictionary
                _peerConnections.TryRemove(endpointKey, out _);
            };

            // Add timeout to the connection attempt
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token);
            cts.CancelAfter(TimeSpan.FromSeconds(15));
            
            await connection.StartAsync(cts.Token);
            
            _peerConnections[endpointKey] = connection;
            
            _logger.LogInformation("Successfully connected to peer at {Endpoint}", endpointKey);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Timeout connecting to peer at {Endpoint} - peer may not be ready yet", endpointKey);
        }
        catch (HttpRequestException ex) when (ex.InnerException is System.Net.Sockets.SocketException socketEx)
        {
            _logger.LogWarning("Cannot reach peer at {Endpoint}: {Error} (likely DNS or network issue)", endpointKey, socketEx.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to peer at {Endpoint}", endpointKey);
        }
    }

    private async Task DisconnectAsync(HubConnection connection)
    {
        try
        {
            await connection.StopAsync();
            await connection.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disconnecting from peer");
        }
    }

    private static string GetEndpointKey(ServiceEndpoint endpoint)
    {
        return $"{endpoint.Address}:{endpoint.Port}";
    }

    /// <inheritdoc/>
    public async Task BroadcastInvalidationAsync(uint key, string instanceId, CancellationToken cancellationToken = default)
    {
        var connections = _peerConnections.Values.ToArray();
        
        if (connections.Length == 0)
        {
            _logger.LogDebug("No peer connections available for broadcasting invalidation");
            return;
        }

        _logger.LogDebug("Broadcasting invalidation for key {Key} to {Count} peers", key, connections.Length);

        var broadcastTasks = connections
            .Where(conn => conn.State == HubConnectionState.Connected)
            .Select(async conn =>
            {
                try
                {
                    await conn.InvokeAsync("BroadcastInvalidation", key, instanceId, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to broadcast invalidation to a peer");
                }
            });

        await Task.WhenAll(broadcastTasks);
    }

    /// <inheritdoc/>
    public void Subscribe(Func<uint, string, Task> onInvalidation)
    {
        if (onInvalidation == null)
        {
            throw new ArgumentNullException(nameof(onInvalidation));
        }

        _subscribers.Add(onInvalidation);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        if (!_cancellationTokenSource.IsCancellationRequested)
        {
            _cancellationTokenSource.Cancel();
        }
        
        var disconnectTasks = _peerConnections.Values.Select(conn => DisconnectAsync(conn));
        Task.WhenAll(disconnectTasks).Wait(TimeSpan.FromSeconds(5));

        _cancellationTokenSource.Dispose();
        _connectionLock.Dispose();
    }
}

