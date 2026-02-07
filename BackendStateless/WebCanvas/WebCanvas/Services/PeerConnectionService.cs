using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.SignalR.Client;
using VeritasClient.ServiceRegistration;

namespace WebCanvas.Services;

public class PeerConnectionService : IPeerConnectionService, IHostedService, IDisposable
{
    private readonly IServiceRegistration _serviceRegistration;
    private readonly ILogger<PeerConnectionService> _logger;
    private readonly string _instanceId;
    private readonly ConcurrentDictionary<string, TcpClient> _peerConnections = new();
    private readonly ConcurrentDictionary<string, ServiceEndpoint> _failedEndpoints = new();
    private readonly ConcurrentBag<Func<uint, Task>> _subscribers = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ConcurrentQueue<uint> _broadcastQueue = new();
    private readonly SemaphoreSlim _broadcastSignal = new(0);
    private Task? _retryTask;
    private Task? _listeningTask;
    private Task? _broadcastTask;
    private bool _disposed = false;

    const int CONNECTION_TIMEOUT_SECONDS = 15;
    const int PORT = 5050;
    const int MTU = 1460; // 1500 -header mut be divisible by 4
    const int MAX_KEYS_PER_BATCH = MTU / 4; // Maximum keys per batch (365)
    const int BATCH_DELAY_MS = 10; // Wait up to 10ms to collect more keys

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
            await ProcessEndpointsAsync(serviceRegistration.Endpoints, new(), new());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to get initial service endpoints (error: {ErrorType}: {ErrorMessage}), will retry on updates",
                ex.GetType().Name,
                ex.Message);
        }

        // Start periodic retry task for failed connections
        _retryTask = Task.Run(() => RetryFailedConnectionsLoopAsync(_cancellationTokenSource.Token));

        _listeningTask = Task.Run(() => StartListening());

        // Start broadcast batching task
        _broadcastTask = Task.Run(() => BroadcastBatchingLoopAsync(_cancellationTokenSource.Token));
    }

    /// <summary>
    /// Begins listening for incoming peer connections on the configured TCP port and handles each connection
    /// asynchronously.
    /// </summary>
    /// <remarks>This method runs continuously until the associated cancellation token is triggered. Each
    /// accepted peer connection is processed without blocking the acceptance of new connections. Logging is performed
    /// for connection events and errors.</remarks>
    /// <returns>A task that represents the asynchronous listening operation.</returns>
    private async Task StartListening()
    {
        var listener = new TcpListener(IPAddress.Any, PORT);
        listener.Start();
        _logger.LogInformation("Listening for peer connections on port {Port}", PORT);

        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                var client = await listener.AcceptTcpClientAsync();
                _logger.LogInformation("Accepted new peer connection from {RemoteEndPoint}", client.Client.RemoteEndPoint);
                _ = HandlePeerConnectionAsync(client);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error accepting peer connection");
            }
        }
    }

    /// <summary>
    /// Handles communication with a connected peer instance. (One way: receiving invalidation messages)
    /// </summary>
    /// <param name="client">The TCP client representing the peer connection.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task HandlePeerConnectionAsync(TcpClient client)
    {
        try
        {
            using var networkStream = client.GetStream();
            while (!_cancellationTokenSource.Token.IsCancellationRequested && client.Connected)
            {
                var buffer = new byte[MTU];
                var bytesRead = await networkStream.ReadAsync(buffer, 0, buffer.Length, _cancellationTokenSource.Token);
                if (bytesRead == 0)
                {
                    _logger.LogInformation("Peer at {RemoteEndPoint} closed the connection", client.Client.RemoteEndPoint);

                    // The client is responsible for reconnecting if needed.
                    break; // Connection closed
                }

                if (bytesRead % 4 != 0)
                {
                    _logger.LogWarning("Received {BytesRead} bytes from peer at {RemoteEndPoint}, which is not a multiple of 4",
                        bytesRead, client.Client.RemoteEndPoint);
                    continue;
                }

                _logger.LogDebug("Received {BytesRead} bytes from peer at {RemoteEndPoint}", bytesRead, client.Client.RemoteEndPoint);

                // Split multiple different invalidation keys
                for (int i = 0; i < bytesRead - 3; i += 4)
                {
                    // Parse the invalidation key
                    var key = BitConverter.ToUInt32(buffer[i..(i + 4)], 0);

                    _logger.LogDebug("Received invalidation for key {Key} from instance", key);

                    // Notify subscribers
                    foreach (var subscriber in _subscribers)
                    {
                        try
                        {
                            await subscriber(key);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error notifying subscriber of invalidation for key {Key}", key);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling peer connection");
        }
        finally
        {
            client.Close();
            _logger.LogInformation("Closed peer connection from {RemoteEndPoint}", client.Client.RemoteEndPoint);
        }
    }

    /// </inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping peer connection service");
        _cancellationTokenSource.Cancel();

        // Disconnect all peer connections
        foreach(var client in _peerConnections.Values)
        {
            client.Close();
        }
    }

    /// <summary>
    /// Is called when service endpoints are updated.
    /// </summary>
    /// <param name="update">The updated service endpoints information.</param>
    /// <returns>The task representing the asynchronous operation.</returns>
    private async Task OnEndpointsUpdatedAsync(ServiceEndpointsUpdate update)
    {
        _logger.LogInformation(
            "Service endpoints updated: {AddedCount} added, {RemovedCount} removed, {UpdatedCount} updated",
            update.AddedEndpoints.Count,
            update.RemovedEndpoints.Count,
            update.UpdatedEndpoints.Count);

        _logger.LogDebug("Processing endpoint updates...");
        _logger.LogDebug("Added endpoints: {AddedEndpoints}", string.Join(", ", update.AddedEndpoints.Select(ep => ep.Id)));
        _logger.LogDebug("Removed endpoints: {RemovedEndpoints}", string.Join(", ", update.RemovedEndpoints.Select(ep => ep.Id)));
        _logger.LogDebug("Updated endpoints: {UpdatedEndpoints}", string.Join(", ", update.UpdatedEndpoints.Select(ep => ep.Id)));

        await ProcessEndpointsAsync(update.AddedEndpoints, update.RemovedEndpoints, update.UpdatedEndpoints);
    }

    private async Task ProcessEndpointsAsync(
        List<ServiceEndpoint> addedEndpoints,
        List<ServiceEndpoint> removedEndpoints,
        List<ServiceEndpoint> updatedEndpoints)
    {
        await _connectionLock.WaitAsync();
        try
        {
            // For the updated endpoints, treat them as removed then added
            removedEndpoints.AddRange(updatedEndpoints);
            addedEndpoints.AddRange(updatedEndpoints);

            // Remove connections to endpoints that are no longer available
            foreach (var endpoint in removedEndpoints)
            {
                // Remove from active connections
                if (_peerConnections.TryRemove(endpoint.Id, out var connection))
                {
                    _logger.LogInformation($"Removing peer connection to {endpoint.Id}");
                    // Disconnect the connection and dispose
                    connection.Close();
                }

                // Remove from failed endpoints list
                _failedEndpoints.TryRemove(endpoint.Id, out _);
            }

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

            // Add connections to new valid endpoints
            foreach (var endpoint in validEndpoints)
            {
                // Skip if we already have a connection
                if (_peerConnections.ContainsKey(endpoint.Id))
                {
                    continue;
                }

                _logger.LogInformation("Connecting to peer instance at {Endpoint}", endpoint.Id);
                await ConnectToPeerAsync(endpoint);
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// Connects to a peer at the specified service endpoint.
    /// </summary>
    /// <param name="serviceEndpoint">The service endpoint to connect to.</param>
    /// <returns>The task representing the asynchronous operation.</returns>
    private async Task ConnectToPeerAsync(ServiceEndpoint serviceEndpoint)
    {
        var endpoint = $"{serviceEndpoint.Address}:{PORT}";

        try
        {
            var client = new TcpClient();

            // Add timeout to the connection attempt
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token);
            cts.CancelAfter(TimeSpan.FromSeconds(CONNECTION_TIMEOUT_SECONDS));

            await client.ConnectAsync(IPEndPoint.Parse(endpoint), cts.Token);

            _peerConnections.AddOrUpdate(serviceEndpoint.Id, client, (key, oldValue) => client);

            // Remove from failed endpoints if it was there
            _failedEndpoints.TryRemove(serviceEndpoint.Id, out _);

            _logger.LogInformation("Successfully connected to peer at {Endpoint}", serviceEndpoint.Id);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Timeout connecting to peer at {Endpoint} - peer may not be ready yet, will retry later", serviceEndpoint.Id);
            _failedEndpoints[serviceEndpoint.Id] = serviceEndpoint;
        }
        catch (SocketException socketEx)
        {
            _logger.LogWarning("Cannot reach peer at {Endpoint}: {Error} (likely DNS or network issue), will retry later", serviceEndpoint.Id, socketEx.Message);
            _failedEndpoints[serviceEndpoint.Id] = serviceEndpoint;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to peer at {Endpoint}, will retry later", serviceEndpoint.Id);
            _failedEndpoints[serviceEndpoint.Id] = serviceEndpoint;
        }
    }

    /// <summary>
    /// Sometimes connections to peers fail (e.g., peer not ready yet).
    /// This method retries failed connections periodically.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token to signal the operation's cancellation.</param>
    /// <returns>The task representing the asynchronous operation.</returns>
    private async Task RetryFailedConnectionsLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting retry loop for failed peer connections");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Wait 30 seconds between retry attempts
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);

                if (_failedEndpoints.IsEmpty)
                {
                    continue;
                }

                _logger.LogInformation("Retrying {Count} failed peer connection(s)", _failedEndpoints.Count);

                var endpointsToRetry = _failedEndpoints.ToArray();

                foreach (var kvp in endpointsToRetry)
                {
                    var endpointKey = kvp.Key;
                    var endpoint = kvp.Value;

                    // Skip if already connected (might have succeeded in the meantime)
                    if (_peerConnections.ContainsKey(endpointKey))
                    {
                        _failedEndpoints.TryRemove(endpointKey, out _);
                        continue;
                    }

                    _logger.LogDebug("Retrying connection to peer at {Endpoint}", endpointKey);

                    // Try to connect
                    await ConnectToPeerAsync(endpoint);

                    // If successful, ConnectToPeerAsync will add to _peerConnections and we can remove from failed
                    if (_peerConnections.ContainsKey(endpointKey))
                    {
                        _failedEndpoints.TryRemove(endpointKey, out _);
                        _logger.LogInformation("Successfully connected to previously failed peer at {Endpoint}", endpointKey);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in retry loop for failed connections");
            }
        }

        _logger.LogDebug("Retry loop for failed peer connections has exited");
    }

    /// <inheritdoc/>
    public Task BroadcastInvalidationAsync(uint key, CancellationToken cancellationToken = default)
    {
        _broadcastQueue.Enqueue(key);
        _broadcastSignal.Release();
        _logger.LogDebug("Queued invalidation for key {Key} for batched broadcast", key);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Background task that batches invalidation keys and broadcasts them to all peers.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token to signal the operation's cancellation.</param>
    /// <returns>The task representing the asynchronous operation.</returns>
    private async Task BroadcastBatchingLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting broadcast batching loop");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Wait for at least one key to be available
                await _broadcastSignal.WaitAsync(cancellationToken);

                var batch = new List<uint>();

                // Collect the first key
                if (_broadcastQueue.TryDequeue(out var firstKey))
                {
                    batch.Add(firstKey);
                }

                // Wait a short time to collect more keys (up to BATCH_DELAY_MS)
                var deadline = DateTime.UtcNow.AddMilliseconds(BATCH_DELAY_MS);
                while (batch.Count < MAX_KEYS_PER_BATCH && DateTime.UtcNow < deadline)
                {
                    // Try to get more keys from the queue
                    if (_broadcastQueue.TryDequeue(out var key))
                    {
                        batch.Add(key);
                    }
                    else
                    {
                        // No more keys immediately available, wait a tiny bit
                        await Task.Delay(1, cancellationToken);
                    }
                }

                // Collect any remaining keys up to the batch limit (non-blocking final sweep)
                while (batch.Count < MAX_KEYS_PER_BATCH && _broadcastQueue.TryDequeue(out var key))
                {
                    batch.Add(key);
                }

                // Drain any excess semaphore signals (one signal per dequeued item)
                for (int i = 1; i < batch.Count; i++)
                {
                    if (_broadcastSignal.CurrentCount > 0)
                    {
                        _broadcastSignal.Wait(0);
                    }
                }

                if (batch.Count == 0)
                {
                    continue;
                }

                await BroadcastBatchAsync(batch, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in broadcast batching loop");
            }
        }

        _logger.LogDebug("Broadcast batching loop has exited");
    }

    /// <summary>
    /// Broadcasts a batch of invalidation keys to all connected peers.
    /// </summary>
    /// <param name="batch">The list of invalidation keys to broadcast.</param>
    /// <param name="cancellationToken">The cancellation token to signal the operation's cancellation.</param>
    /// <returns>The task representing the asynchronous operation.</returns>
    private async Task BroadcastBatchAsync(List<uint> batch, CancellationToken cancellationToken)
    {
        var connections = _peerConnections.Values.ToArray();

        if (connections.Length == 0)
        {
            _logger.LogDebug("No peer connections available for broadcasting {Count} invalidation(s)", batch.Count);
            return;
        }

        _logger.LogDebug("Broadcasting batch of {BatchSize} invalidation key(s) to {PeerCount} peer(s)", 
            batch.Count, connections.Length);

        // Convert all keys to a single byte array
        var buffer = new byte[batch.Count * 4];
        for (int i = 0; i < batch.Count; i++)
        {
            BitConverter.TryWriteBytes(buffer.AsSpan(i * 4, 4), batch[i]);
        }

        var broadcastTasks = connections
            .Where(client => client.Connected)
            .Select(async client =>
            {
                try
                {
                    await client.GetStream().WriteAsync(buffer, cancellationToken);
                    _logger.LogDebug("Sent batch of {Count} invalidation(s) to peer", batch.Count);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to broadcast batch to a peer");
                }
            });

        await Task.WhenAll(broadcastTasks);
    }

    /// <inheritdoc/>
    public void Subscribe(Func<uint, Task> onInvalidation)
    {
        if (onInvalidation == null)
        {
            throw new ArgumentNullException(nameof(onInvalidation));
        }

        _subscribers.Add(onInvalidation);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (!_cancellationTokenSource.IsCancellationRequested)
        {
            _cancellationTokenSource.Cancel();
        }

        // Wait for retry task to complete
        if (_retryTask != null)
        {
            try
            {
                _retryTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // Task was cancelled, which is expected
            }
        }

        // Dispose all peer connections
        foreach (var client in _peerConnections.Values)
        {
            client.Close();
        }

        _cancellationTokenSource.Dispose();
        _connectionLock.Dispose();
        _broadcastSignal.Dispose();
    }
}

