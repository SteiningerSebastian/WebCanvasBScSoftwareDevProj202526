using System.Collections.Concurrent;

namespace WebCanvas.Services;

public class CacheInvalidationService : ICacheInvalidationService
{
    private readonly IPeerConnectionService _peerConnectionService;
    private readonly ILogger<CacheInvalidationService> _logger;
    private readonly string _instanceId;
    private readonly ConcurrentBag<Func<uint, Task>> _subscribers = new();

    public CacheInvalidationService(
        IPeerConnectionService peerConnectionService,
        ILogger<CacheInvalidationService> logger)
    {
        _peerConnectionService = peerConnectionService ?? throw new ArgumentNullException(nameof(peerConnectionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _instanceId = Guid.NewGuid().ToString();

        // Subscribe to invalidation messages from peer connections
        _peerConnectionService.Subscribe(OnPeerInvalidationAsync);
    }

    private async Task OnPeerInvalidationAsync(uint key, string instanceId)
    {
        // Ignore messages from this instance (shouldn't happen but extra safety)
        if (instanceId == _instanceId)
        {
            return;
        }

        _logger.LogDebug(
            "Received cache invalidation for key {Key} from instance {InstanceId}",
            key,
            instanceId);

        // Notify all local subscribers
        var notifyTasks = _subscribers.Select(subscriber =>
            Task.Run(async () =>
            {
                try
                {
                    await subscriber(key);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in cache invalidation subscriber");
                }
            }));

        await Task.WhenAll(notifyTasks);
    }

    public async Task PublishInvalidationAsync(uint key, CancellationToken cancellationToken = default)
    {
        try
        {
            // Broadcast to all peer instances via SignalR (fast!)
            await _peerConnectionService.BroadcastInvalidationAsync(key, _instanceId, cancellationToken);
            
            _logger.LogDebug("Published cache invalidation for key {Key} to peers", key);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish cache invalidation for key {Key}", key);
            // Don't throw - cache invalidation is best-effort
            // Worst case: some instances have stale cache until next update
        }
    }

    public void Subscribe(Func<uint, Task> onInvalidate)
    {
        if (onInvalidate == null)
        {
            throw new ArgumentNullException(nameof(onInvalidate));
        }

        _subscribers.Add(onInvalidate);
    }
}

