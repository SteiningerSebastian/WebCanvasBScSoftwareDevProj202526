using Microsoft.AspNetCore.SignalR;

namespace WebCanvas.Hubs;

/// <summary>
/// SignalR hub for inter-service cache invalidation communication between WebCanvas backend instances.
/// This hub is NOT for end-user clients - it's for backend-to-backend communication.
/// </summary>
public class InvalidationHub : Hub
{
    private readonly ILogger<InvalidationHub> _logger;

    public InvalidationHub(ILogger<InvalidationHub> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Called by other WebCanvas instances to broadcast a cache invalidation.
    /// </summary>
    /// <param name="key">The cache key to invalidate</param>
    /// <param name="instanceId">The ID of the instance that originated the change</param>
    public async Task BroadcastInvalidation(uint key, string instanceId)
    {
        _logger.LogDebug(
            "Received invalidation broadcast for key {Key} from instance {InstanceId}",
            key,
            instanceId);

        // Forward to all other connected backend instances (except the caller)
        await Clients.Others.SendAsync("InvalidateCache", key, instanceId);
    }

    public override Task OnConnectedAsync()
    {
        _logger.LogInformation(
            "Backend instance connected to InvalidationHub: {ConnectionId}",
            Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        if (exception != null)
        {
            _logger.LogWarning(
                exception,
                "Backend instance disconnected from InvalidationHub with error: {ConnectionId}",
                Context.ConnectionId);
        }
        else
        {
            _logger.LogInformation(
                "Backend instance disconnected from InvalidationHub: {ConnectionId}",
                Context.ConnectionId);
        }

        return base.OnDisconnectedAsync(exception);
    }
}
