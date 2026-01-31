namespace WebCanvas.Services
{
    /// <summary>
    /// Manages SignalR connections to peer WebCanvas instances for cache invalidation.
    /// Discovers peers via service registration and maintains connections to them.
    /// </summary>
    public interface IPeerConnectionService
    {
        /// <summary>
        /// Broadcast a cache invalidation to all connected peer instances.
        /// </summary>
        Task BroadcastInvalidationAsync(uint key, string instanceId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Subscribe to cache invalidation messages from peers.
        /// </summary>
        void Subscribe(Func<uint, string, Task> onInvalidation);
    }
}
