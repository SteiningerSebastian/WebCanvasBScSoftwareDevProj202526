namespace WebCanvas.Services
{
    /// <summary>
    /// Service that handles distributed cache invalidation notifications using SignalR peer connections.
    /// Much faster than Veritas-based approach since cache invalidations don't need linearizability.
    /// </summary>
    public interface ICacheInvalidationService
    {
        /// <summary>
        /// Publish a cache invalidation message to all other instances.
        /// </summary>
        Task PublishInvalidationAsync(uint key, CancellationToken cancellationToken = default);

        /// <summary>
        /// Subscribe to cache invalidation messages.
        /// </summary>
        void Subscribe(Func<uint, Task> onInvalidate);
    }
}
