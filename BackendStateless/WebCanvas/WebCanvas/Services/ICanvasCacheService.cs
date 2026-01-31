using WebCanvas.Models;

namespace WebCanvas.Services
{
    /// <summary>
    /// Service that provides write-through caching for canvas pixels.
    /// Coordinates with the partitioning controller and handles distributed cache invalidation.
    /// </summary>
    public interface ICanvasCacheService
    {
        /// <summary>
        /// Set a pixel value (write-through to partitioning controller).
        /// </summary>
        Task SetPixelAsync(uint x, uint y, RGBColor color, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get a pixel value (from cache).
        /// </summary>
        Task<Pixel?> GetPixelAsync(uint x, uint y, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get all pixels from the cache.
        /// </summary>
        IEnumerable<Pixel> GetAllPixels();

        /// <summary>
        /// Wait for the cache to be warmed up and ready.
        /// </summary>
        Task WaitForCacheWarmupAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Subscribe to pixel change notifications.
        /// </summary>
        void OnPixelChanged(Func<Pixel, Task> callback);
    }
}
