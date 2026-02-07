using System.Collections.Concurrent;
using NoredbPartitioningControllerClient;
using WebCanvas.Models;

namespace WebCanvas.Services;

public class CanvasCacheService : ICanvasCacheService
{
    private readonly IPartitioningControllerClient _partitioningClient;
    private readonly IPeerConnectionService _peerConnectionService;
    private readonly ILogger<CanvasCacheService> _logger;
    private readonly ConcurrentDictionary<uint, RGBColor> _cache = new();
    private readonly ConcurrentQueue<uint> _invalidationQueue = new();
    private readonly ConcurrentBag<Func<Pixel, Task>> _pixelChangedCallbacks = new();
    private readonly TaskCompletionSource<bool> _cacheWarmupComplete = new();
    private volatile bool _isWarming = true;

    public CanvasCacheService(
        IPartitioningControllerClient partitioningClient,
        IPeerConnectionService peerConnectionService,
        ILogger<CanvasCacheService> logger)
    {
        _partitioningClient = partitioningClient ?? throw new ArgumentNullException(nameof(partitioningClient));
        _peerConnectionService = peerConnectionService ?? throw new ArgumentNullException(nameof(peerConnectionService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Subscribe to cache invalidation notifications before warming
        _peerConnectionService.Subscribe(OnCacheInvalidationAsync);

        // Start cache warming in the background
        _ = Task.Run(WarmCacheAsync);
    }
    
    /// <summary>
    /// Asynchronously loads all pixel data from the database into the cache, ensuring the cache is fully populated and
    /// ready to serve requests.
    /// </summary>
    /// <remarks>If an error occurs during the warm-up process, the method retries up to a fixed number of
    /// times before propagating the exception. Any cache invalidations that occur during warm-up are processed after
    /// the initial load completes. This method should be called before serving requests that depend on the cache being
    /// populated.</remarks>
    /// <returns>A task that represents the asynchronous cache warm-up operation.</returns>
    private async Task WarmCacheAsync()
    {
        const int MAX_RETRIES = 10;
        const int INITIAL_RETRY_DELAY_MS = 2000;
        const int MAX_RETRY_DELAY_MS = 30000;
        
        int currentRetryDelay = INITIAL_RETRY_DELAY_MS;
        
        for (int i = 0; i < MAX_RETRIES; i++)
        {
            try
            {
                _logger.LogInformation("Starting cache warm-up... (attempt {Attempt}/{MaxRetries})", i + 1, MAX_RETRIES);
                var startTime = DateTime.UtcNow;

                // Load all pixels from the database into cache
                int pixelCount = 0;
                await foreach (var response in _partitioningClient.GetAllAsync())
                {
                    if (response.Value.Length == 0)
                    {
                        continue;
                    }

                    var color = RGBColor.FromBytes(response.Value.ToByteArray());
                    _cache[response.Key] = color;
                    pixelCount++;
                    
                    if (pixelCount % 1000 == 0)
                    {
                        _logger.LogDebug("Loaded {Count} pixels so far...", pixelCount);
                    }
                }

                var duration = DateTime.UtcNow - startTime;
                _logger.LogInformation(
                    "Cache warm-up complete. Loaded {Count} pixels in {Duration}ms",
                    _cache.Count,
                    duration.TotalMilliseconds);

                // Mark warming as complete
                _isWarming = false;

                // Process queued invalidations that arrived during warm-up
                _logger.LogInformation("Processing {Count} queued invalidations", _invalidationQueue.Count);
                while (_invalidationQueue.TryDequeue(out var key))
                {
                    await RefreshCacheEntryAsync(key);
                }

                _logger.LogInformation("Cache is now ready to serve requests");
                _cacheWarmupComplete.TrySetResult(true);
                return; // Success, exit the method
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, 
                    "Failed to warm cache (attempt {Attempt}/{MaxRetries}). Error: {ErrorType}: {ErrorMessage}",
                    i + 1,
                    MAX_RETRIES,
                    ex.GetType().Name,
                    ex.Message);

                if (i < MAX_RETRIES - 1)
                {
                    _logger.LogInformation("Retrying cache warm-up in {Delay}ms...", currentRetryDelay);
                    await Task.Delay(currentRetryDelay);
                    currentRetryDelay = Math.Min(currentRetryDelay * 2, MAX_RETRY_DELAY_MS);
                }
                else
                {
                    // All retries exhausted - fall back to lazy-load mode
                    _logger.LogError("Max retries reached. Cache will operate in lazy-load mode.");
                    _isWarming = false;
                    _cacheWarmupComplete.TrySetResult(true);
                }
            }
        }
    }

    private async Task OnCacheInvalidationAsync(uint key)
    {
        // If still warming, queue the invalidation
        if (_isWarming)
        {
            _invalidationQueue.Enqueue(key);
            _logger.LogDebug("Queued invalidation for key {Key} (cache warming in progress)", key);
            return;
        }

        // Otherwise, process immediately
        await RefreshCacheEntryAsync(key);
    }

    private async Task RefreshCacheEntryAsync(uint key)
    {
        try
        {
            // Fetch the latest value from the partitioning controller
            var response = await _partitioningClient.GetAsync(key, CancellationToken.None);

            RGBColor color;
            if (response.Value.Length == 0)
            {
                // Pixel was deleted, remove from cache
                _cache.TryRemove(key, out _);
                _logger.LogDebug("Removed key {Key} from cache (deleted)", key);
                return;
            }
            else
            {
                color = RGBColor.FromBytes(response.Value.ToByteArray());
                _cache[key] = color;
                _logger.LogDebug("Refreshed cache for key {Key}", key);
            }

            // Notify SignalR clients about the change
            var pixel = Pixel.FromKey(key, color);
            var notifyTasks = _pixelChangedCallbacks.Select(callback =>
                Task.Run(async () =>
                {
                    try
                    {
                        await callback(pixel);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in pixel changed callback");
                    }
                }));

            await Task.WhenAll(notifyTasks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to refresh cache entry for key {Key}", key);
        }
    }

    public async Task<bool> SetPixelAsync(uint x, uint y, RGBColor color, CancellationToken cancellationToken = default)
    {
        var pixel = new Pixel { X = x, Y = y, Color = color };
        var key = pixel.ToKey();

        try
        {
            // Write-through: Write to partitioning controller first
            var result = await _partitioningClient.SetAsync(key, pixel.Color.ToBytes(), cancellationToken);

            if (result.Status != 0b0001)
            {
                _logger.LogWarning("Failed to set pixel ({X}, {Y}) in partitioning controller: {Status}", x, y, result.Status);
                return false;
            }

            // Update local cache
            _cache[key] = color;

            _logger.LogDebug("Set pixel ({X}, {Y}) to color {Color}", x, y, color);

            // Notify local SignalR clients about the change
            var notifyTasks = _pixelChangedCallbacks.Select(callback =>
                Task.Run(async () =>
                {
                    try
                    {
                        await callback(pixel);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in pixel changed callback");
                    }
                }));

            await Task.WhenAll(notifyTasks);

            // Publish cache invalidation to other instances (they will pull and notify their clients)
            await _peerConnectionService.BroadcastInvalidationAsync(key, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to set pixel ({X}, {Y})", x, y);
            throw;
        }

        return true;
    }

    public async Task<Pixel?> GetPixelAsync(uint x, uint y, CancellationToken cancellationToken = default)
    {
        // Ensure cache is warmed up before serving requests
        if (this._cacheWarmupComplete.Task.IsCompleted == false)
        {
            this._logger.LogWarning("Cache not warmed up yet. GetPixelAsync returning null for ({X}, {Y})", x, y);
            return null;
        }

        var pixel = new Pixel { X = x, Y = y };
        var key = pixel.ToKey();

        // Always serve from cache (cache is kept up-to-date via invalidations)
        if (_cache.TryGetValue(key, out var color))
        {
            return new Pixel
            {
                X = x,
                Y = y,
                Color = color
            };
        }

        _logger.LogDebug("Cache miss for pixel ({X}, {Y})", x, y);

        // Fallback: Pixel not found in cache
        var rsp = await _partitioningClient.GetAsync(key, cancellationToken);

        if (rsp is null || rsp?.Value.Length == 0)
        {
            return null; // Pixel does not exist
        }

        var byteArray = rsp!.Value.ToByteArray();
        if (byteArray.Length < 3)
        {
            _logger.LogWarning("Invalid pixel data for ({X}, {Y}): expected at least 3 bytes (RGB), got {Length} bytes", x, y, byteArray.Length);
            return null; // Invalid pixel data
        }

        color = RGBColor.FromBytes(byteArray);
        // Update cache
        _cache[key] = color;

        return new Pixel
        {
            X = x,
            Y = y,
            Color = color
        };
    }

    public IEnumerable<Pixel> GetAllPixels()
    {
        // Serve all pixels from cache
        foreach (var kvp in _cache)
        {
            yield return Pixel.FromKey(kvp.Key, kvp.Value);
        }
    }

    public async Task WaitForCacheWarmupAsync(CancellationToken cancellationToken = default)
    {
        using var registration = cancellationToken.Register(() => _cacheWarmupComplete.TrySetCanceled());
        await _cacheWarmupComplete.Task;
    }

    public void OnPixelChanged(Func<Pixel, Task> callback)
    {
        if (callback == null)
        {
            throw new ArgumentNullException(nameof(callback));
        }

        _pixelChangedCallbacks.Add(callback);
    }
}

