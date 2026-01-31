using Microsoft.AspNetCore.SignalR;
using WebCanvas.Models;
using WebCanvas.Services;

namespace WebCanvas.Hubs;

/// <summary>
/// SignalR hub for real-time canvas operations with write-through caching.
/// Supports setting pixels, getting pixels, and streaming the entire canvas.
/// </summary>
public class CanvasHub : Microsoft.AspNetCore.SignalR.Hub
{
    private readonly ICanvasCacheService _cacheService;
    private readonly ILogger<CanvasHub> _logger;

    public CanvasHub(
        ICanvasCacheService cacheService,
        ILogger<CanvasHub> logger)
    {
        _cacheService = cacheService ?? throw new ArgumentNullException(nameof(cacheService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Set a pixel on the canvas. This triggers a write-through to the partitioning controller,
    /// invalidates the cache on all other instances, and notifies all connected clients.
    /// </summary>
    /// <param name="request">The pixel data to set</param>
    public async Task SetPixel(SetPixelRequest request)
    {
        try
        {
            _logger.LogInformation(
                "Client {ConnectionId} setting pixel ({X}, {Y}) to color {Color}",
                Context.ConnectionId,
                request.X,
                request.Y,
                request.Color);

            // Write-through to storage and update cache
            // This also publishes invalidation to other instances
            await _cacheService.SetPixelAsync(request.X, request.Y, request.Color, Context.ConnectionAborted);

            _logger.LogDebug(
                "Pixel ({X}, {Y}) updated successfully",
                request.X,
                request.Y);

            // Note: Other instances will be notified via cache invalidation,
            // and their caches will refresh and notify their SignalR clients
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to set pixel ({X}, {Y}) for client {ConnectionId}",
                request.X,
                request.Y,
                Context.ConnectionId);

            throw new HubException("Failed to set pixel", ex);
        }
    }

    /// <summary>
    /// Get a specific pixel from the canvas. Always reads from cache (fast).
    /// </summary>
    /// <param name="request">The coordinates of the pixel to get</param>
    /// <returns>The pixel data or null if not found</returns>
    public async Task<PixelResponse> GetPixel(GetPixelRequest request)
    {
        try
        {
            _logger.LogDebug(
                "Client {ConnectionId} getting pixel ({X}, {Y})",
                Context.ConnectionId,
                request.X,
                request.Y);

            // Always serve from cache - no DB hit
            var pixel = await _cacheService.GetPixelAsync(request.X, request.Y, Context.ConnectionAborted);

            if (pixel == null)
            {
                return new PixelResponse
                {
                    X = request.X,
                    Y = request.Y,
                    Success = false,
                    ErrorMessage = "Pixel not found"
                };
            }

            return new PixelResponse
            {
                X = pixel.X,
                Y = pixel.Y,
                Color = pixel.Color,
                Success = true
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to get pixel ({X}, {Y}) for client {ConnectionId}",
                request.X,
                request.Y,
                Context.ConnectionId);

            return new PixelResponse
            {
                X = request.X,
                Y = request.Y,
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Stream all pixels from the canvas to the client. This method iterates through
    /// the cached pixels (memory) - no DB hits.
    /// </summary>
    public async Task StreamCanvas()
    {
        try
        {
            _logger.LogInformation(
                "Client {ConnectionId} requesting canvas stream",
                Context.ConnectionId);

            // Wait for cache to be warmed up
            await _cacheService.WaitForCacheWarmupAsync(Context.ConnectionAborted);

            var pixelCount = 0;

            // Stream from cache (memory) - no DB hits
            foreach (var pixel in _cacheService.GetAllPixels())
            {
                var response = new PixelResponse
                {
                    X = pixel.X,
                    Y = pixel.Y,
                    Color = pixel.Color,
                    Success = true
                };

                // Send each pixel to the caller
                await Clients.Caller.SendAsync("PixelReceived", response, Context.ConnectionAborted);
                
                pixelCount++;

                // Add a small yield to prevent overwhelming the client
                if (pixelCount % 1000 == 0)
                {
                    await Task.Delay(1, Context.ConnectionAborted);
                }
            }

            // Notify the caller that streaming is complete
            await Clients.Caller.SendAsync("CanvasStreamComplete", pixelCount, Context.ConnectionAborted);

            _logger.LogInformation(
                "Canvas stream completed for client {ConnectionId}. Sent {PixelCount} pixels",
                Context.ConnectionId,
                pixelCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to stream canvas for client {ConnectionId}",
                Context.ConnectionId);

            await Clients.Caller.SendAsync(
                "CanvasStreamFailed",
                ex.Message,
                Context.ConnectionAborted);
        }
    }

    /// <summary>
    /// Called when a client connects to the hub.
    /// </summary>
    public override Task OnConnectedAsync()
    {
        _logger.LogInformation("Client {ConnectionId} connected to CanvasHub", Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    /// <summary>
    /// Called when a client disconnects from the hub.
    /// </summary>
    public override Task OnDisconnectedAsync(Exception? exception)
    {
        if (exception != null)
        {
            _logger.LogWarning(
                exception,
                "Client {ConnectionId} disconnected from CanvasHub with error",
                Context.ConnectionId);
        }
        else
        {
            _logger.LogInformation("Client {ConnectionId} disconnected from CanvasHub", Context.ConnectionId);
        }

        return base.OnDisconnectedAsync(exception);
    }
}

