using Microsoft.AspNetCore.SignalR;
using WebCanvas.Hubs;
using WebCanvas.Models;
using WebCanvas.Services;

namespace WebCanvas.Services;

/// <summary>
/// Bridge service that connects cache invalidation events to SignalR client notifications.
/// When a pixel is updated in another instance, this service notifies all connected SignalR clients.
/// </summary>
public class SignalRNotificationService : IHostedService
{
    private readonly ICanvasCacheService _cacheService;
    private readonly IHubContext<CanvasHub> _hubContext;
    private readonly ILogger<SignalRNotificationService> _logger;

    public SignalRNotificationService(
        ICanvasCacheService cacheService,
        IHubContext<CanvasHub> hubContext,
        ILogger<SignalRNotificationService> logger)
    {
        _cacheService = cacheService ?? throw new ArgumentNullException(nameof(cacheService));
        _hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Subscribe to pixel change events from the cache service
        _cacheService.OnPixelChanged(OnPixelChangedAsync);

        _logger.LogInformation("SignalR notification service started");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("SignalR notification service stopped");
        return Task.CompletedTask;
    }

    private async Task OnPixelChangedAsync(Pixel pixel)
    {
        try
        {
            // Notify all connected SignalR clients about the pixel update
            var response = new PixelResponse
            {
                X = pixel.X,
                Y = pixel.Y,
                Color = pixel.Color,
                Success = true
            };

            await _hubContext.Clients.All.SendAsync("PixelUpdated", response);

            _logger.LogDebug(
                "Broadcasted pixel update to SignalR clients: ({X}, {Y}) = {Color}",
                pixel.X,
                pixel.Y,
                pixel.Color);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to broadcast pixel update to SignalR clients: ({X}, {Y})",
                pixel.X,
                pixel.Y);
        }
    }
}
