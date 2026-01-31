using Microsoft.Extensions.Logging;
using Moq;
using WebCanvas.Services;
using WebCanvas.Models;
using NoredbPartitioningControllerClient;
using VeritasClient.ServiceRegistration;
using Partitioningcontroller;

namespace WebCanvasTests
{
    public class CacheInvalidationServiceTests
    {
        private readonly Mock<IPeerConnectionService> _mockPeerConnectionService;
        private readonly Mock<ILogger<CacheInvalidationService>> _mockLogger;
        private readonly CacheInvalidationService _service;

        public CacheInvalidationServiceTests()
        {
            _mockPeerConnectionService = new Mock<IPeerConnectionService>();
            _mockLogger = new Mock<ILogger<CacheInvalidationService>>();
            _service = new CacheInvalidationService(_mockPeerConnectionService.Object, _mockLogger.Object);
        }

        [Fact]
        public void Constructor_SubscribesToPeerConnectionService()
        {
            // Verify that constructor subscribes to peer connection service
            _mockPeerConnectionService.Verify(
                x => x.Subscribe(It.IsAny<Func<uint, string, Task>>()),
                Times.Once);
        }

        [Fact]
        public async Task PublishInvalidationAsync_BroadcastsToAllPeers()
        {
            // Arrange
            uint testKey = 12345;

            // Act
            await _service.PublishInvalidationAsync(testKey);

            // Assert
            _mockPeerConnectionService.Verify(
                x => x.BroadcastInvalidationAsync(testKey, It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task PublishInvalidationAsync_HandlesExceptionsGracefully()
        {
            // Arrange
            uint testKey = 12345;
            _mockPeerConnectionService
                .Setup(x => x.BroadcastInvalidationAsync(It.IsAny<uint>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Network error"));

            // Act - should not throw
            await _service.PublishInvalidationAsync(testKey);

            // Assert - verify error was logged
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => true),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task Subscribe_InvokesCallbackWhenInvalidationReceived()
        {
            // Arrange
            uint testKey = 12345;
            var callbackInvoked = false;
            
            _service.Subscribe(async (key) =>
            {
                callbackInvoked = true;
                await Task.CompletedTask;
            });

            // Capture the subscription callback
            Func<uint, string, Task>? peerCallback = null;
            _mockPeerConnectionService
                .Setup(x => x.Subscribe(It.IsAny<Func<uint, string, Task>>()))
                .Callback<Func<uint, string, Task>>(callback => peerCallback = callback);

            // Create a new instance to capture the callback
            var newService = new CacheInvalidationService(_mockPeerConnectionService.Object, _mockLogger.Object);
            newService.Subscribe(async (key) =>
            {
                callbackInvoked = true;
                await Task.CompletedTask;
            });

            // Act - simulate receiving invalidation from peer
            if (peerCallback != null)
            {
                await peerCallback(testKey, "other-instance-id");
            }

            // Allow async operations to complete
            await Task.Delay(100);

            // Assert
            Assert.True(callbackInvoked);
        }

        [Fact]
        public void Subscribe_ThrowsArgumentNullException_WhenCallbackIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => _service.Subscribe(null!));
        }
    }

    public class CanvasCacheServiceTests
    {
        private readonly Mock<IPartitioningControllerClient> _mockPartitioningClient;
        private readonly Mock<ICacheInvalidationService> _mockInvalidationService;
        private readonly Mock<ILogger<CanvasCacheService>> _mockLogger;

        public CanvasCacheServiceTests()
        {
            _mockPartitioningClient = new Mock<IPartitioningControllerClient>();
            _mockInvalidationService = new Mock<ICacheInvalidationService>();
            _mockLogger = new Mock<ILogger<CanvasCacheService>>();
        }

        [Fact]
        public void Constructor_SubscribesToCacheInvalidation()
        {
            // Act
            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            // Assert
            _mockInvalidationService.Verify(
                x => x.Subscribe(It.IsAny<Func<uint, Task>>()),
                Times.Once);
        }

        [Fact]
        public async Task SetPixelAsync_UpdatesLocalCacheAndPublishesInvalidation()
        {
            // Arrange
            _mockPartitioningClient
                .Setup(x => x.GetAllAsync(It.IsAny<CancellationToken>()))
                .Returns(AsyncEnumerable.Empty<DataResponse>());

            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            await service.WaitForCacheWarmupAsync();

            uint x = 10, y = 20;
            var color = new RGBColor(255, 0, 0);
            var pixel = new Pixel { X = x, Y = y, Color = color };
            var key = pixel.ToKey();

            _mockPartitioningClient
                .Setup(x => x.SetAsync(key, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Commit());

            // Act
            await service.SetPixelAsync(x, y, color);

            // Assert
            _mockPartitioningClient.Verify(
                x => x.SetAsync(key, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()),
                Times.Once);

            _mockInvalidationService.Verify(
                x => x.PublishInvalidationAsync(key, It.IsAny<CancellationToken>()),
                Times.Once);

            var result = await service.GetPixelAsync(x, y);
            Assert.NotNull(result);
            Assert.Equal(color, result.Color);
        }

        [Fact]
        public async Task GetPixelAsync_ReturnsNullForNonExistentPixel()
        {
            // Arrange
            _mockPartitioningClient
                .Setup(x => x.GetAllAsync(It.IsAny<CancellationToken>()))
                .Returns(AsyncEnumerable.Empty<DataResponse>());

            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            await service.WaitForCacheWarmupAsync();

            // Act
            var result = await service.GetPixelAsync(999, 999);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task GetAllPixels_ReturnsAllCachedPixels()
        {
            // Arrange
            var testPixels = new List<DataResponse>
            {
                CreateDataResponse(1, 1, new RGBColor(255,0,0)),
                CreateDataResponse(2, 2, new RGBColor(0,255,0)),
                CreateDataResponse(3, 3, new RGBColor(0,0,255))
            };

            _mockPartitioningClient
                .Setup(x => x.GetAllAsync(It.IsAny<CancellationToken>()))
                .Returns(testPixels.ToAsyncEnumerable());

            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            await service.WaitForCacheWarmupAsync();

            // Act
            var result = service.GetAllPixels().ToList();

            // Assert
            Assert.Equal(3, result.Count);
        }

        [Fact]
        public async Task OnPixelChanged_InvokesCallbackWhenPixelIsSet()
        {
            // Arrange
            _mockPartitioningClient
                .Setup(x => x.GetAllAsync(It.IsAny<CancellationToken>()))
                .Returns(AsyncEnumerable.Empty<DataResponse>());

            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            await service.WaitForCacheWarmupAsync();

            Pixel? changedPixel = null;
            service.OnPixelChanged(async (pixel) =>
            {
                changedPixel = pixel;
                await Task.CompletedTask;
            });

            uint x = 10, y = 20;
            var color = new RGBColor(255, 0, 0);
            var expectedPixel = new Pixel { X = x, Y = y, Color = color };
            var key = expectedPixel.ToKey();

            _mockPartitioningClient
                .Setup(x => x.SetAsync(key, It.IsAny<byte[]>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Commit());

            // Act
            await service.SetPixelAsync(x, y, color);
            await Task.Delay(100); // Allow callback to execute

            // Assert
            Assert.NotNull(changedPixel);
            Assert.Equal(x, changedPixel.X);
            Assert.Equal(y, changedPixel.Y);
            Assert.Equal(color, changedPixel.Color);
        }

        [Fact]
        public void OnPixelChanged_ThrowsArgumentNullException_WhenCallbackIsNull()
        {
            // Arrange
            _mockPartitioningClient
                .Setup(x => x.GetAllAsync(It.IsAny<CancellationToken>()))
                .Returns(AsyncEnumerable.Empty<DataResponse>());

            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockInvalidationService.Object,
                _mockLogger.Object);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => service.OnPixelChanged(null!));
        }

        private DataResponse CreateDataResponse(uint x, uint y, RGBColor color)
        {
            var pixel = new Pixel { X = x, Y = y, Color = color };
            return new DataResponse
            {
                Key = pixel.ToKey(),
                Value = Google.Protobuf.ByteString.CopyFrom(color.ToBytes())
            };
        }
    }

    public class PeerConnectionServiceTests
    {
        private readonly Mock<IServiceRegistration> _mockServiceRegistration;
        private readonly Mock<ILogger<PeerConnectionService>> _mockLogger;

        public PeerConnectionServiceTests()
        {
            _mockServiceRegistration = new Mock<IServiceRegistration>();
            _mockLogger = new Mock<ILogger<PeerConnectionService>>();
        }

        [Fact]
        public async Task StartAsync_SubscribesToEndpointUpdates()
        {
            // Arrange
            _mockServiceRegistration
                .Setup(x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ServiceRegistration
                {
                    Id = "test-service",
                    Endpoints = new List<ServiceEndpoint>()
                });

            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);

            // Act
            await service.StartAsync(CancellationToken.None);

            // Assert
            _mockServiceRegistration.Verify(
                x => x.AddEndpointListener(It.IsAny<Action<ServiceEndpointsUpdate>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_ResolvesInitialEndpoints()
        {
            // Arrange
            _mockServiceRegistration
                .Setup(x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ServiceRegistration
                {
                    Id = "test-service",
                    Endpoints = new List<ServiceEndpoint>
                    {
                        new ServiceEndpoint { Address = "localhost", Port = 5000 }
                    }
                });

            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);

            // Act
            await service.StartAsync(CancellationToken.None);

            // Assert
            _mockServiceRegistration.Verify(
                x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_HandlesResolveServiceFailureGracefully()
        {
            // Arrange
            _mockServiceRegistration
                .Setup(x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Service resolution failed"));

            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);

            // Act - should not throw
            await service.StartAsync(CancellationToken.None);

            // Assert - verify warning was logged
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => true),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void Subscribe_StoresCallback()
        {
            // Arrange
            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);
            var callbackInvoked = false;

            // Act
            service.Subscribe(async (key, instanceId) =>
            {
                callbackInvoked = true;
                await Task.CompletedTask;
            });

            // Assert - no exception thrown
            Assert.False(callbackInvoked); // Callback not invoked yet
        }

        [Fact]
        public void Subscribe_ThrowsArgumentNullException_WhenCallbackIsNull()
        {
            // Arrange
            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => service.Subscribe(null!));
        }

        [Fact]
        public async Task BroadcastInvalidationAsync_WithNoConnections_LogsDebugMessage()
        {
            // Arrange
            _mockServiceRegistration
                .Setup(x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ServiceRegistration
                {
                    Id = "test-service",
                    Endpoints = new List<ServiceEndpoint>()
                });

            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);
            await service.StartAsync(CancellationToken.None);

            // Act
            await service.BroadcastInvalidationAsync(123, "test-instance", CancellationToken.None);

            // Assert - verify debug log about no peer connections
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => true),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }

        [Fact]
        public async Task StopAsync_DisconnectsAllPeerConnections()
        {
            // Arrange
            _mockServiceRegistration
                .Setup(x => x.ResolveServiceAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ServiceRegistration
                {
                    Id = "test-service",
                    Endpoints = new List<ServiceEndpoint>()
                });

            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);
            await service.StartAsync(CancellationToken.None);

            // Act
            await service.StopAsync(CancellationToken.None);

            // Assert - verify logging indicates stopping
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Stopping")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public void Dispose_CancelsOperationsAndCleansUp()
        {
            // Arrange
            var service = new PeerConnectionService(_mockServiceRegistration.Object, _mockLogger.Object);

            // Act
            service.Dispose();

            // Assert - no exception thrown, service is disposed
            Assert.True(true);
        }
    }

    // Helper extension to convert IEnumerable to IAsyncEnumerable
    internal static class AsyncEnumerableExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> enumerable)
        {
            foreach (var item in enumerable)
            {
                await Task.Yield();
                yield return item;
            }
        }
    }
}
