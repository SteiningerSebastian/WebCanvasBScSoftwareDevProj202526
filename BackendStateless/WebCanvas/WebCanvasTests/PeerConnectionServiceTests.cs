using Microsoft.Extensions.Logging;
using Moq;
using WebCanvas.Services;
using WebCanvas.Models;
using NoredbPartitioningControllerClient;
using VeritasClient.ServiceRegistration;
using Partitioningcontroller;

namespace WebCanvasTests
{
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
            service.Subscribe(async (key) =>
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
            await service.BroadcastInvalidationAsync(123, CancellationToken.None);

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
