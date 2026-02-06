using Microsoft.Extensions.Logging;
using Moq;
using NoredbPartitioningControllerClient;
using Partitioningcontroller;
using System;
using System.Collections.Generic;
using System.Text;
using WebCanvas.Models;
using WebCanvas.Services;

namespace WebCanvasTests
{
    public class CanvasCacheServiceTests
    {
        private readonly Mock<IPartitioningControllerClient> _mockPartitioningClient;
        private readonly Mock<IPeerConnectionService> _mockPeerConnectionService;
        private readonly Mock<ILogger<CanvasCacheService>> _mockLogger;

        public CanvasCacheServiceTests()
        {
            _mockPartitioningClient = new Mock<IPartitioningControllerClient>();
            _mockPeerConnectionService = new Mock<IPeerConnectionService>();
            _mockLogger = new Mock<ILogger<CanvasCacheService>>();
        }

        [Fact]
        public void Constructor_SubscribesToCacheInvalidation()
        {
            // Act
            var service = new CanvasCacheService(
                _mockPartitioningClient.Object,
                _mockPeerConnectionService.Object,
                _mockLogger.Object);

            // Assert
            _mockPeerConnectionService.Verify(
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
                _mockPeerConnectionService.Object,
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

            _mockPeerConnectionService.Verify(
                x => x.BroadcastInvalidationAsync(key, It.IsAny<CancellationToken>()),
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
                _mockPeerConnectionService.Object,
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
                _mockPeerConnectionService.Object,
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
                _mockPeerConnectionService.Object,
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
                _mockPeerConnectionService.Object,
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

}
