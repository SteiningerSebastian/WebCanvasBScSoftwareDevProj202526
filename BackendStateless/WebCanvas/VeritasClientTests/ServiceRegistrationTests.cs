using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using VeritasClient;
using VeritasClient.Models;
using VeritasClient.ServiceRegistration;

namespace VeritasClientTests
{
    public class ServiceRegistrationTests : IDisposable
    {
        private readonly Mock<IVeritasClient> _mockClient;
        private readonly List<ServiceRegistrationHandler> _handlers = new();

        public ServiceRegistrationTests()
        {
            _mockClient = new Mock<IVeritasClient>();
        }

        public void Dispose()
        {
            foreach (var handler in _handlers)
            {
                handler.Dispose();
            }
        }

        private ServiceRegistrationHandler CreateHandler(string serviceName, ILogger<ServiceRegistrationHandler>? logger = null)
        {
            var handler = new ServiceRegistrationHandler(
                _mockClient.Object, 
                serviceName, 
                logger ?? NullLogger<ServiceRegistrationHandler>.Instance);
            _handlers.Add(handler);
            return handler;
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_ShouldThrowArgumentNullException_WhenClientIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new ServiceRegistrationHandler(null!, "test-service", NullLogger<ServiceRegistrationHandler>.Instance));
        }

        [Fact]
        public void Constructor_ShouldThrowArgumentNullException_WhenServiceNameIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new ServiceRegistrationHandler(_mockClient.Object, null!, NullLogger<ServiceRegistrationHandler>.Instance));
        }

        [Fact]
        public void Constructor_ShouldStartWatchingService()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.Is<string[]>(names => names.Length == 1 && names[0] == "test-service"),
                It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            // Act
            var handler = CreateHandler("test-service");

            // Assert
            _mockClient.Verify(c => c.WatchVariablesAutoReconnect(
                It.Is<string[]>(names => names.Length == 1 && names[0] == "test-service"),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        #endregion

        #region ResolveServiceAsync Tests

        [Fact]
        public async Task ResolveServiceAsync_ShouldReturnServiceRegistration_WhenServiceExists()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var expectedService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(expectedService);
            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync(jsonStr);

            var handler = CreateHandler("test-service");

            // Act
            var result = await handler.ResolveServiceAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(expectedService.Id, result.Id);
            Assert.Single(result.Endpoints);
            Assert.Equal(expectedService.Endpoints[0].Address, result.Endpoints[0].Address);
        }

        [Fact]
        public async Task ResolveServiceAsync_ShouldThrowServiceNotFoundException_WhenServiceDoesNotExist()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Variable not found"));

            var handler = CreateHandler("test-service");

            // Act & Assert
            await Assert.ThrowsAsync<ServiceNotFoundException>(() => handler.ResolveServiceAsync());
        }

        [Fact]
        public async Task ResolveServiceAsync_ShouldThrowServiceNotFoundException_WhenJsonIsInvalid()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync("invalid json {{{");

            var handler = CreateHandler("test-service");

            // Act & Assert
            await Assert.ThrowsAsync<ServiceNotFoundException>(() => handler.ResolveServiceAsync());
        }

        #endregion

        #region RegisterOrUpdateServiceAsync Tests

        [Fact]
        public async Task RegisterOrUpdateServiceAsync_ShouldSucceed_WhenCompareAndSetSucceeds()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                "test-service",
                "",
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handler = CreateHandler("test-service");

            // Act
            await handler.RegisterOrUpdateServiceAsync(service);

            // Assert
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                "test-service",
                "",
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RegisterOrUpdateServiceAsync_ShouldThrowServiceRegistrationChangedException_WhenCompareAndSetFails()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var service = new ServiceRegistration { Id = "test-service" };

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(false);

            var handler = CreateHandler("test-service");

            // Act & Assert
            await Assert.ThrowsAsync<ServiceRegistrationChangedException>(
                () => handler.RegisterOrUpdateServiceAsync(service));
        }

        [Fact]
        public async Task RegisterOrUpdateServiceAsync_ShouldUpdateWithOldService_WhenProvided()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var oldService = new ServiceRegistration { Id = "test-service" };
            var newService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var oldJson = JsonSerializer.Serialize(oldService);

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                "test-service",
                oldJson,
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handler = CreateHandler("test-service");

            // Act
            await handler.RegisterOrUpdateServiceAsync(newService, oldService);

            // Assert
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                "test-service",
                oldJson,
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RegisterOrUpdateServiceAsync_ShouldThrowArgumentNullException_WhenServiceIsNull()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => handler.RegisterOrUpdateServiceAsync(null!));
        }

        #endregion

        #region RegisterOrUpdateEndpointAsync Tests

        [Fact]
        public async Task RegisterOrUpdateEndpointAsync_ShouldAddNewEndpoint_WhenEndpointDoesNotExist()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var existingService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var existingJson = JsonSerializer.Serialize(existingService);
            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync(existingJson);

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                "test-service",
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handler = CreateHandler("test-service");

            var newEndpoint = new ServiceEndpoint { Id = "ep2", Address = "localhost", Port = 8081, Timestamp = DateTime.UtcNow };

            // Act
            await handler.RegisterOrUpdateEndpointAsync(newEndpoint);

            // Assert
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                "test-service",
                existingJson,
                It.Is<string>(json => json.Contains("ep2")),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RegisterOrUpdateEndpointAsync_ShouldUpdateExistingEndpoint_WhenEndpointExists()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var timestamp1 = DateTime.UtcNow.AddMinutes(-5);
            var timestamp2 = DateTime.UtcNow;

            var existingService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = timestamp1 }
                }
            };

            var existingJson = JsonSerializer.Serialize(existingService);
            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync(existingJson);

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                "test-service",
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handler = CreateHandler("test-service");

            var updatedEndpoint = new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 9090, Timestamp = timestamp2 };

            // Act
            await handler.RegisterOrUpdateEndpointAsync(updatedEndpoint);

            // Assert
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                "test-service",
                existingJson,
                It.Is<string>(json => json.Contains("9090")),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RegisterOrUpdateEndpointAsync_ShouldThrowArgumentNullException_WhenEndpointIsNull()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => handler.RegisterOrUpdateEndpointAsync(null!));
        }

        #endregion

        #region Listener Tests

        [Fact]
        public void AddListener_ShouldThrowArgumentNullException_WhenCallbackIsNull()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => handler.AddListener(null!));
        }

        #endregion

        #region Endpoint Listener Tests

        [Fact]
        public void AddEndpointListener_ShouldThrowArgumentNullException_WhenCallbackIsNull()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => handler.AddEndpointListener(null!));
        }

       
        #endregion

        #region TryCleanupOldEndpointsAsync Tests

        [Fact]
        public async Task TryCleanupOldEndpointsAsync_ShouldRemoveExpiredEndpoints()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var oldTimestamp = DateTime.UtcNow.AddMinutes(-10);
            var newTimestamp = DateTime.UtcNow;

            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = oldTimestamp },
                    new ServiceEndpoint { Id = "ep2", Address = "localhost", Port = 8081, Timestamp = newTimestamp }
                }
            };

            var serviceJson = JsonSerializer.Serialize(service);
            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync(serviceJson);

            _mockClient.Setup(c => c.CompareAndSetVariableAsync(
                "test-service",
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var handler = CreateHandler("test-service");

            // Act
            await handler.TryCleanupOldEndpointsAsync(TimeSpan.FromMinutes(5));

            // Assert - Should update with only the non-expired endpoint
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                "test-service",
                serviceJson,
                It.Is<string>(json => !json.Contains("ep1") && json.Contains("ep2")),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task TryCleanupOldEndpointsAsync_ShouldNotUpdate_WhenNoEndpointsExpired()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var recentTimestamp = DateTime.UtcNow;

            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = recentTimestamp },
                    new ServiceEndpoint { Id = "ep2", Address = "localhost", Port = 8081, Timestamp = recentTimestamp }
                }
            };

            var serviceJson = JsonSerializer.Serialize(service);
            _mockClient.Setup(c => c.GetVariableAsync("test-service", It.IsAny<CancellationToken>()))
                .ReturnsAsync(serviceJson);

            var handler = CreateHandler("test-service");

            // Act
            await handler.TryCleanupOldEndpointsAsync(TimeSpan.FromMinutes(5));

            // Assert - Should not call CompareAndSet since no endpoints were removed
            _mockClient.Verify(c => c.CompareAndSetVariableAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }

        #endregion

        #region GetCurrentRegistration Tests

        [Fact]
        public void GetCurrentRegistration_ShouldReturnNull_WhenNoRegistrationCached()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act
            var result = handler.GetCurrentRegistration();

            // Assert
            Assert.Null(result);
        }

        #endregion

        #region Error Handling Tests


        [Fact]
        public async Task Handler_ShouldLogError_WhenWatchErrorOccurs()
        {
            // Arrange
            var mockLogger = new Mock<ILogger<ServiceRegistrationHandler>>();
            var tcs = new TaskCompletionSource<bool>();

            mockLogger.Setup(x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Error watching service registration")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
                .Callback(() => tcs.TrySetResult(true));

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((AsyncEnumerable.Empty<UpdateNotification>(), CreateAsyncEnumerableError(new Exception("Watch error"))));

            var handler = CreateHandler("test-service", mockLogger.Object);

            // Wait for the error to be logged
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Error was not logged within timeout");
        }

        private static async IAsyncEnumerable<Exception> CreateAsyncEnumerableError(Exception error)
        {
            await Task.Delay(10);
            yield return error;
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_ShouldCancelWatchTask()
        {
            // Arrange
            var updates = AsyncEnumerable.Empty<UpdateNotification>();
            var errors = AsyncEnumerable.Empty<Exception>();
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(It.IsAny<string[]>(), It.IsAny<CancellationToken>()))
                .Returns((updates, errors));

            var handler = CreateHandler("test-service");

            // Act
            handler.Dispose();

            // Assert - Should not throw
            handler.Dispose(); // Second dispose should be safe
        }

        #endregion
    }
}
