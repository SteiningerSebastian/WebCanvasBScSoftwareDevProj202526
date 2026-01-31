using System.Text.Json;
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

        private ServiceRegistrationHandler CreateHandler(string serviceName, Action<string>? logError = null)
        {
            var handler = new ServiceRegistrationHandler(_mockClient.Object, serviceName, logError);
            _handlers.Add(handler);
            return handler;
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_ShouldThrowArgumentNullException_WhenClientIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new ServiceRegistrationHandler(null!, "test-service"));
        }

        [Fact]
        public void Constructor_ShouldThrowArgumentNullException_WhenServiceNameIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new ServiceRegistrationHandler(_mockClient.Object, null!));
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
        public async Task AddListener_ShouldInvokeCallback_WhenServiceUpdates()
        {
            // Arrange
            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(service);
            var notification = new UpdateNotification
            {
                Key = "test-service",
                OldValue = "",
                NewValue = jsonStr
            };

            var tcs = new TaskCompletionSource<ServiceRegistration>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerable(notification), AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddListener(s =>
            {
                tcs.TrySetResult(s);
            });

            // Wait for the callback to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Callback was not invoked within timeout");
            var receivedService = await tcs.Task;
            Assert.NotNull(receivedService);
            Assert.Equal(service.Id, receivedService!.Id);
        }

        private static async IAsyncEnumerable<UpdateNotification> CreateAsyncEnumerable(UpdateNotification notification)
        {
            await Task.Delay(10); // Small delay to ensure handler is ready
            yield return notification;
        }

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

        [Fact]
        public async Task AddListener_ShouldSkipEmptyUpdates()
        {
            // Arrange - Simulate initial watch with empty value followed by actual registration
            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(service);

            var tcs = new TaskCompletionSource<ServiceRegistration>();

            // Setup mock BEFORE creating handler - send empty string first, then valid JSON
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerableMultiple(
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = "" },
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = jsonStr }),
                    AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddListener(s =>
            {
                tcs.TrySetResult(s);
            });

            // Wait for the callback to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert - Should receive the valid update, not the empty one
            Assert.True(completedTask == tcs.Task, "Callback was not invoked within timeout");
            var receivedService = await tcs.Task;
            Assert.NotNull(receivedService);
            Assert.Equal(service.Id, receivedService!.Id);
            Assert.Single(receivedService.Endpoints);
        }

        #endregion

        #region Endpoint Listener Tests

        [Fact]
        public async Task AddEndpointListener_ShouldInvokeCallback_WhenEndpointsChange()
        {
            // Arrange
            var oldService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var newService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow },
                    new ServiceEndpoint { Id = "ep2", Address = "localhost", Port = 8081, Timestamp = DateTime.UtcNow }
                }
            };

            var oldJson = JsonSerializer.Serialize(oldService);
            var newJson = JsonSerializer.Serialize(newService);

            var updates = new List<ServiceEndpointsUpdate>();
            var tcs = new TaskCompletionSource<bool>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerableMultiple(
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = oldJson },
                    new UpdateNotification { Key = "test-service", OldValue = oldJson, NewValue = newJson }),
                    AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddEndpointListener(update =>
            {
                updates.Add(update);
                if (updates.Count == 2)
                {
                    tcs.TrySetResult(true);
                }
            });

            // Wait for both callbacks to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Callbacks were not invoked within timeout");
            Assert.Equal(2, updates.Count);
            
            // First update: ep1 added (initial state)
            Assert.Single(updates[0].AddedEndpoints);
            Assert.Empty(updates[0].RemovedEndpoints);
            Assert.Equal("ep1", updates[0].AddedEndpoints[0].Id);
            
            // Second update: ep2 added
            Assert.Single(updates[1].AddedEndpoints);
            Assert.Empty(updates[1].RemovedEndpoints);
            Assert.Equal("ep2", updates[1].AddedEndpoints[0].Id);
        }

        [Fact]
        public async Task AddEndpointListener_ShouldNotifyOnFirstUpdate()
        {
            // Arrange
            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(service);

            var tcs = new TaskCompletionSource<ServiceEndpointsUpdate>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerable(
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = jsonStr }),
                    AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddEndpointListener(update =>
            {
                tcs.TrySetResult(update);
            });

            // Wait for the callback to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Callback was not invoked within timeout");
            var receivedUpdate = await tcs.Task;
            Assert.NotNull(receivedUpdate);
            Assert.Single(receivedUpdate!.AddedEndpoints);
            Assert.Empty(receivedUpdate.RemovedEndpoints);
            Assert.Equal("ep1", receivedUpdate.AddedEndpoints[0].Id);
        }

        private static async IAsyncEnumerable<UpdateNotification> CreateAsyncEnumerableMultiple(params UpdateNotification[] notifications)
        {
            await Task.Delay(10);
            foreach (var notification in notifications)
            {
                yield return notification;
                await Task.Delay(10);
            }
        }

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

        [Fact]
        public async Task AddEndpointListener_ShouldSkipEmptyUpdates()
        {
            // Arrange - Simulate initial watch with empty value followed by actual registration
            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(service);

            var tcs = new TaskCompletionSource<ServiceEndpointsUpdate>();

            // Setup mock BEFORE creating handler - send empty string first, then valid JSON
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerableMultiple(
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = "" },
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = "   " },
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = jsonStr }),
                    AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddEndpointListener(update =>
            {
                tcs.TrySetResult(update);
            });

            // Wait for the callback to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert - Should receive the valid update with ep1 added, not the empty ones
            Assert.True(completedTask == tcs.Task, "Callback was not invoked within timeout");
            var receivedUpdate = await tcs.Task;
            Assert.NotNull(receivedUpdate);
            Assert.Single(receivedUpdate!.AddedEndpoints);
            Assert.Empty(receivedUpdate.RemovedEndpoints);
            Assert.Equal("ep1", receivedUpdate.AddedEndpoints[0].Id);
        }

        [Fact]
        public async Task AddEndpointListener_ShouldNotifyRemovedEndpoints()
        {
            // Arrange
            var oldService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow },
                    new ServiceEndpoint { Id = "ep2", Address = "localhost", Port = 8081, Timestamp = DateTime.UtcNow }
                }
            };

            var newService = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var oldJson = JsonSerializer.Serialize(oldService);
            var newJson = JsonSerializer.Serialize(newService);

            var updates = new List<ServiceEndpointsUpdate>();
            var tcs = new TaskCompletionSource<bool>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerableMultiple(
                    new UpdateNotification { Key = "test-service", OldValue = "", NewValue = oldJson },
                    new UpdateNotification { Key = "test-service", OldValue = oldJson, NewValue = newJson }),
                    AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddEndpointListener(update =>
            {
                updates.Add(update);
                if (updates.Count == 2)
                {
                    tcs.TrySetResult(true);
                }
            });

            // Wait for both callbacks to be invoked
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Callbacks were not invoked within timeout");
            Assert.Equal(2, updates.Count);
            
            // First update: ep1 and ep2 added (initial state)
            Assert.Equal(2, updates[0].AddedEndpoints.Count);
            Assert.Empty(updates[0].RemovedEndpoints);
            
            // Second update: ep2 removed
            Assert.Empty(updates[1].AddedEndpoints);
            Assert.Single(updates[1].RemovedEndpoints);
            Assert.Equal("ep2", updates[1].RemovedEndpoints[0].Id);
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

        [Fact]
        public async Task GetCurrentRegistration_ShouldReturnCachedRegistration_AfterUpdate()
        {
            // Arrange
            var service = new ServiceRegistration
            {
                Id = "test-service",
                Endpoints = new List<ServiceEndpoint>
                {
                    new ServiceEndpoint { Id = "ep1", Address = "localhost", Port = 8080, Timestamp = DateTime.UtcNow }
                }
            };

            var jsonStr = JsonSerializer.Serialize(service);
            var notification = new UpdateNotification
            {
                Key = "test-service",
                OldValue = "",
                NewValue = jsonStr
            };

            var tcs = new TaskCompletionSource<ServiceRegistration>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerable(notification), AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service");
            handler.AddListener(s => tcs.TrySetResult(s));

            // Wait for the watch task to process the update
            var timeoutTask = Task.Delay(2000);
            await Task.WhenAny(tcs.Task, timeoutTask);

            // Act
            var result = handler.GetCurrentRegistration();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(service.Id, result!.Id);
            Assert.Single(result.Endpoints);
        }

        #endregion

        #region Error Handling Tests

        [Fact]
        public async Task Handler_ShouldLogError_WhenInvalidJsonReceived()
        {
            // Arrange
            var tcs = new TaskCompletionSource<string>();
            
            var notification = new UpdateNotification
            {
                Key = "test-service",
                OldValue = "",
                NewValue = "invalid json {{{"
            };

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((CreateAsyncEnumerable(notification), AsyncEnumerable.Empty<Exception>()));

            var handler = CreateHandler("test-service", logError => tcs.TrySetResult(logError));

            // Wait for the error to be logged
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Error was not logged within timeout");
            var loggedError = await tcs.Task;
            Assert.NotNull(loggedError);
            Assert.Contains("Failed to parse service registration JSON", loggedError);
        }

        [Fact]
        public async Task Handler_ShouldLogError_WhenWatchErrorOccurs()
        {
            // Arrange
            var tcs = new TaskCompletionSource<string>();

            // Setup mock BEFORE creating handler
            _mockClient.Setup(c => c.WatchVariablesAutoReconnect(
                It.IsAny<string[]>(),
                It.IsAny<CancellationToken>()))
                .Returns((AsyncEnumerable.Empty<UpdateNotification>(), CreateAsyncEnumerableError(new Exception("Watch error"))));

            var handler = CreateHandler("test-service", logError => tcs.TrySetResult(logError));

            // Wait for the error to be logged
            var timeoutTask = Task.Delay(2000);
            var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

            // Assert
            Assert.True(completedTask == tcs.Task, "Error was not logged within timeout");
            var loggedError = await tcs.Task;
            Assert.NotNull(loggedError);
            Assert.Contains("Error watching service registration", loggedError);
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
