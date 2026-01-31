using System.Net;
using Moq;
using Moq.Contrib.HttpClient;
using VeritasClient.Exceptions;

namespace VeritasClientTests
{
    public class VeritasClientTests
    {
        private readonly Mock<HttpMessageHandler> _mockHttpHandler;
        private readonly HttpClient _httpClient;

        public VeritasClientTests()
        {
            _mockHttpHandler = new Mock<HttpMessageHandler>();
            _httpClient = _mockHttpHandler.CreateClient();
            // Set base address to help with relative URIs
            _httpClient.BaseAddress = new Uri("http://localhost:8080/");
        }

        #region Constructor Tests

        [Fact]
        public void Constructor_ShouldThrowArgumentException_WhenNoEndpointsProvided()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new VeritasClient.VeritasClient(
                    Array.Empty<string>(),
                    TimeSpan.FromSeconds(5)));
        }

        [Fact]
        public void Constructor_ShouldThrowArgumentException_WhenEndpointsIsNull()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new VeritasClient.VeritasClient(
                    null!,
                    TimeSpan.FromSeconds(5)));
        }

        [Fact]
        public void Constructor_ShouldSucceed_WithValidEndpoints()
        {
            // Act
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Assert
            Assert.NotNull(client);
        }

        #endregion

        #region GetVariableAsync Tests

        [Fact]
        public async Task GetVariableAsync_ShouldReturnValue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("testValue");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
        }

        [Fact]
        public async Task GetVariableAsync_ShouldThrowInvalidNameException_WhenNameIsInvalid()
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.GetVariableAsync("invalid key with spaces"));
        }

        [Fact]
        public async Task GetVariableAsync_ShouldThrowInvalidNameException_WhenNameIsEmpty()
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.GetVariableAsync(""));
        }

        [Fact]
        public async Task GetVariableAsync_ShouldRetryAndSucceed_WhenFirstRequestFails()
        {
            // Arrange
            var attempts = 0;
            _mockHttpHandler
                .SetupAnyRequest()
                .Returns(() =>
                {
                    attempts++;
                    if (attempts == 1)
                    {
                        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError));
                    }
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("testValue")
                    });
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
        }

        [Fact]
        public async Task GetVariableAsync_ShouldThrowMaxRetriesReachedException_WhenAllRetriesFail()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse(HttpStatusCode.InternalServerError);

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<MaxRetriesReachedException>(() =>
                client.GetVariableAsync("testKey"));
        }

        #endregion

        #region GetVariableEventualAsync Tests

        [Fact]
        public async Task GetVariableEventualAsync_ShouldReturnValue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("testValue");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableEventualAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
        }

        [Fact]
        public async Task GetVariableEventualAsync_ShouldThrowInvalidNameException_WhenNameIsInvalid()
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.GetVariableEventualAsync("invalid/name"));
        }

        #endregion

        #region PeekVariableAsync Tests

        [Fact]
        public async Task PeekVariableAsync_ShouldReturnValue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("testValue");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.PeekVariableAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
        }

        #endregion

        #region SetVariableAsync Tests

        [Fact]
        public async Task SetVariableAsync_ShouldReturnTrue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("true");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.SetVariableAsync("testKey", "testValue");

            // Assert
            Assert.True(result);
        }

        [Fact]
        public async Task SetVariableAsync_ShouldReturnFalse_WhenRequestReturnsFalse()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("false");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.SetVariableAsync("testKey", "testValue");

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task SetVariableAsync_ShouldThrowInvalidNameException_WhenNameIsInvalid()
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.SetVariableAsync("invalid key!", "value"));
        }

        #endregion

        #region AppendVariableAsync Tests

        [Fact]
        public async Task AppendVariableAsync_ShouldReturnTrue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("true");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.AppendVariableAsync("testKey", "appendedValue");

            // Assert
            Assert.True(result);
        }

        #endregion

        #region ReplaceVariableAsync Tests

        [Fact]
        public async Task ReplaceVariableAsync_ShouldReturnTrue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("true");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.ReplaceVariableAsync("testKey", "oldValue", "newValue");

            // Assert
            Assert.True(result);
        }

        [Fact]
        public async Task ReplaceVariableAsync_ShouldReturnFalse_WhenOldValueDoesNotMatch()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("false");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.ReplaceVariableAsync("testKey", "wrongOldValue", "newValue");

            // Assert
            Assert.False(result);
        }

        #endregion

        #region CompareAndSetVariableAsync Tests

        [Fact]
        public async Task CompareAndSetVariableAsync_ShouldReturnTrue_WhenExpectedValueMatches()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("true");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.CompareAndSetVariableAsync("testKey", "expectedValue", "newValue");

            // Assert
            Assert.True(result);
        }

        [Fact]
        public async Task CompareAndSetVariableAsync_ShouldReturnFalse_WhenExpectedValueDoesNotMatch()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("false");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.CompareAndSetVariableAsync("testKey", "wrongExpectedValue", "newValue");

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task CompareAndSetVariableAsync_ShouldThrowInvalidNameException_WhenNameIsInvalid()
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.CompareAndSetVariableAsync("invalid@name", "expected", "new"));
        }

        #endregion

        #region GetAndAddVariableAsync Tests

        [Fact]
        public async Task GetAndAddVariableAsync_ShouldReturnLongValue_WhenRequestSucceeds()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("42");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetAndAddVariableAsync("counter");

            // Assert
            Assert.Equal(42, result);
        }

        [Fact]
        public async Task GetAndAddVariableAsync_ShouldThrowFormatException_WhenResponseIsNotNumeric()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("not a number");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<FormatException>(() =>
                client.GetAndAddVariableAsync("counter"));
        }

        #endregion

        #region Multiple Endpoints Tests

        [Fact]
        public async Task GetVariableAsync_ShouldUseSecondEndpoint_WhenFirstEndpointFails()
        {
            // Arrange
            var callCount = 0;
            _mockHttpHandler
                .SetupAnyRequest()
                .Returns(() =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError));
                    }
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("testValue")
                    });
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "endpoint1:8080", "endpoint2:8080" },
                TimeSpan.FromSeconds(5),
                failureThreshold: 1,
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
            Assert.True(callCount >= 2, "Should have tried multiple endpoints");
        }

        #endregion

        #region Circuit Breaker Integration Tests

        [Fact]
        public async Task GetVariableAsync_ShouldOpenCircuit_AfterRepeatedFailures()
        {
            // Arrange
            var callCount = 0;
            _mockHttpHandler
                .SetupAnyRequest()
                .Returns(() =>
                {
                    callCount++;
                    // First 2 calls fail (opening circuit for endpoint1)
                    if (callCount <= 2)
                    {
                        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError));
                    }
                    // Subsequent calls succeed (using endpoint2)
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("testValue")
                    });
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "endpoint1:8080", "endpoint2:8080" },
                TimeSpan.FromSeconds(5),
                failureThreshold: 2,
                successThreshold: 2,
                openTimeout: TimeSpan.FromMilliseconds(100),
                httpClient: _httpClient);

            // Act - Cause endpoint1 to fail twice (opening the circuit)
            await client.GetVariableAsync("testKey");
            await client.GetVariableAsync("testKey");

            // Further requests should use endpoint2
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("testValue", result);
        }

        #endregion

        #region Edge Case Tests

        [Fact]
        public async Task GetVariableAsync_ShouldHandleEmptyResponse()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("", result);
        }

        [Fact]
        public async Task SetVariableAsync_ShouldHandleEmptyValue()
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("true");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.SetVariableAsync("testKey", "");

            // Assert
            Assert.True(result);
        }

        [Theory]
        [InlineData("valid-key")]
        [InlineData("valid_key")]
        [InlineData("validkey123")]
        [InlineData("123")]
        [InlineData("a")]
        public async Task GetVariableAsync_ShouldAcceptValidNames(string validName)
        {
            // Arrange
            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("value");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync(validName);

            // Assert
            Assert.Equal("value", result);
        }

        [Theory]
        [InlineData("invalid key")]
        [InlineData("invalid/key")]
        [InlineData("invalid?key")]
        [InlineData("invalid&key")]
        [InlineData("")]
        public async Task GetVariableAsync_ShouldRejectInvalidNames(string invalidName)
        {
            // Arrange
            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidNameException>(() =>
                client.GetVariableAsync(invalidName));
        }

        #endregion

        #region Cancellation Tests

        [Fact]
        public async Task GetVariableAsync_ShouldRespectCancellationToken()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            cts.Cancel();

            _mockHttpHandler
                .SetupAnyRequest()
                .ReturnsResponse("value");

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act & Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.GetVariableAsync("testKey", cts.Token));
        }

        #endregion

        #region Retry Logic Tests

        [Fact]
        public async Task GetVariableAsync_ShouldRetryWithExponentialBackoff()
        {
            // Arrange
            var attempts = 0;
            var attemptTimes = new List<DateTime>();
            var lockObj = new object();

            _mockHttpHandler
                .SetupAnyRequest()
                .Returns(() =>
                {
                    lock (lockObj)
                    {
                        attempts++;
                        attemptTimes.Add(DateTime.UtcNow);

                        if (attempts < 3)
                        {
                            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError));
                        }

                        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new StringContent("success")
                        });
                    }
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(10),
                httpClient: _httpClient);

            // Act
            var result = await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal("success", result);
            Assert.Equal(3, attempts);

            // Verify exponential backoff (second attempt should be delayed more than first retry)
            if (attemptTimes.Count >= 3)
            {
                var firstRetryDelay = attemptTimes[1] - attemptTimes[0];
                var secondRetryDelay = attemptTimes[2] - attemptTimes[1];
                Assert.True(secondRetryDelay > firstRetryDelay,
                    "Second retry delay should be greater than first retry delay (exponential backoff)");
            }
        }

        #endregion

        #region HTTP Method and Path Verification Tests

        [Fact]
        public async Task SetVariableAsync_ShouldUsePutMethodAndCorrectPath()
        {
            // Arrange
            HttpMethod? capturedMethod = null;
            string? capturedPath = null;

            _mockHttpHandler
                .SetupAnyRequest()
                .Returns((HttpRequestMessage req, CancellationToken _) =>
                {
                    capturedMethod = req.Method;
                    capturedPath = req.RequestUri?.PathAndQuery;
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("true")
                    });
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            await client.SetVariableAsync("testKey", "testValue");

            // Assert
            Assert.Equal(HttpMethod.Put, capturedMethod);
            Assert.Contains("/set/testKey", capturedPath);
        }

        [Fact]
        public async Task GetVariableAsync_ShouldUseGetMethodAndCorrectPath()
        {
            // Arrange
            HttpMethod? capturedMethod = null;
            string? capturedPath = null;

            _mockHttpHandler
                .SetupAnyRequest()
                .Returns((HttpRequestMessage req, CancellationToken _) =>
                {
                    capturedMethod = req.Method;
                    capturedPath = req.RequestUri?.PathAndQuery;
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("value")
                    });
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            await client.GetVariableAsync("testKey");

            // Assert
            Assert.Equal(HttpMethod.Get, capturedMethod);
            Assert.Contains("/get/testKey", capturedPath);
        }

        [Fact]
        public async Task CompareAndSetVariableAsync_ShouldSendCorrectBodyFormat()
        {
            // Arrange
            string? capturedBody = null;

            _mockHttpHandler
                .SetupAnyRequest()
                .Returns(async (HttpRequestMessage req, CancellationToken _) =>
                {
                    if (req.Content != null)
                    {
                        capturedBody = await req.Content.ReadAsStringAsync();
                    }
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("true")
                    };
                });

            var client = new VeritasClient.VeritasClient(
                new[] { "localhost:8080" },
                TimeSpan.FromSeconds(5),
                httpClient: _httpClient);

            // Act
            await client.CompareAndSetVariableAsync("testKey", "oldValue", "newValue");

            // Assert
            Assert.NotNull(capturedBody);
            // Format should be: "{expectedLength};{expectedValue}{newValue}"
            Assert.StartsWith("8;", capturedBody); // "oldValue" has length 8
            Assert.Contains("oldValue", capturedBody);
            Assert.Contains("newValue", capturedBody);
        }

        #endregion
    }
}