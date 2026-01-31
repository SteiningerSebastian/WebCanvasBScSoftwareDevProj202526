using VeritasClient.CircuitBreaker;
using VeritasClient.Exceptions;

namespace VeritasClientTests
{
    public class TestCircuitBreaker
    {
        [Fact]
        public void Get_ShouldReturnFirstEndpoint_WhenAllAreHealthy()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 3,
                failureThreshold: 3,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Act
            var endpoint = selector.Get();

            // Assert
            Assert.Equal(0, endpoint);
        }

        [Fact]
        public void Get_ShouldSkipOpenEndpoint_AndReturnNextHealthyOne()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 3,
                failureThreshold: 2,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Open the first endpoint by failing it twice
            selector.OnFailure(0);
            selector.OnFailure(0);

            // Act
            var endpoint = selector.Get();

            // Assert
            Assert.Equal(1, endpoint); // Should skip endpoint 0 and return endpoint 1
        }

        [Fact]
        public void Get_ShouldThrowException_WhenAllEndpointsAreOpen()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 2,
                failureThreshold: 1,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Open all endpoints
            selector.OnFailure(0);
            selector.OnFailure(1);

            // Act & Assert
            Assert.Throws<NoHealthyEndpointException>(() => selector.Get());
        }

        [Fact]
        public void OnFailure_ShouldOpenCircuit_WhenThresholdReached()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 2,
                failureThreshold: 3,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Act - Fail 3 times to reach threshold
            selector.OnFailure(0);
            selector.OnFailure(0);
            selector.OnFailure(0);

            // Assert - Should skip endpoint 0 and return 1
            var endpoint = selector.Get();
            Assert.Equal(1, endpoint);
        }

        [Fact]
        public void OnSuccess_ShouldResetFailureCount()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 1,
                failureThreshold: 3,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Act - Fail twice, then succeed
            selector.OnFailure(0);
            selector.OnFailure(0);
            selector.OnSuccess(0);
            selector.OnFailure(0); // This should be the first failure after reset

            // Assert - Circuit should still be closed
            var endpoint = selector.Get();
            Assert.Equal(0, endpoint);
        }

        [Fact]
        public void CircuitBreaker_ShouldTransitionToHalfOpen_AfterTimeout()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 1,
                failureThreshold: 1,
                successThreshold: 2,
                openTimeout: TimeSpan.FromMilliseconds(100)
            );

            // Open the circuit
            selector.OnFailure(0);

            // Verify circuit is open
            Assert.Throws<NoHealthyEndpointException>(() => selector.Get());

            // Act - Wait for timeout
            Thread.Sleep(150);

            // Assert - Should be able to get endpoint (now in half-open state)
            var endpoint = selector.Get();
            Assert.Equal(0, endpoint);
        }

        [Fact]
        public void HalfOpenCircuit_ShouldTransitionToClosed_AfterSuccessThreshold()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 1,
                failureThreshold: 2,
                successThreshold: 3,
                openTimeout: TimeSpan.FromMilliseconds(100)
            );

            // Open the circuit (2 failures)
            selector.OnFailure(0);
            selector.OnFailure(0);

            // Wait for half-open
            Thread.Sleep(150);

            // Act - Succeed 3 times to close the circuit
            selector.Get(); // Gets in half-open state
            selector.OnSuccess(0);
            selector.OnSuccess(0);
            selector.OnSuccess(0);

            // Assert - Circuit should be closed, can fail once without opening (needs 2 failures to open)
            selector.OnFailure(0);
            var endpoint = selector.Get();
            Assert.Equal(0, endpoint);
        }

        [Fact]
        public void HalfOpenCircuit_ShouldResetSuccessCount_OnFailure()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 2,
                failureThreshold: 1,
                successThreshold: 2,
                openTimeout: TimeSpan.FromMilliseconds(100)
            );

            // Open the circuit for endpoint 0
            selector.OnFailure(0);

            // Wait for half-open
            Thread.Sleep(150);

            // Act - One success, then a failure (should reset success count and open circuit again)
            selector.Get(); // Gets endpoint 0 in half-open state
            selector.OnSuccess(0);
            selector.OnFailure(0);

            // Assert - Should skip endpoint 0 (now open again) and return endpoint 1
            var endpoint = selector.Get();
            Assert.Equal(1, endpoint);
        }

        [Fact]
        public void MultipleEndpoints_ShouldRotateToHealthyOnes()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 3,
                failureThreshold: 1,
                successThreshold: 2,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            // Act & Assert - Open endpoints one by one
            Assert.Equal(0, selector.Get());
            selector.OnFailure(0);

            Assert.Equal(1, selector.Get());
            selector.OnFailure(1);

            Assert.Equal(2, selector.Get());
            selector.OnFailure(2);

            // All endpoints open
            Assert.Throws<NoHealthyEndpointException>(() => selector.Get());
        }

        [Fact]
        public void Get_ShouldBeThreadSafe()
        {
            // Arrange
            var selector = new EndpointSelector(
                endpointCount: 2,
                failureThreshold: 5,
                successThreshold: 3,
                openTimeout: TimeSpan.FromSeconds(5)
            );

            var exceptions = new List<Exception>();
            var threads = new List<Thread>();

            // Act - Multiple threads accessing concurrently
            for (int i = 0; i < 10; i++)
            {
                var thread = new Thread(() =>
                {
                    try
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            var endpoint = selector.Get();
                            if (j % 3 == 0)
                            {
                                selector.OnFailure(endpoint);
                            }
                            else
                            {
                                selector.OnSuccess(endpoint);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions)
                        {
                            exceptions.Add(ex);
                        }
                    }
                });
                threads.Add(thread);
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            // Assert - Should not have any unexpected exceptions (only NoHealthyEndpointException is expected)
            var unexpectedExceptions = exceptions.Where(e => e is not NoHealthyEndpointException).ToList();
            Assert.Empty(unexpectedExceptions);
        }
    }
}
