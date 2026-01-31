// Translated from Go implementation using Claude Sonnet

using VeritasClient.Exceptions;

namespace VeritasClient.CircuitBreaker;

public class EndpointSelector
{
    private readonly int _endpointCount;
    private readonly int _failureThreshold;
    private readonly int _successThreshold;
    private readonly TimeSpan _openTimeout;
    private readonly EndpointState[] _states;
    private readonly object _lock = new();

    public EndpointSelector(int endpointCount, int failureThreshold, int successThreshold, TimeSpan openTimeout)
    {
        _endpointCount = endpointCount;
        _failureThreshold = failureThreshold;
        _successThreshold = successThreshold;
        _openTimeout = openTimeout;
        _states = new EndpointState[endpointCount];

        for (int i = 0; i < endpointCount; i++)
        {
            _states[i] = new EndpointState();
        }
    }

    /// <summary>
    /// Returns a functioning Endpoint that should be used.
    /// </summary>
    /// <returns>The id of the endpoint that should be used.</returns>
    /// <exception cref="NoHealthyEndpointException">Is thrown if no healthy endpoints are available.</exception>
    public int Get()
    {
        // Its more important that this is correct than it is that it is fast.
        lock (_lock)
        {
            // Try to find a healthy endpoint
            for (int i = 0; i < _endpointCount; i++)
            {
                var state = _states[i];
                
                // Check if circuit breaker should transition from open to half-open
                if (state.State == CircuitState.Open && 
                    DateTime.UtcNow - state.OpenedAt >= _openTimeout)
                {
                    state.State = CircuitState.HalfOpen;
                    state.SuccessCount = 0;
                }

                // Return first closed or half-open endpoint
                if (state.State == CircuitState.Closed || state.State == CircuitState.HalfOpen)
                {
                    return i;
                }
            }

            throw new NoHealthyEndpointException();
        }
    }

    /// <summary>
    /// Resets the failure count and records a successful call for the specified endpoint. If the circuit is half-open
    /// and the number of consecutive successes meets the configured threshold, the circuit transitions to the closed
    /// state.
    /// </summary>
    /// <remarks>Call this method after a successful operation to update the circuit breaker state for the
    /// given endpoint. This is typically used in conjunction with failure tracking to manage circuit breaker
    /// transitions.</remarks>
    /// <param name="endpointIndex">The zero-based index of the endpoint for which the successful call is being recorded.</param>
    public void OnSuccess(int endpointIndex)
    {
        lock (_lock)
        {
            var state = _states[endpointIndex];
            state.FailureCount = 0;
            state.SuccessCount++;

            if (state.State == CircuitState.HalfOpen && state.SuccessCount >= _successThreshold)
            {
                state.State = CircuitState.Closed;
                state.SuccessCount = 0;
            }
        }
    }

    /// <summary>
    /// Records a failure for the specified endpoint and updates its circuit breaker state if the failure threshold is
    /// reached.
    /// </summary>
    /// <remarks>If the number of consecutive failures for the endpoint reaches the configured failure
    /// threshold, the circuit breaker for that endpoint transitions to the open state. This prevents further operations
    /// from being attempted on the endpoint until the circuit resets.</remarks>
    /// <param name="endpointIndex">The zero-based index of the endpoint for which the failure is being recorded. Must be within the valid range of
    /// configured endpoints.</param>
    public void OnFailure(int endpointIndex)
    {
        lock (_lock)
        {
            var state = _states[endpointIndex];
            state.FailureCount++;
            state.SuccessCount = 0;

            if (state.FailureCount >= _failureThreshold)
            {
                state.State = CircuitState.Open;
                state.OpenedAt = DateTime.UtcNow;
                state.FailureCount = 0;
            }
        }
    }

    private class EndpointState
    {
        public CircuitState State { get; set; } = CircuitState.Closed;
        public int FailureCount { get; set; }
        public int SuccessCount { get; set; }
        public DateTime OpenedAt { get; set; }
    }

    private enum CircuitState
    {
        Closed,
        Open,
        HalfOpen
    }
}
