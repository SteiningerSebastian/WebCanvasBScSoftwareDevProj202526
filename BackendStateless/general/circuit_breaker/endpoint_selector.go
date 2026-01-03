package circuitbreaker

import (
	"sync"
	"time"
)

// EndpointSelector manages circuit breakers for multiple endpoints.
// While we could implement the circuit breaker logic using atomic operations for performance,
// we choose to use a mutex for simplicity and clarity in this implementation.
// The performance impact is negligible for our use case.
type EndpointSelector struct {
	breakers      []*CircuitBreaker
	failureThresh int32
	successThresh int32
	openTimeout   time.Duration
	selected      int
	lock          sync.Mutex
}

type NoHealthyEndpointError struct{}

func (e *NoHealthyEndpointError) Error() string {
	return "no healthy endpoint available"
}

// NewEndpointSelector creates an EndpointSelector managing circuit breakers for multiple endpoints.
func NewEndpointSelector(numEndpoints int, failureThresh, successThresh int32, openTimeout time.Duration) *EndpointSelector {
	breakers := make([]*CircuitBreaker, numEndpoints)
	for i := 0; i < numEndpoints; i++ {
		breakers[i] = NewCircuitBreaker(failureThresh, successThresh, openTimeout)
	}

	return &EndpointSelector{
		breakers:      breakers,
		failureThresh: failureThresh,
		successThresh: successThresh,
		openTimeout:   openTimeout,
	}
}

// Get selected endpoint
func (es *EndpointSelector) Get() (int, error) {
	es.lock.Lock()
	defer es.lock.Unlock()

	// if the currently selected endpoint is healthy, return it
	if es.breakers[es.selected].state != Open {
		return es.selected, nil
	}

	// Simple round-robin selection with circuit breaker check
	for i, cb := range es.breakers {
		if cb.state != Open {
			es.selected = i
			return i, nil
		}
	}

	// Find one where the open timeout has elapsed
	for i, cb := range es.breakers {
		if cb.state == Open && time.Since(cb.lastStateChange) > cb.openTimeout {
			es.selected = i
			return i, nil
		}
	}

	return -1, &NoHealthyEndpointError{}
}

// OnSuccess notifies the selected endpoint's circuit breaker of a successful call.
func (es *EndpointSelector) OnSuccess(n int) {
	es.breakers[n].OnSuccess()
}

// OnFailure notifies the selected endpoint's circuit breaker of a failed call.
func (es *EndpointSelector) OnFailure(n int) {
	es.breakers[n].OnFailure()
}
