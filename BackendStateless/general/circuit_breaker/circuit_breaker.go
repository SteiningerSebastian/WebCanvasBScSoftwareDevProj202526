package circuitbreaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

// While we could implement the circuit breaker logic using atomic operations for performance,
// we choose to use a mutex for simplicity and clarity in this implementation.
// The performance impact is negligible for our use case.
type CircuitBreaker struct {
	failureCount     int32         // number of consecutive failures
	successCount     int32         // number of consecutive successes in Half-Open state
	state            State         // current state of the circuit breaker
	lastStateChange  time.Time     // timestamp of the last state change
	failureThreshold int32         // threshold to trip the circuit to Open
	successThreshold int32         // threshold to close the circuit from Half-Open
	openTimeout      time.Duration // duration to wait before transitioning from Open to Half-Open
	lock             sync.RWMutex  // protects state transitions
}

func NewCircuitBreaker(failureThreshold, successThreshold int32, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		openTimeout:      openTimeout,
	}
}

func (cb *CircuitBreaker) OnSuccess() {
	cb.lock.Lock()
	defer cb.lock.Unlock()

	switch cb.state {
	case Open:
		if time.Since(cb.lastStateChange) > cb.openTimeout {
			cb.state = HalfOpen
			cb.successCount = 1
			cb.lastStateChange = time.Now()
		}
	case HalfOpen:
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = Closed
			cb.failureCount = 0
			cb.lastStateChange = time.Now()
		}
	case Closed:
		// No action needed on success when closed
	}
}

func (cb *CircuitBreaker) OnFailure() {
	cb.lock.Lock()
	defer cb.lock.Unlock()

	switch cb.state {
	case Closed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThreshold {
			cb.state = Open
			cb.lastStateChange = time.Now()
		}
	case HalfOpen:
		cb.state = Open
		cb.lastStateChange = time.Now()
	case Open:
		// No action needed on failure when open
	}
}

func (cb *CircuitBreaker) GetState() State {
	cb.lock.RLock()
	defer cb.lock.RUnlock()
	return cb.state
}
