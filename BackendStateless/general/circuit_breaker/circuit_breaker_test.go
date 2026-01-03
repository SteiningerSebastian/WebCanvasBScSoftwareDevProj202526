package circuitbreaker

import (
	"testing"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 5*time.Second)
	if cb.GetState() != Closed {
		t.Errorf("expected initial state to be Closed, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_OnFailure_TransitionsToOpen(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 5*time.Second)
	cb.OnFailure()
	cb.OnFailure()
	cb.OnFailure()

	if cb.GetState() != Open {
		t.Errorf("expected state to be Open after reaching failure threshold, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_OpenToHalfOpenAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 1*time.Second)
	cb.OnFailure()
	cb.OnFailure()
	cb.OnFailure()

	time.Sleep(2 * time.Second)
	cb.OnSuccess()

	if cb.GetState() != HalfOpen {
		t.Errorf("expected state to transition to HalfOpen after timeout, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpenToClosedOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 1*time.Second)
	cb.OnFailure()
	cb.OnFailure()
	cb.OnFailure()

	time.Sleep(2 * time.Second)
	cb.OnSuccess()
	cb.OnSuccess()

	if cb.GetState() != Closed {
		t.Errorf("expected state to transition to Closed after success threshold, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpenToOpenOnFailure(t *testing.T) {
	cb := NewCircuitBreaker(3, 2, 1*time.Second)
	cb.OnFailure()
	cb.OnFailure()
	cb.OnFailure()

	time.Sleep(2 * time.Second)
	cb.OnSuccess()
	cb.OnFailure()

	if cb.GetState() != Open {
		t.Errorf("expected state to transition to Open after failure in HalfOpen, got %v", cb.GetState())
	}
}
