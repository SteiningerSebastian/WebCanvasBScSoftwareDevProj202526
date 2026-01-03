package circuitbreaker

import (
	"testing"
	"time"
)

func TestEndpointSelector_InitialSelection(t *testing.T) {
	es := NewEndpointSelector(3, 3, 2, 5*time.Second)
	selected, err := es.Get()
	if err != nil {
		t.Errorf("expected no error on initial selection, got %v", err)
	}
	if selected != 0 {
		t.Errorf("expected initial selection to be 0, got %d", selected)
	}
}

func TestEndpointSelector_RoundRobinSelection(t *testing.T) {
	es := NewEndpointSelector(3, 3, 2, 5*time.Second)
	es.OnFailure(0)
	es.OnFailure(0)
	es.OnFailure(0)

	selected, err := es.Get()
	if err != nil {
		t.Errorf("expected no error on round-robin selection, got %v", err)
	}
	if selected != 1 {
		t.Errorf("expected selection to move to 1, got %d", selected)
	}
}

func TestEndpointSelector_NoHealthyEndpoint(t *testing.T) {
	es := NewEndpointSelector(2, 1, 1, 1*time.Second)
	es.OnFailure(0)
	es.OnFailure(1)

	_, err := es.Get()
	if err == nil {
		t.Errorf("expected error when no healthy endpoint is available, got nil")
	}
}

func TestEndpointSelector_OpenTimeoutRecovery(t *testing.T) {
	es := NewEndpointSelector(2, 1, 1, 1*time.Second)
	es.OnFailure(0)
	es.OnFailure(1)

	time.Sleep(2 * time.Second)
	selected, err := es.Get()
	if err != nil {
		t.Errorf("expected no error after open timeout recovery, got %v", err)
	}
	if selected != 0 {
		t.Errorf("expected selection to recover to 0, got %d", selected)
	}
}

func TestEndpointSelector_OnSuccessUpdatesBreaker(t *testing.T) {
	es := NewEndpointSelector(1, 1, 1, 1*time.Second)

	// Transition the breaker to Open state
	es.OnFailure(0)
	es.OnFailure(0)

	// Wait for the open timeout to elapse
	time.Sleep(2 * time.Second)

	// Call OnSuccess to transition to HalfOpen
	es.OnSuccess(0)
	if es.breakers[0].GetState() != HalfOpen {
		t.Errorf("expected breaker to transition to HalfOpen on success, got %v", es.breakers[0].GetState())
	}
}
