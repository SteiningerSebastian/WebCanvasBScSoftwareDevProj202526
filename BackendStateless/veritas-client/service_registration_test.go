package veritasclient

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// MockVeritasClient for testing
type MockVeritasClient struct {
	storage       map[string]string
	watchChannels []chan UpdateNotification
	mu            sync.RWMutex
}

func NewMockVeritasClient() *MockVeritasClient {
	return &MockVeritasClient{
		storage:       make(map[string]string),
		watchChannels: make([]chan UpdateNotification, 0),
	}
}

func (m *MockVeritasClient) GetVariable(ctx context.Context, name string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if val, exists := m.storage[name]; exists {
		return val, nil
	}
	return "", fmt.Errorf("variable not found: %s", name)
}

func (m *MockVeritasClient) GetVariableEventual(ctx context.Context, name string) (string, error) {
	return m.GetVariable(ctx, name)
}

func (m *MockVeritasClient) PeekVariable(ctx context.Context, name string) (string, error) {
	return m.GetVariable(ctx, name)
}

func (m *MockVeritasClient) SetVariable(ctx context.Context, name string, value string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storage[name] = value
	return true, nil
}

func (m *MockVeritasClient) AppendVariable(ctx context.Context, name string, value string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.storage[name]
	m.storage[name] = current + value
	return true, nil
}

func (m *MockVeritasClient) ReplaceVariable(ctx context.Context, name string, oldValue string, newValue string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, exists := m.storage[name]; exists {
		m.storage[name] = current // In real implementation would replace oldValue with newValue
		return true, nil
	}
	return false, nil
}

func (m *MockVeritasClient) CompareAndSetVariable(ctx context.Context, name string, expectedValue string, newValue string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.storage[name]
	if current == expectedValue {
		m.storage[name] = newValue
		return true, nil
	}
	return false, nil
}

func (m *MockVeritasClient) GetAndAddVariable(ctx context.Context, name string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := int64(0)
	if val, exists := m.storage[name]; exists {
		fmt.Sscanf(val, "%d", &current)
	}
	m.storage[name] = fmt.Sprintf("%d", current+1)
	return current, nil
}

func (m *MockVeritasClient) WatchVariables(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error, error) {
	updateChan := make(chan UpdateNotification, 10)
	errChan := make(chan error, 10)

	m.mu.Lock()
	m.watchChannels = append(m.watchChannels, updateChan)
	m.mu.Unlock()

	return updateChan, errChan, nil
}

func (m *MockVeritasClient) WatchVariablesAutoReconnect(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error) {
	updateChan, errChan, _ := m.WatchVariables(ctx, names)
	return updateChan, errChan
}

func (m *MockVeritasClient) TriggerWatchUpdate(key string, newValue string) {
	m.mu.RLock()
	channels := m.watchChannels
	m.mu.RUnlock()

	notification := UpdateNotification{
		Key:      key,
		NewValue: newValue,
		OldValue: "",
	}

	for _, ch := range channels {
		select {
		case ch <- notification:
		default:
			// Channel is full or closed, skip
		}
	}
}

func TestResolveService(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Setup initial service registration
	service := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-1",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	jsonBytes, err := json.Marshal(&service)
	if err != nil {
		t.Fatalf("Failed to marshal service: %v", err)
	}
	_, err = mockClient.SetVariable(ctx, "test-service", string(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to set variable: %v", err)
	}

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Test resolve_service
	resolved, err := handler.ResolveService(ctx)
	if err != nil {
		t.Fatalf("Failed to resolve service: %v", err)
	}

	if resolved.ID != "test-service" {
		t.Errorf("Expected ID 'test-service', got '%s'", resolved.ID)
	}
	if len(resolved.Endpoints) != 1 {
		t.Errorf("Expected 1 endpoint, got %d", len(resolved.Endpoints))
	}
	if resolved.Endpoints[0].Address != "127.0.0.1" {
		t.Errorf("Expected address '127.0.0.1', got '%s'", resolved.Endpoints[0].Address)
	}
	if resolved.Endpoints[0].Port != 8080 {
		t.Errorf("Expected port 8080, got %d", resolved.Endpoints[0].Port)
	}
}

func TestUpdateService(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Setup initial service registration
	oldService := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-1",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	oldJSON, err := json.Marshal(&oldService)
	if err != nil {
		t.Fatalf("Failed to marshal old service: %v", err)
	}
	_, err = mockClient.SetVariable(ctx, "test-service", string(oldJSON))
	if err != nil {
		t.Fatalf("Failed to set variable: %v", err)
	}

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Update service
	newService := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-2",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
			{
				ID:        "endpoint-3",
				Address:   "127.0.0.1",
				Port:      8081,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	err = handler.RegisterOrUpdateService(ctx, &newService, &oldService)
	if err != nil {
		t.Fatalf("Failed to update service: %v", err)
	}

	// Verify update
	resolved, err := handler.ResolveService(ctx)
	if err != nil {
		t.Fatalf("Failed to resolve service: %v", err)
	}
	if len(resolved.Endpoints) != 2 {
		t.Errorf("Expected 2 endpoints, got %d", len(resolved.Endpoints))
	}
}

func TestRegisterEndpoint(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Setup initial service registration
	service := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-1",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	jsonBytes, err := json.Marshal(&service)
	if err != nil {
		t.Fatalf("Failed to marshal service: %v", err)
	}
	_, err = mockClient.SetVariable(ctx, "test-service", string(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to set variable: %v", err)
	}

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Register new endpoint
	newEndpoint := ServiceEndpoint{
		ID:        "endpoint-2",
		Address:   "127.0.0.1",
		Port:      9090,
		Timestamp: time.Now(),
	}

	err = handler.RegisterOrUpdateEndpoint(ctx, &newEndpoint)
	if err != nil {
		t.Fatalf("Failed to register endpoint: %v", err)
	}

	// Verify endpoint was added
	resolved, err := handler.ResolveService(ctx)
	if err != nil {
		t.Fatalf("Failed to resolve service: %v", err)
	}
	if len(resolved.Endpoints) != 2 {
		t.Errorf("Expected 2 endpoints, got %d", len(resolved.Endpoints))
	}

	foundEndpoint := false
	for _, ep := range resolved.Endpoints {
		if ep.Port == 9090 {
			foundEndpoint = true
			break
		}
	}
	if !foundEndpoint {
		t.Error("New endpoint with port 9090 was not found")
	}
}

func TestAddListener(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Add listener
	err = handler.AddListener(func(service ServiceRegistration) {
	})
	if err != nil {
		t.Fatalf("Failed to add listener: %v", err)
	}

	// Verify listener was added
	handler.mu.RLock()
	listenerCount := len(handler.listeners)
	handler.mu.RUnlock()

	if listenerCount != 1 {
		t.Errorf("Expected 1 listener, got %d", listenerCount)
	}
}

func TestAddEndpointListener(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Add endpoint listener
	err = handler.AddEndpointListener(func(update ServiceEndpointsUpdate) {
	})
	if err != nil {
		t.Fatalf("Failed to add endpoint listener: %v", err)
	}

	// Verify listener was added
	handler.mu.RLock()
	listenerCount := len(handler.endpointListeners)
	handler.mu.RUnlock()

	if listenerCount != 1 {
		t.Errorf("Expected 1 endpoint listener, got %d", listenerCount)
	}
}

func TestServiceEndpointEquality(t *testing.T) {
	now := time.Now()
	endpoint1 := ServiceEndpoint{
		ID:        "endpoint-1",
		Address:   "127.0.0.1",
		Port:      8080,
		Timestamp: now,
	}

	endpoint2 := ServiceEndpoint{
		ID:        "endpoint-2",
		Address:   "127.0.0.1",
		Port:      8080,
		Timestamp: now,
	}

	endpoint3 := ServiceEndpoint{
		ID:        "endpoint-3",
		Address:   "127.0.0.1",
		Port:      9090,
		Timestamp: now,
	}

	if !endpoint1.Equals(&endpoint2) {
		t.Error("endpoint1 and endpoint2 should be equal")
	}
	if endpoint1.Equals(&endpoint3) {
		t.Error("endpoint1 and endpoint3 should not be equal")
	}
}

func TestUpdateServiceWithoutOldService(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Create handler without initial service
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Create new service
	newService := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-1",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	// Update service without old service
	err = handler.RegisterOrUpdateService(ctx, &newService, nil)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	// Verify service was created
	resolved, err := handler.ResolveService(ctx)
	if err != nil {
		t.Fatalf("Failed to resolve service: %v", err)
	}
	if resolved.ID != "test-service" {
		t.Errorf("Expected ID 'test-service', got '%s'", resolved.ID)
	}
	if len(resolved.Endpoints) != 1 {
		t.Errorf("Expected 1 endpoint, got %d", len(resolved.Endpoints))
	}
}

func TestRegisterEndpointErrorWhenServiceNotFound(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Create handler without setting up initial service
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "nonexistent-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Try to register endpoint
	newEndpoint := ServiceEndpoint{
		ID:        "endpoint-2",
		Address:   "127.0.0.1",
		Port:      9090,
		Timestamp: time.Now(),
	}

	err = handler.RegisterOrUpdateEndpoint(ctx, &newEndpoint)
	if err == nil {
		t.Error("Expected error when registering endpoint for non-existent service")
	}
}

func TestWatchUpdatesNotifyListeners(t *testing.T) {
	mockClient := NewMockVeritasClient()
	ctx := context.Background()

	// Setup initial service registration
	service := ServiceRegistration{
		ID: "test-service",
		Endpoints: []ServiceEndpoint{
			{
				ID:        "endpoint-1",
				Address:   "127.0.0.1",
				Port:      8080,
				Timestamp: time.Now(),
			},
		},
		Meta: make(map[string]string),
	}

	jsonBytes, err := json.Marshal(&service)
	if err != nil {
		t.Fatalf("Failed to marshal service: %v", err)
	}
	_, err = mockClient.SetVariable(ctx, "test-service", string(jsonBytes))
	if err != nil {
		t.Fatalf("Failed to set variable: %v", err)
	}

	// Create handler
	handler, err := NewServiceRegistrationHandler(ctx, mockClient, "test-service")
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	defer handler.Close()

	// Add listener
	listenerCalled := make(chan bool, 1)
	err = handler.AddListener(func(service ServiceRegistration) {
		listenerCalled <- true
	})
	if err != nil {
		t.Fatalf("Failed to add listener: %v", err)
	}

	// Update service and trigger watch update
	updatedService := service
	updatedService.Endpoints = append(updatedService.Endpoints, ServiceEndpoint{
		ID:        "endpoint-2",
		Address:   "127.0.0.1",
		Port:      8081,
		Timestamp: time.Now(),
	})

	updatedJSON, err := json.Marshal(&updatedService)
	if err != nil {
		t.Fatalf("Failed to marshal updated service: %v", err)
	}

	mockClient.TriggerWatchUpdate("test-service", string(updatedJSON))

	// Wait for listener to be called
	select {
	case <-listenerCalled:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Listener was not called within timeout")
	}
}
