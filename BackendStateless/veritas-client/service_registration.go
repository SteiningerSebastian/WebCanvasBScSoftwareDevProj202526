// Translated from rust implementation using Cloude Sonnet 4.5

package veritasclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Error types
var (
	ErrServiceNotFound            = errors.New("service not found")
	ErrServiceRegistrationFailed  = errors.New("service registration failed")
	ErrServiceRegistrationChanged = errors.New("service registration changed")
	ErrEndpointRegistrationFailed = errors.New("endpoint registration failed")
)

// ServiceEndpoint represents a service endpoint with address, port, and metadata
type ServiceEndpoint struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Port      uint16    `json:"port"`
	Timestamp time.Time `json:"timestamp"`
}

func (se *ServiceEndpoint) String() string {
	return fmt.Sprintf("%s:%d", se.Address, se.Port)
}

// Equals checks if two ServiceEndpoints are equal (by address and port)
func (se *ServiceEndpoint) Equals(other *ServiceEndpoint) bool {
	return se.Address == other.Address && se.Port == other.Port
}

// ServiceRegistration represents a service registration with endpoints and metadata
type ServiceRegistration struct {
	ID        string            `json:"id"`
	Endpoints []ServiceEndpoint `json:"endpoints"`
	Meta      map[string]string `json:"meta"`
}

// / Clone creates a deep copy of the ServiceRegistration
func (sr *ServiceRegistration) Clone() ServiceRegistration {
	clone := *sr
	clone.Meta = make(map[string]string)
	for k, v := range sr.Meta {
		clone.Meta[k] = v
	}
	clone.Endpoints = make([]ServiceEndpoint, len(sr.Endpoints))
	copy(clone.Endpoints, sr.Endpoints)
	return clone
}

// ServiceEndpointsUpdate represents changes in service endpoints
type ServiceEndpointsUpdate struct {
	AddedEndpoints   []ServiceEndpoint
	RemovedEndpoints []ServiceEndpoint
}

// ServiceRegistrationInterface defines the interface for service registration operations
type ServiceRegistrationInterface interface {
	// RegisterOrUpdateService registers a new service or updates an existing one with the service registry.
	// Parameters:
	//   - ctx: Context for cancellation
	//   - service: The ServiceRegistration object containing service details
	//   - oldService: The previous ServiceRegistration object for updates (nil for new registrations)
	// Returns: error if the registration fails
	RegisterOrUpdateService(ctx context.Context, service *ServiceRegistration, oldService *ServiceRegistration) error

	// RegisterOrUpdateEndpoint registers or updates a service endpoint.
	// Parameters:
	//   - ctx: Context for cancellation
	//   - endpoint: A reference to the ServiceEndpoint to be registered
	// Returns: error if the registration fails
	RegisterOrUpdateEndpoint(ctx context.Context, endpoint *ServiceEndpoint) error

	// AddListener adds a listener for service registration updates.
	// The callback will be called whenever there is a change in service registrations.
	// Parameters:
	//   - callback: A function that takes a ServiceRegistration
	// Returns: error if the listener cannot be added
	AddListener(callback func(ServiceRegistration)) error

	// ResolveService resolves the current service registration information.
	// Parameters:
	//   - ctx: Context for cancellation
	// Returns: ServiceRegistration if successful, error otherwise
	ResolveService(ctx context.Context) (*ServiceRegistration, error)

	// AddEndpointListener adds a listener for endpoint updates.
	// The callback will be called whenever there is a change in service endpoints.
	// Parameters:
	//   - callback: A function that takes a ServiceEndpointsUpdate
	// Returns: error if the listener cannot be added
	AddEndpointListener(callback func(ServiceEndpointsUpdate)) error
}

// ServiceRegistrationHandler implements ServiceRegistrationInterface
type ServiceRegistrationHandler struct {
	client              VeritasClientInterface
	serviceName         string
	listeners           []func(ServiceRegistration)
	endpointListeners   []func(ServiceEndpointsUpdate)
	currentRegistration *ServiceRegistration
	mu                  sync.RWMutex
	cancelFunc          context.CancelFunc
}

// NewServiceRegistrationHandler creates a new ServiceRegistrationHandler
func NewServiceRegistrationHandler(ctx context.Context, client VeritasClientInterface, serviceName string) (*ServiceRegistrationHandler, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	handler := &ServiceRegistrationHandler{
		client:              client,
		serviceName:         serviceName,
		listeners:           make([]func(ServiceRegistration), 0),
		endpointListeners:   make([]func(ServiceEndpointsUpdate), 0),
		currentRegistration: nil,
		cancelFunc:          cancel,
	}

	// Start watching for updates
	updateChan, errChan := client.WatchVariablesAutoReconnect(watchCtx, []string{serviceName})

	// Start goroutine to handle watch updates
	go handler.handleWatchUpdates(watchCtx, updateChan, errChan)

	return handler, nil
}

// handleWatchUpdates processes watch update notifications
func (h *ServiceRegistrationHandler) handleWatchUpdates(ctx context.Context, updateChan <-chan UpdateNotification, errChan <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			if err != nil {
				slog.Error("Error watching service registration for '" + h.serviceName + "': " + err.Error())
			}
		case update, ok := <-updateChan:
			if !ok {
				return
			}

			var serviceRegistration ServiceRegistration
			if err := json.Unmarshal([]byte(update.NewValue), &serviceRegistration); err != nil {
				slog.Error("Failed to parse service registration JSON for key '"+h.serviceName+"': "+err.Error(), "value", update.NewValue)
				continue
			}

			h.mu.Lock()
			oldRegistration := h.currentRegistration

			// Notify all listeners about the update
			for _, listener := range h.listeners {
				go listener(serviceRegistration)
			}

			// Determine added and removed endpoints
			if oldRegistration != nil {
				oldEndpointsMap := make(map[string]ServiceEndpoint)
				for _, ep := range oldRegistration.Endpoints {
					key := fmt.Sprintf("%s:%d", ep.Address, ep.Port)
					oldEndpointsMap[key] = ep
				}

				newEndpointsMap := make(map[string]ServiceEndpoint)
				for _, ep := range serviceRegistration.Endpoints {
					key := fmt.Sprintf("%s:%d", ep.Address, ep.Port)
					newEndpointsMap[key] = ep
				}

				addedEndpoints := []ServiceEndpoint{}
				for key, ep := range newEndpointsMap {
					if _, exists := oldEndpointsMap[key]; !exists {
						addedEndpoints = append(addedEndpoints, ep)
					}
				}

				removedEndpoints := []ServiceEndpoint{}
				for key, ep := range oldEndpointsMap {
					if _, exists := newEndpointsMap[key]; !exists {
						removedEndpoints = append(removedEndpoints, ep)
					}
				}

				if len(addedEndpoints) > 0 || len(removedEndpoints) > 0 {
					update := ServiceEndpointsUpdate{
						AddedEndpoints:   addedEndpoints,
						RemovedEndpoints: removedEndpoints,
					}

					for _, endpointListener := range h.endpointListeners {
						go endpointListener(update)
					}
				}
			}

			h.currentRegistration = &serviceRegistration
			h.mu.Unlock()
		}
	}
}

// RegisterOrUpdateService registers or updates a service registration
func (h *ServiceRegistrationHandler) RegisterOrUpdateService(ctx context.Context, service *ServiceRegistration, oldService *ServiceRegistration) error {
	// Convert ServiceRegistration to JSON
	jsonBytes, err := json.Marshal(service)
	if err != nil {
		return ErrServiceRegistrationFailed
	}
	jsonStr := string(jsonBytes)

	oldJSON := ""
	if oldService != nil {
		oldJSONBytes, err := json.Marshal(oldService)
		if err != nil {
			return ErrServiceRegistrationFailed
		}
		oldJSON = string(oldJSONBytes)
	}

	// Use CompareAndSetVariable to update the service registration atomically
	success, err := h.client.CompareAndSetVariable(ctx, h.serviceName, oldJSON, jsonStr)
	if err != nil {
		return ErrServiceRegistrationFailed
	}
	if !success {
		return ErrServiceRegistrationChanged
	}

	h.mu.Lock()
	h.currentRegistration = service
	h.mu.Unlock()

	return nil
}

// RegisterOrUpdateEndpoint registers or updates a service endpoint
func (h *ServiceRegistrationHandler) RegisterOrUpdateEndpoint(ctx context.Context, endpoint *ServiceEndpoint) error {
	// Get current service registration
	current, err := h.ResolveService(ctx)
	if err != nil {
		return err
	}

	// Create updated service registration
	updated := *current
	updated.Endpoints = make([]ServiceEndpoint, len(current.Endpoints))
	copy(updated.Endpoints, current.Endpoints)

	// Check if endpoint already exists
	found := false
	for i, ep := range updated.Endpoints {
		if ep.ID == endpoint.ID {
			updated.Endpoints[i] = *endpoint
			found = true
			break
		}
	}

	if !found {
		updated.Endpoints = append(updated.Endpoints, *endpoint)
	}

	// Convert to JSON
	jsonBytes, err := json.Marshal(&updated)
	if err != nil {
		return ErrEndpointRegistrationFailed
	}
	jsonStr := string(jsonBytes)

	oldJSONBytes, err := json.Marshal(current)
	if err != nil {
		return ErrEndpointRegistrationFailed
	}
	oldJSON := string(oldJSONBytes)

	// Use CompareAndSetVariable to update the service registration atomically
	success, err := h.client.CompareAndSetVariable(ctx, h.serviceName, oldJSON, jsonStr)
	if err != nil {
		return ErrEndpointRegistrationFailed
	}
	if !success {
		return ErrServiceRegistrationChanged
	}

	h.mu.Lock()
	h.currentRegistration = &updated
	h.mu.Unlock()

	return nil
}

// AddListener adds a listener for service registration updates
func (h *ServiceRegistrationHandler) AddListener(callback func(ServiceRegistration)) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.listeners = append(h.listeners, callback)
	return nil
}

// ResolveService resolves the current service registration
func (h *ServiceRegistrationHandler) ResolveService(ctx context.Context) (*ServiceRegistration, error) {
	jsonStr, err := h.client.GetVariable(ctx, h.serviceName)
	if err != nil {
		return nil, ErrServiceNotFound
	}

	var registration ServiceRegistration
	if err := json.Unmarshal([]byte(jsonStr), &registration); err != nil {
		return nil, ErrServiceNotFound
	}

	return &registration, nil
}

// AddEndpointListener adds a listener for endpoint updates
func (h *ServiceRegistrationHandler) AddEndpointListener(callback func(ServiceEndpointsUpdate)) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.endpointListeners = append(h.endpointListeners, callback)
	return nil
}

// Close stops the watch goroutine and cleans up resources
func (h *ServiceRegistrationHandler) Close() {
	if h.cancelFunc != nil {
		h.cancelFunc()
	}
}

// Cleanupt old enpoints that are no longer valid.
func (h *ServiceRegistrationHandler) TryCleanupOldEndpoints(ctx context.Context, timeout time.Duration) error {
	reg, err := h.ResolveService(ctx)

	if err != nil {
		return err
	}

	old_reg := reg.Clone()

	for i := 0; i < len(reg.Endpoints); i++ {
		ep := reg.Endpoints[i]

		// Check if the registration is timed out, if so remove it to avoid clutter.
		if !ep.Timestamp.Add(timeout).After(time.Now()) {
			// Remove endpoint from the list
			reg.Endpoints = append(reg.Endpoints[:i], reg.Endpoints[i+1:]...)
		}
	}

	// Update the service
	return h.RegisterOrUpdateService(ctx, reg, &old_reg)
}
