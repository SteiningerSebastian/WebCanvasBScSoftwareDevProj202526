package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
	veritasclient "veritas-client"
)

const VERITAS_PORT = 80
const VERITAS_TIMEOUT = 5 * time.Second
const VERITAS_FAILURE_THRESH int32 = 3
const VERITAS_SUCCESS_THRESH int32 = 2
const VERITAS_OPENTIME time.Duration = 10 * time.Second
const SERVICE_REGISTRATION_KEY = "service-partitioning-controller"
const SERVICE_REGISTRATION_INTERVAL = 10 * time.Second
const SERVICE_REGISTRATION_TIMEOUT = 30 * time.Second

// handleServiceRegistration manages the registration and periodic endpoint updates for the service.
func handleServiceRegistration(ctx context.Context, handler *veritasclient.ServiceRegistrationHandler) (chan<- error, error) {
	errorChan := make(chan error)

	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		slog.Error(fmt.Sprintf("Unable to register service, error getting hostname: %v\n", err))
		return nil, err
	}

	// Create service description
	serviceDescription := veritasclient.ServiceRegistration{
		ID:        SERVICE_REGISTRATION_KEY,
		Endpoints: make([]veritasclient.ServiceEndpoint, 0),
		Meta:      map[string]string{"version": "1.0.0"},
	}

	// Register the service if not already registered
	handler.RegisterOrUpdateService(ctx, &serviceDescription, nil)

	// Start updating service endpoints
	go func() {
		for {
			select {
			case <-ctx.Done():
				slog.Info("Service registration handler context done, exiting endpoint update loop.")
				return
			default:
				time.Sleep(SERVICE_REGISTRATION_INTERVAL)
				// Update service endpoints
				endpoint := veritasclient.ServiceEndpoint{
					ID:        hostname,
					Address:   hostname,
					Port:      50051,
					Timestamp: time.Now(),
				}
				err := handler.RegisterOrUpdateEndpoint(ctx, &endpoint)

				if err != nil {
					slog.Error(fmt.Sprintf("Error updating service endpoint: %v\n", err))
					errorChan <- err
				}

				slog.Debug(fmt.Sprintf("Service endpoint updated successfully: %s\n", endpoint.String()))
			}
		}
	}()

	return errorChan, nil
}

func main() {
	// From the environment load the log level and the veritas nodes
	logLevelstr := os.Getenv("LOG_LEVEL")
	if logLevelstr == "" {
		logLevelstr = "INFO" // Default log level
	}

	// Set up logging
	var logLevel slog.Level
	switch strings.ToUpper(logLevelstr) {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "INFO":
		logLevel = slog.LevelInfo
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	// Initialize the logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	slog.Info(fmt.Sprintf("Log level set to: %s\n", logLevel))

	// Load Veritas nodes from environment variable
	veritasNodesEnv := os.Getenv("VERITAS_NODES")
	if veritasNodesEnv == "" {
		slog.Error("No Veritas nodes specified.")
		return
	} else {
		slog.Info(fmt.Sprintf("Veritas nodes: %s\n", veritasNodesEnv))
	}

	// Initialize Veritas client
	veritasNodes := strings.Split(veritasNodesEnv, ",")
	endpoints := make([]string, len(veritasNodes))
	for _, node := range veritasNodes {
		strings.TrimSpace(node)
		if node != "" {
			slog.Info(fmt.Sprintf("Adding Veritas node endpoint: %s\n", node))
		}

		endpoints = append(endpoints, fmt.Sprintf("%s:%d", node, VERITAS_PORT))
	}

	slog.Info(fmt.Sprintf("Final Veritas endpoints: %v\n", endpoints))
	veritasClient, err := veritasclient.NewVeritasClient(endpoints, VERITAS_TIMEOUT, VERITAS_FAILURE_THRESH, VERITAS_SUCCESS_THRESH, VERITAS_OPENTIME)

	if err != nil {
		slog.Error(fmt.Sprintf("Error initializing Veritas client: %v\n", err))
		return
	}

	// Initialize the service registration handler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serviceHandler, err := veritasclient.NewServiceRegistrationHandler(ctx, veritasClient, SERVICE_REGISTRATION_KEY)
	if err != nil {
		slog.Error(fmt.Sprintf("Error initializing service registration handler: %v\n", err))
		return
	}

	// Start handling service registration and endpoints
	go handleServiceRegistration(ctx, serviceHandler)

	// TODO: Handle the partitioning logic, service discovery of noredb nodes, etc.

	// Block forever
	select {}
}
