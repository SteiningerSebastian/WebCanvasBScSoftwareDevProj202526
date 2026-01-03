package veritasclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	circuitbreaker "general/circuit_breaker"

	"github.com/gorilla/websocket"
)

// Number of retries before failing a request
const RETRY_BEFORE_FAIL = 8
const RETRY_DELAY_MS = 32
const EXPONENTIAL_BACKOFF_BASE = 1.5

// VeritasClientInterface defines the interface for interacting with the Veritas distributed key-value store.
type VeritasClientInterface interface {
	// Get the value of a variable by name lineralizably.
	GetVariable(ctx context.Context, name string) (string, error)

	// Get the value of a variable by name eventualy. (eventual consistency)
	GetVariableEventual(ctx context.Context, name string) (string, error)

	// Peek at the value storred on the current node only. (no consistency guarantees but way faster)
	PeekVariable(ctx context.Context, name string) (string, error)

	// Set the value of a variable by name lineralizably.
	SetVariable(ctx context.Context, name string, value string) (bool, error)

	// Append a value to a variable by name lineralizably.
	AppendVariable(ctx context.Context, name string, value string) (bool, error)

	// Replace oldValue with newValue for a variable by name lineralizably.
	ReplaceVariable(ctx context.Context, name string, oldValue string, newValue string) (bool, error)

	// Compare and set a variable by name lineralizably.
	CompareAndSetVariable(ctx context.Context, name string, expectedValue string, newValue string) (bool, error)

	// Atomically add delta to the integer variable by name lineralizably and return the old value. Each call will get a unique value.
	GetAndAddVariable(ctx context.Context, name string) (int64, error)

	// Watch for changes to a variable by name. The callback is called lineralizably whenever the variable changes.
	// If a disconnection occurse, it can't automatically reconnect as updates between disconnection and reconnection are lost and
	// the linearizability guarantee would be violated.
	// The returned channels are closed when the context is cancelled.
	WatchVariables(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error)

	// Watch for changes to a variable by name with automatic reconnection. The callback is called lineralizably whenever the variable changes.
	// If a disconnection occurse, it will automatically try to reconnect.
	// The returned channels are closed when the context is cancelled.
	//
	// NOTE: Updates that occur between disconnection and reconnection are lost. Therefore, the linearizability guarantee is only preserved
	//       for periods when the connection is active. (Use WatchVariable for strict linearizability and handle reconnections manually.)
	WatchVariablesAutoReconnect(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error)
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type VeritasClient struct {
	endpoints        []string
	endpointSelector *circuitbreaker.EndpointSelector
	client           HTTPClient
	timeout          time.Duration
}

// NewVeritasClient creates a new VeritasClient with the given endpoints and timeout.
func NewVeritasClient(endpoints []string, timeout time.Duration, failureThresh, successThresh int32, openTimeout time.Duration) (*VeritasClient, error) {
	// Shuffle endpoints to distribute load (endpoint selection is deterministic after this point - first healthy endpoint found is always selected)
	rand.Shuffle(len(endpoints), func(i, j int) {
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	})

	// Create the client
	client := VeritasClient{
		endpoints:        endpoints,
		client:           &http.Client{},
		timeout:          timeout,
		endpointSelector: circuitbreaker.NewEndpointSelector(len(endpoints), failureThresh, successThresh, openTimeout),
	}

	return &client, nil
}

// getEndpoint picks an endpoint from the list of endpoints using the endpoint selector.
func (vc *VeritasClient) getEndpoint() (int, error) {
	return vc.endpointSelector.Get()
}

type MaxRetriesReached struct{}

func (e *MaxRetriesReached) Error() string {
	return "Maximum retries reached"
}

type InvalidName struct {
	name string
}

func (e *InvalidName) Error() string {
	return fmt.Sprintf("Variable name '%s' is not allowed.", e.name)
}

// isValidName checks if the given variable name is valid (non-empty and url-safe).
func isValidName(name string) bool {
	if name == "" {
		return false
	}

	// Make sure the name is url-safe
	escaped_name := url.PathEscape(name)

	slog.Debug(fmt.Sprintf("Validating name: original='%s', escaped='%s'", name, escaped_name))

	return escaped_name == name
}

// executeRequest executes the given HTTP request with retry logic.
// This blocks until the request is complete or the maximum number of retries is reached.
// It should only be used by a go routine so that concurrent requests can be handled correctly.
//
// NOTE: Make sure to close the response body after using the response.
func (vc *VeritasClient) executeRequest(req *http.Request) (*string, error) {
	// Try the request with retries and different endpoints
	for retries := 0; retries < RETRY_BEFORE_FAIL; retries++ {
		// Get the current endpoint
		current_endpoint, err := vc.getEndpoint()
		if err != nil {
			if _, ok := err.(*circuitbreaker.NoHealthyEndpointError); ok {
				slog.Warn("No healthy endpoint available, retrying ...")
				// Wait a bit before retrying
				backoff_duration := time.Duration(float64(RETRY_DELAY_MS)*math.Pow(EXPONENTIAL_BACKOFF_BASE, float64(retries))) * time.Millisecond
				time.Sleep(backoff_duration)
				continue
			}
			return nil, err
		}
		slog.Debug(fmt.Sprintf("Executing request to %s (attempt %d)", vc.endpoints[current_endpoint], retries+1))

		req.URL.Host = vc.endpoints[current_endpoint]
		req.URL.Scheme = "http"

		// Execute the request
		rsp, err := vc.client.Do(req)

		// If there was an error or a server error, try again with a different endpoint
		if err != nil || (rsp != nil && rsp.StatusCode >= 500) {
			var statusCode int
			if rsp != nil {
				statusCode = rsp.StatusCode
			}
			slog.Debug(fmt.Sprintf("Request failed (%s returned %d), retrying ...", vc.endpoints[current_endpoint], statusCode))

			// Notify the endpoint selector of the failure
			vc.endpointSelector.OnFailure(current_endpoint)

			// Wait a bit before retrying
			backoff_duration := time.Duration(float64(RETRY_DELAY_MS)*math.Pow(EXPONENTIAL_BACKOFF_BASE, float64(retries))) * time.Millisecond
			time.Sleep(backoff_duration)
			continue
		}

		// Ensure response body is closed after reading
		defer rsp.Body.Close()

		// Read response body
		bytes, err := io.ReadAll(rsp.Body)
		if err != nil {
			return nil, err
		}

		response_str := string(bytes)

		slog.Debug(fmt.Sprintf("Request succeeded :%s", vc.endpoints[current_endpoint]))
		// Notify the endpoint selector of the success
		vc.endpointSelector.OnSuccess(current_endpoint)

		return &response_str, err
	}

	// If we have reached the maximum number of retries, return an error
	return nil, &MaxRetriesReached{}
}

// buildRequest builds an HTTP request for the given method, path, and body.
func buildRequest(ctx context.Context, method string, path string, body []byte) (*http.Request, error) {
	url := &url.URL{
		Scheme: "http", // Scheme will be overwritten in executeRequest
		Host:   "",     // Host will be set in executeRequest
		Path:   path,
	}
	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)

	if err != nil {
		return nil, err
	}

	if body == nil {
		req.Body = http.NoBody
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return req, nil
}

// buildRequestWithTimeout builds an HTTP request with a timeout context.
func (vc *VeritasClient) buildRequestWithTimeout(ctx context.Context, method string, path string, body []byte) (*http.Request, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	req, err := buildRequest(ctx, method, path, body)
	return req, cancel, err
}

// executeRequestFromPath builds and executes an HTTP request for the given method, path, and body.
func (vc *VeritasClient) executeRequestFromPath(ctx context.Context, method string, path string, body []byte) (string, error) {
	req, cancel, err := vc.buildRequestWithTimeout(ctx, method, path, body)
	defer cancel()

	if err != nil {
		return "", err
	}
	rsp, err := vc.executeRequest(req)
	if rsp == nil || err != nil {
		return "", err
	}
	return *rsp, err
}

// GetVariable gets the value of a variable by name lineralizably.
func (vc *VeritasClient) GetVariable(ctx context.Context, name string) (string, error) {
	if !isValidName(name) {
		return "", &InvalidName{name: name}
	}
	return vc.executeRequestFromPath(ctx, "GET", "/get/"+name, nil)
}

func (vc *VeritasClient) GetVariableEventual(ctx context.Context, name string) (string, error) {
	if !isValidName(name) {
		return "", &InvalidName{name: name}
	}
	return vc.executeRequestFromPath(ctx, "GET", "/get_eventual/"+name, nil)
}

func (vc *VeritasClient) PeekVariable(ctx context.Context, name string) (string, error) {
	if !isValidName(name) {
		return "", &InvalidName{name: name}
	}
	return vc.executeRequestFromPath(ctx, "GET", "/peek/"+name, nil)
}

func (vc *VeritasClient) SetVariable(ctx context.Context, name string, value string) (bool, error) {
	if !isValidName(name) {
		return false, &InvalidName{name: name}
	}

	// Implement the SetVariable method
	rsp, err := vc.executeRequestFromPath(ctx, "PUT", "/set/"+name, []byte(value))

	if rsp == "" || err != nil {
		return false, err
	}
	return strings.ToLower(rsp) == "true", err
}

func (vc *VeritasClient) AppendVariable(ctx context.Context, name string, value string) (bool, error) {
	if !isValidName(name) {
		return false, &InvalidName{name: name}
	}

	// Implement the SetVariable method
	rsp, err := vc.executeRequestFromPath(ctx, "PUT", "/append/"+name, []byte(value))

	if rsp == "" || err != nil {
		return false, err
	}
	return strings.ToLower(rsp) == "true", err
}

func (vc *VeritasClient) ReplaceVariable(ctx context.Context, name string, oldValue string, newValue string) (bool, error) {
	if !isValidName(name) {
		return false, &InvalidName{name: name}
	}

	// Implement the SetVariable method
	rsp, err := vc.executeRequestFromPath(ctx, "PUT", "/replace/"+name, []byte(fmt.Sprintf("%d;%s%s", len(oldValue), oldValue, newValue)))

	if rsp == "" || err != nil {
		return false, err
	}
	return strings.ToLower(rsp) == "true", err
}

func (vc *VeritasClient) CompareAndSetVariable(ctx context.Context, name string, expectedValue string, newValue string) (bool, error) {
	if !isValidName(name) {
		return false, &InvalidName{name: name}
	}

	// Implement the SetVariable method
	rsp, err := vc.executeRequestFromPath(ctx, "PUT", "/compare_set/"+name, []byte(fmt.Sprintf("%d;%s%s", len(expectedValue), expectedValue, newValue)))

	if rsp == "" || err != nil {
		return false, err
	}
	return strings.ToLower(rsp) == "true", err
}

func (vc *VeritasClient) GetAndAddVariable(ctx context.Context, name string) (int64, error) {
	if !isValidName(name) {
		return 0, &InvalidName{name: name}
	}

	rsp, err := vc.executeRequestFromPath(ctx, "GET", "/get_add/"+name, nil)

	if rsp == "" || err != nil {
		return 0, err
	}

	var value int64
	_, scan_err := fmt.Sscanf(rsp, "%d", &value)
	if scan_err != nil {
		return 0, scan_err
	}

	return value, err
}

// openWebSocketConnection opens a websocket connection to the given endpoint and path.
func openWebSocketConnection(ctx context.Context, endpoint string, path string) (*websocket.Conn, error) {
	ws_url := url.URL{
		Scheme: "ws",
		Host:   endpoint,
		Path:   path,
	}

	dialer := websocket.DefaultDialer
	con, _, err := dialer.DialContext(ctx, ws_url.String(), nil)

	if err != nil {
		return nil, err
	}

	return con, nil
}

type MessageTypeNotSupported struct {
	messageType int
	message     []byte
}

func (e *MessageTypeNotSupported) Error() string {
	return fmt.Sprintf("Message-Type %d is not supported. Message: %s", e.messageType, e.message)
}

type CommandNotSupported struct {
	command string
}

func (e *CommandNotSupported) Error() string {
	return fmt.Sprintf("Command %s is not supported.", e.command)
}

func validateNames(names []string) error {
	for _, name := range names {
		if !isValidName(name) {
			return &InvalidName{name: name}
		}
	}
	return nil
}

// startWebSocketReader starts a goroutine that reads messages from the websocket and sends them to the given channels.
func startWebSocketReader(ctx context.Context, ws *websocket.Conn) (<-chan string, <-chan error) {
	recChan := make(chan string)
	errChan := make(chan error)

	// Making sure that the websocket is closed when the context is done so that the goroutine can exit. (ws.ReadMessage will return an error then.)
	go func() {
		<-ctx.Done()
		ws.Close()
	}()

	go func() {
		// make sure to close channels when the goroutine exits
		defer close(recChan)
		defer close(errChan)

		for {
			// Read message
			messageType, message, err := ws.ReadMessage()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					errChan <- err
					return
				}
			}

			if messageType == websocket.TextMessage {
				select {
				case recChan <- string(message):
				case <-ctx.Done():
					return
				}
			} else {
				errChan <- &MessageTypeNotSupported{messageType: messageType, message: message}
			}
		}
	}()

	return recChan, errChan
}

// sendWatchCommands sends watch commands for the given variable names to the websocket.
func (vc *VeritasClient) sendWatchCommands(ws *websocket.Conn, names []string, errorChan chan<- error, ctx context.Context) {
	// Send watch commands for each variable name
	for _, name := range names {
		watchCmd := WatchCommand{
			Key: name,
		}
		// Convert to WebsocketCommand
		cmd := FromWatchCommand(&watchCmd)
		json_bytes, err := json.Marshal(cmd)
		if err != nil {
			errorChan <- err
			return
		}
		// Send the variable name to watch
		err = ws.WriteMessage(websocket.TextMessage, json_bytes)
		if err != nil {
			errorChan <- err // Don't cancel, yet other watches might still work
		}
	}
}

func (vc *VeritasClient) watchVariables(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error) {
	ctx, cancel := context.WithCancel(ctx)

	valueChan := make(chan UpdateNotification)
	errorChan := make(chan error)

	go func() {
		// make sure to close channels when the goroutine exits
		defer close(valueChan)
		defer close(errorChan)
		defer cancel()

		// Get endpoint
		current_endpoint, err := vc.getEndpoint()

		if err != nil {
			if _, ok := err.(*circuitbreaker.NoHealthyEndpointError); ok {
				slog.Warn("No healthy endpoint available.")
			}
			errorChan <- err
			return
		}

		ws, err := openWebSocketConnection(ctx, vc.endpoints[current_endpoint], "/ws")

		if err != nil {
			errorChan <- err
			// Notify the endpoint selector of the failure
			vc.endpointSelector.OnFailure(current_endpoint)
			return
		}

		// Send watch commands for each variable name
		vc.sendWatchCommands(ws, names, errorChan, ctx)

		// Start a go routine to read messages from the websocket
		recChan, errChan := startWebSocketReader(ctx, ws)

		for {
			select {
			// If we are cancelled, exit
			case <-ctx.Done():
				return
			// If we receive a message, send it to the value channel
			case msg := <-recChan:
				var rsp WebsocketResponse
				err := json.Unmarshal([]byte(msg), &rsp)
				if err != nil {
					errorChan <- err
					return
				}

				notification, parse_err := rsp.ToUpdateNotification()
				if parse_err != nil {
					errorChan <- parse_err
					return
				}

				// Notify the endpoint selector of the success
				vc.endpointSelector.OnSuccess(current_endpoint)

				valueChan <- *notification
			// If we receive an error, send it to the error channel
			case err := <-errChan:
				if err == context.Canceled {
					return
				}

				errorChan <- err

				// If the error is a deadline exceeded, we should try to reconnect / connect to a different endpoint
				if err == context.DeadlineExceeded {
					// Notify the endpoint selector of the failure
					vc.endpointSelector.OnFailure(current_endpoint)

					cancel() // Cancel the context to stop the goroutine
					return
				}
			}
		}
	}()

	return valueChan, errorChan
}

func (vc *VeritasClient) WatchVariables(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error, error) {
	// Validate names
	if err := validateNames(names); err != nil {
		return nil, nil, err
	}

	valueChan, errorChan := vc.watchVariables(ctx, names)

	return valueChan, errorChan, nil
}

func (vc *VeritasClient) WatchVariableAutoReconnect(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error) {
	valueChan := make(chan UpdateNotification)
	errorChan := make(chan error)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		// make sure to close channels when the goroutine exits
		defer close(valueChan)
		defer close(errorChan)
		defer cancel()

		for retries := 0; ; retries++ {
			innerctx, innercancel := context.WithCancel(ctx)
			defer innercancel() // Clean up the inner context when done
			innerValueChan, innerErrorChan, err := vc.WatchVariables(innerctx, names)

			// If there was an error during WatchVariables, send it to the error channel and exit
			if err != nil {
				errorChan <- err
				return
			}

			is_connected := true
			for is_connected {
				select {
				case <-ctx.Done():
					return
				case <-innerctx.Done():
					is_connected = false // Inner context cancelled, break to reconnect
				case val, ok := <-innerValueChan:
					// If the inner channel is closed, break to reconnect
					if !ok {
						is_connected = false
					} else {
						valueChan <- val
					}
				case err, ok := <-innerErrorChan:
					// If the inner channel is closed, break to reconnect
					if !ok {
						is_connected = false
					} else {
						if _, ok := err.(*circuitbreaker.NoHealthyEndpointError); ok {
							slog.Warn("No healthy endpoint available, retrying ...")

							// Wait a bit before retrying
							time.Sleep(RETRY_DELAY_MS)
							is_connected = false
						}
						errorChan <- err
					}
				}
			}
			innercancel() // Cancel the inner context to stop the inner goroutine
			// Wait a bit before reconnecting to avoid busy looping
			time.Sleep(1 * time.Second)
		}
	}()

	return valueChan, errorChan
}
