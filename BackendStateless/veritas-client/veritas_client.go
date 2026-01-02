package veritasclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Number of retries before failing a request
const RETRY_BEFORE_FAIL = 3

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
	GetAndAddVariable(ctx context.Context, name string, delta string) (int64, error)

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

type VeritasClient struct {
	endpoints []string
	client    *http.Client
	lock      sync.Mutex
	current   string
	timeout   time.Duration
}

// NewVeritasClient creates a new VeritasClient with the given endpoints and timeout.
func NewVeritasClient(endpoints []string, timeout time.Duration) (*VeritasClient, error) {
	endpoint, err := pick_endpoint(&endpoints)
	if err != nil {
		return nil, err
	}

	client := VeritasClient{
		endpoints: endpoints,
		client:    &http.Client{},
		timeout:   timeout,
		current:   endpoint,
		lock:      sync.Mutex{},
	}

	return &client, nil
}

func pick_endpoint(endpoints *[]string) (string, error) {
	if len(*endpoints) > 0 {
		// Choose a random endpoint to distribute load
		return (*endpoints)[rand.Uint32()%uint32(len(*endpoints))], nil
	}
	return "", nil
}

type MaxRetriesReached struct{}

func (e *MaxRetriesReached) Error() string {
	return "Maximum retries reached"
}

// executeRequest executes the given HTTP request with retry logic.
// This blocks until the request is complete or the maximum number of retries is reached.
// It should only be used by a go routine so that concurrent requests can be handled correctly.
//
// NOTE: Make sure to close the response body after using the response.
func (vc *VeritasClient) executeRequest(req *http.Request, retries int) (*string, error) {
	if retries >= RETRY_BEFORE_FAIL {
		return nil, &MaxRetriesReached{}
	}

	vc.lock.Lock()
	current_endpoint := vc.current
	vc.lock.Unlock()

	req.URL.Host = current_endpoint
	req.URL.Scheme = "http"

	// Set timeout
	client := vc.client
	client.Timeout = vc.timeout

	rsp, err := client.Do(req)

	if err != nil || rsp.StatusCode >= 500 {
		vc.lock.Lock()
		// If the endpoint that failed is still the current one, switch to a new one
		// In concurrent scenarios, another goroutine might have already switched the endpoint, then we avoid switching again.
		if current_endpoint == vc.current {
			// try another endpoint
			new_endpoint, pick_err := pick_endpoint(&vc.endpoints)
			if pick_err != nil {
				vc.current = new_endpoint
			}
		}

		vc.lock.Unlock()

		// Retry the request up to RETRY_BEFORE_FAIL times
		return vc.executeRequest(req, retries+1)
	}

	// Ensure response body is closed after reading
	defer rsp.Body.Close()

	// Read response body
	bytes, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	response_str := string(bytes)

	return &response_str, err
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

	if body != nil {
		req.Body = http.NoBody
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return req, nil
}

// GetVariable gets the value of a variable by name lineralizably.
func (vc *VeritasClient) GetVariable(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the GetVariable method
	req, err := buildRequest(ctx, "GET", "/get/"+name, nil)
	if err != nil {
		return "", err
	}

	rsp, err := vc.executeRequest(req, 0)
	if rsp == nil || err != nil {
		return "", err
	}

	return *rsp, err
}

func (vc *VeritasClient) GetVariableEventual(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the GetVariableEventual method
	req, err := buildRequest(ctx, "GET", "/get_eventual/"+name, nil)
	if err != nil {
		return "", err
	}

	rsp, err := vc.executeRequest(req, 0)
	if rsp == nil || err != nil {
		return "", err
	}

	return *rsp, err
}

func (vc *VeritasClient) PeekVariable(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the PeekVariable method
	req, err := buildRequest(ctx, "GET", "/peek/"+name, nil)
	if err != nil {
		return "", err
	}

	rsp, err := vc.executeRequest(req, 0)
	if rsp == nil || err != nil {
		return "", err
	}

	return *rsp, err
}

func (vc *VeritasClient) SetVariable(ctx context.Context, name string, value string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the SetVariable method
	req, err := buildRequest(ctx, "POST", "/set/"+name, []byte(value))
	if err != nil {
		return false, err
	}

	set, err := vc.executeRequest(req, 0)
	if set == nil || err != nil {
		return false, err
	}

	return strings.ToLower(*set) == "true", err
}

func (vc *VeritasClient) AppendVariable(ctx context.Context, name string, value string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the AppendVariable method
	req, err := buildRequest(ctx, "POST", "/append/"+name, []byte(value))
	if err != nil {
		return false, err
	}

	append, err := vc.executeRequest(req, 0)
	if append == nil || err != nil {
		return false, err
	}
	return strings.ToLower(*append) == "true", err
}

func (vc *VeritasClient) ReplaceVariable(ctx context.Context, name string, oldValue string, newValue string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the ReplaceVariable method
	req, err := buildRequest(ctx, "POST", "/replace/"+name+"/"+oldValue, []byte(newValue))

	if err != nil {
		return false, err
	}
	replace, err := vc.executeRequest(req, 0)
	if replace == nil || err != nil {
		return false, err
	}
	return strings.ToLower(*replace) == "true", err
}

func (vc *VeritasClient) CompareAndSetVariable(ctx context.Context, name string, expectedValue string, newValue string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Construct the body as "13;expectedValuenewValue"
	body := []byte(fmt.Sprintf("%d;%s%s", len(expectedValue), expectedValue, newValue))

	// Implement the CompareAndSetVariable method
	req, err := buildRequest(ctx, "POST", "/compare_set/"+name, body)
	if err != nil {
		return false, err
	}
	cas, err := vc.executeRequest(req, 0)
	if cas == nil || err != nil {
		return false, err
	}
	return strings.ToLower(*cas) == "true", err
}

func (vc *VeritasClient) GetAndAddVariable(ctx context.Context, name string, delta string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, vc.timeout)
	defer cancel() // Clean up the context when done

	// Implement the GetAndAddVariable method
	req, err := buildRequest(ctx, "GET", "/get_and_add/"+name, []byte(delta))
	if err != nil {
		return 0, err
	}

	rsp, err := vc.executeRequest(req, 0)
	if rsp == nil || err != nil {
		return 0, err
	}

	var value int64
	_, scan_err := fmt.Sscanf(*rsp, "%d", &value)
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

func (vc *VeritasClient) WatchVariables(ctx context.Context, names []string) (<-chan UpdateNotification, <-chan error) {
	valueChan := make(chan UpdateNotification)
	errorChan := make(chan error)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		// make sure to close channels when the goroutine exits
		defer close(valueChan)
		defer close(errorChan)
		defer cancel()

		ws, err := openWebSocketConnection(ctx, vc.current, "/ws")

		if err != nil {
			errorChan <- err
			cancel() // Cancel the context to stop the goroutine
			return
		}

		recChan := make(chan string)
		errChan := make(chan error)

		// Send watch commands for each variable name
		for _, name := range names {
			watchCmd := WatchCommand{
				Key: name,
			}

			cmd := FromWatchCommand(&watchCmd)
			json_bytes, err := json.Marshal(cmd)
			if err != nil {
				errorChan <- err
				cancel() // Cancel the context to stop the goroutine
				return
			}

			// Send the variable name to watch
			err = ws.WriteMessage(websocket.TextMessage, json_bytes)
			if err != nil {
				errorChan <- err // Don't cancel, yet other watches might still work
			}
		}

		// Start a go routine to read messages from the websocket
		go func() {
			for {
				// Read message
				messageType, message, err := ws.ReadMessage()
				if err != nil {
					errChan <- err
					continue // Continue reading messages
				}
				if messageType == websocket.TextMessage {
					recChan <- string(message)
				} else {
					errChan <- &MessageTypeNotSupported{messageType: messageType, message: message}
				}
			}
		}()

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
					continue
				}

				notification, parse_err := rsp.ToUpdateNotification()
				if parse_err != nil {
					errorChan <- parse_err
					continue
				}

				valueChan <- *notification
			// If we receive an error, send it to the error channel
			case err := <-errChan:
				if err == context.Canceled {
					return
				}

				errorChan <- err

				// If the error is a deadline exceeded, we should reconnect
				if err == context.DeadlineExceeded {
					cancel() // Cancel the context to stop the goroutine
					return
				}
			}
		}
	}()

	return valueChan, errorChan
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

		for {
			// innerValueChan, innerErrorChan := vc.WatchVariables(ctx, names)

			// for {
			// 	switch {
			// 		case <-ctx.Done():
			// 			return
			// 		case val := <-innerValueChan:
			// 			valueChan <- val
			// 		case err := <-innerErrorChan:
			// 			errorChan <- err

			// 	}
			// }
		}
	}()

	return valueChan, errorChan
}
