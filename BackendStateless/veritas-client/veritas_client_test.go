package veritasclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	circuitbreaker "general/circuit_breaker"
)

// Mock HTTP Client for unit tests
type MockHTTPClient struct {
	mu              sync.Mutex
	responses       map[string]*http.Response
	requestCount    int
	lastRequestURL  string
	lastRequestBody []byte
	shouldFail      bool
	failCount       int
	currentFailures int
}

func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		responses: make(map[string]*http.Response),
	}
}

func (m *MockHTTPClient) SetResponse(path string, statusCode int, body string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[path] = &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func (m *MockHTTPClient) SetFailureCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCount = count
	m.currentFailures = 0
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.requestCount++
	m.lastRequestURL = req.URL.Path

	// Read and store request body
	if req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		m.lastRequestBody = body
		// Reset the body for potential retries
		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	// Simulate failures for retry testing
	if m.currentFailures < m.failCount {
		m.currentFailures++
		return &http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader("Server Error")),
			Request:    req,
		}, nil
	}

	// Return the mock response
	if resp, ok := m.responses[req.URL.Path]; ok {
		// Clone the response to avoid issues with body being read multiple times
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		return &http.Response{
			StatusCode: resp.StatusCode,
			Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
			Request:    req,
		}, nil
	}

	return &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(strings.NewReader("Not Found")),
		Request:    req,
	}, nil
}

func (m *MockHTTPClient) GetRequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestCount
}

func (m *MockHTTPClient) GetLastRequestBody() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRequestBody
}

// Helper function to create a test client with mock HTTP client
func createTestClient(mockClient *MockHTTPClient) *VeritasClient {
	endpoints := []string{"localhost:8080", "localhost:8081"}
	client := &VeritasClient{
		endpoints:        endpoints,
		client:           mockClient,
		timeout:          5 * time.Second,
		endpointSelector: circuitbreaker.NewEndpointSelector(len(endpoints), 3, 2, 10*time.Second),
	}
	return client
}

// Unit Tests - GetVariable

func TestGetVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/get/test_key", 200, "test_value")

	client := createTestClient(mockClient)
	ctx := context.Background()

	value, err := client.GetVariable(ctx, "test_key")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if value != "test_value" {
		t.Errorf("Expected 'test_value', got: %s", value)
	}

	if mockClient.GetRequestCount() < 1 {
		t.Error("Expected at least one request")
	}
}

func TestGetVariable_InvalidName(t *testing.T) {
	mockClient := NewMockHTTPClient()
	client := createTestClient(mockClient)
	ctx := context.Background()

	testCases := []string{"", "invalid name", "name/with/slash"}

	for _, testName := range testCases {
		_, err := client.GetVariable(ctx, testName)
		if err == nil {
			t.Errorf("Expected error for invalid name '%s'", testName)
		}

		if _, ok := err.(*InvalidName); !ok {
			t.Errorf("Expected InvalidName error, got: %T", err)
		}
	}
}

func TestGetVariable_Retry(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/get/test_key", 200, "test_value")
	mockClient.SetFailureCount(2) // Fail twice, then succeed

	client := createTestClient(mockClient)
	ctx := context.Background()

	value, err := client.GetVariable(ctx, "test_key")
	if err != nil {
		t.Fatalf("Expected no error after retries, got: %v", err)
	}

	if value != "test_value" {
		t.Errorf("Expected 'test_value', got: %s", value)
	}

	if mockClient.GetRequestCount() < 3 {
		t.Errorf("Expected at least 3 requests (2 failures + 1 success), got: %d", mockClient.GetRequestCount())
	}
}

// Unit Tests - GetVariableEventual

func TestGetVariableEventual_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/get_eventual/test_key", 200, "eventual_value")

	client := createTestClient(mockClient)
	ctx := context.Background()

	value, err := client.GetVariableEventual(ctx, "test_key")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if value != "eventual_value" {
		t.Errorf("Expected 'eventual_value', got: %s", value)
	}
}

// Unit Tests - PeekVariable

func TestPeekVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/peek/test_key", 200, "peek_value")

	client := createTestClient(mockClient)
	ctx := context.Background()

	value, err := client.PeekVariable(ctx, "test_key")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if value != "peek_value" {
		t.Errorf("Expected 'peek_value', got: %s", value)
	}
}

// Unit Tests - SetVariable

func TestSetVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/set/test_key", 200, "true")

	client := createTestClient(mockClient)
	ctx := context.Background()

	success, err := client.SetVariable(ctx, "test_key", "new_value")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !success {
		t.Error("Expected success to be true")
	}

	// Verify the request body
	body := mockClient.GetLastRequestBody()
	if string(body) != "new_value" {
		t.Errorf("Expected body 'new_value', got: %s", string(body))
	}
}

func TestSetVariable_Failure(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/set/test_key", 200, "false")

	client := createTestClient(mockClient)
	ctx := context.Background()

	success, err := client.SetVariable(ctx, "test_key", "new_value")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if success {
		t.Error("Expected success to be false")
	}
}

// Unit Tests - AppendVariable

func TestAppendVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/append/test_key", 200, "true")

	client := createTestClient(mockClient)
	ctx := context.Background()

	success, err := client.AppendVariable(ctx, "test_key", "_appended")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !success {
		t.Error("Expected success to be true")
	}

	body := mockClient.GetLastRequestBody()
	if string(body) != "_appended" {
		t.Errorf("Expected body '_appended', got: %s", string(body))
	}
}

// Unit Tests - ReplaceVariable

func TestReplaceVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/replace/test_key", 200, "true")

	client := createTestClient(mockClient)
	ctx := context.Background()

	oldValue := "old"
	newValue := "new"
	success, err := client.ReplaceVariable(ctx, "test_key", oldValue, newValue)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !success {
		t.Error("Expected success to be true")
	}

	// Verify the request body format: "len;oldValuenewValue"
	expectedBody := fmt.Sprintf("%d;%s%s", len(oldValue), oldValue, newValue)
	body := mockClient.GetLastRequestBody()
	if string(body) != expectedBody {
		t.Errorf("Expected body '%s', got: %s", expectedBody, string(body))
	}
}

// Unit Tests - CompareAndSetVariable

func TestCompareAndSetVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/compare_set/test_key", 200, "true")

	client := createTestClient(mockClient)
	ctx := context.Background()

	expectedValue := "expected"
	newValue := "new"
	success, err := client.CompareAndSetVariable(ctx, "test_key", expectedValue, newValue)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !success {
		t.Error("Expected success to be true")
	}

	// Verify the request body format
	expectedBody := fmt.Sprintf("%d;%s%s", len(expectedValue), expectedValue, newValue)
	body := mockClient.GetLastRequestBody()
	if string(body) != expectedBody {
		t.Errorf("Expected body '%s', got: %s", expectedBody, string(body))
	}
}

func TestCompareAndSetVariable_Failure(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/compare_set/test_key", 200, "false")

	client := createTestClient(mockClient)
	ctx := context.Background()

	success, err := client.CompareAndSetVariable(ctx, "test_key", "wrong_expected", "new")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if success {
		t.Error("Expected success to be false")
	}
}

// Unit Tests - GetAndAddVariable

func TestGetAndAddVariable_Success(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/get_add/counter", 200, "42")

	client := createTestClient(mockClient)
	ctx := context.Background()

	oldValue, err := client.GetAndAddVariable(ctx, "counter")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if oldValue != 42 {
		t.Errorf("Expected old value 42, got: %d", oldValue)
	}
}

func TestGetAndAddVariable_ParseError(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetResponse("/get_add/counter", 200, "not_a_number")

	client := createTestClient(mockClient)
	ctx := context.Background()

	_, err := client.GetAndAddVariable(ctx, "counter")
	if err == nil {
		t.Error("Expected error for invalid number format")
	}
}

// Unit Tests - isValidName

func TestIsValidName(t *testing.T) {
	testCases := []struct {
		name     string
		expected bool
	}{
		{"valid_name", true},
		{"valid-name", true},
		{"valid123", true},
		{"", false},
		{"invalid name", false},
		{"invalid/name", false},
		{"invalid?name", false},
	}

	for _, tc := range testCases {
		result := isValidName(tc.name)
		if result != tc.expected {
			t.Errorf("isValidName(%q) = %v, expected %v", tc.name, result, tc.expected)
		}
	}
}

// Unit Tests - MaxRetriesReached

func TestMaxRetriesReached(t *testing.T) {
	mockClient := NewMockHTTPClient()
	mockClient.SetFailureCount(100) // Always fail

	client := createTestClient(mockClient)
	ctx := context.Background()

	_, err := client.GetVariable(ctx, "test_key")
	if err == nil {
		t.Fatal("Expected error after max retries")
	}

	if _, ok := err.(*MaxRetriesReached); !ok {
		t.Errorf("Expected MaxRetriesReached error, got: %T", err)
	}
}

// Unit Tests - Messages

func TestFromWatchCommand(t *testing.T) {
	watchCmd := &WatchCommand{Key: "test_key"}
	wsCmd := FromWatchCommand(watchCmd)

	if wsCmd.Command != "WatchCommand" {
		t.Errorf("Expected command 'WatchCommand', got: %s", wsCmd.Command)
	}

	if len(wsCmd.Parameters) != 1 || wsCmd.Parameters[0][0] != "key" || wsCmd.Parameters[0][1] != "test_key" {
		t.Errorf("Unexpected parameters: %v", wsCmd.Parameters)
	}

	// Test marshaling
	_, err := json.Marshal(wsCmd)
	if err != nil {
		t.Errorf("Failed to marshal watch command: %v", err)
	}
}

func TestFromUpdateNotification(t *testing.T) {
	update := &UpdateNotification{
		Key:      "test_key",
		NewValue: "new_val",
		OldValue: "old_val",
	}

	wsResp := FromUpdateNotification(update)

	if wsResp.Command != "UpdateNotification" {
		t.Errorf("Expected command 'UpdateNotification', got: %s", wsResp.Command)
	}

	// Check parameters
	params := make(map[string]string)
	for _, param := range wsResp.Parameters {
		if len(param) == 2 {
			params[param[0]] = param[1]
		}
	}

	if params["key"] != "test_key" || params["new_value"] != "new_val" || params["old_value"] != "old_val" {
		t.Errorf("Unexpected parameters: %v", params)
	}
}

func TestToUpdateNotification(t *testing.T) {
	wsResp := &WebsocketResponse{
		Command: "UpdateNotification",
		Parameters: [][]string{
			{"key", "test_key"},
			{"new_value", "new_val"},
			{"old_value", "old_val"},
		},
	}

	notification, err := wsResp.ToUpdateNotification()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if notification.Key != "test_key" {
		t.Errorf("Expected key 'test_key', got: %s", notification.Key)
	}
	if notification.NewValue != "new_val" {
		t.Errorf("Expected new value 'new_val', got: %s", notification.NewValue)
	}
	if notification.OldValue != "old_val" {
		t.Errorf("Expected old value 'old_val', got: %s", notification.OldValue)
	}
}

// =============================================================================
// Integration Tests (require server running on localhost:8080)
// =============================================================================

// Helper to check if integration tests should run
func shouldRunIntegrationTests(t *testing.T) {
	// Try to connect to localhost:8080 with a quick timeout
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get("http://localhost:8080/peek/test")
	if err != nil {
		t.Skip("Skipping integration tests: server not available on localhost:8080")
		return
	}
	defer resp.Body.Close()

	// Check if we got a valid response (not a 5xx error)
	if resp.StatusCode >= 500 {
		t.Skipf("Skipping integration tests: server returned error %d", resp.StatusCode)
		return
	}
}

func createIntegrationClient(t *testing.T) *VeritasClient {
	// Make sure to port forward to localhost:8080 and localhost:8081
	client, err := NewVeritasClient([]string{"localhost:8080", "localhost:8081"}, 5*time.Second, 3, 2, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return client
}

func TestIntegration_SetAndGet(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_key_%d", time.Now().UnixNano())
	testValue := "test_value_123"

	// Set the variable
	success, err := client.SetVariable(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}
	if !success {
		t.Error("SetVariable returned false")
	}

	// Get the variable
	value, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != testValue {
		t.Errorf("Expected '%s', got '%s'", testValue, value)
	}
}

func TestIntegration_GetVariableEventual(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_eventual_%d", time.Now().UnixNano())
	testValue := "eventual_value"

	// Set the variable first
	_, err := client.SetVariable(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Get eventual
	value, err := client.GetVariableEventual(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariableEventual failed: %v", err)
	}
	if value != testValue {
		t.Errorf("Expected '%s', got '%s'", testValue, value)
	}
}

func TestIntegration_PeekVariable(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_peek_%d", time.Now().UnixNano())
	testValue := "peek_value"

	// Set the variable first
	_, err := client.SetVariable(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Peek at the variable
	value, err := client.PeekVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("PeekVariable failed: %v", err)
	}
	// Peek actually returns the timestamp and the value separated by a semicolon
	parts := strings.SplitN(value, ";", 2)
	if len(parts) != 2 {
		t.Fatalf("Unexpected peek format: %s", value)
	}

	if parts[1] != testValue {
		t.Errorf("Expected '%s', got '%s'", testValue, parts[1])
	}
}

func TestIntegration_AppendVariable(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_append_%d", time.Now().UnixNano())

	// Set initial value
	_, err := client.SetVariable(ctx, testKey, "initial")
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Append to the variable
	success, err := client.AppendVariable(ctx, testKey, "_appended")
	if err != nil {
		t.Fatalf("AppendVariable failed: %v", err)
	}
	if !success {
		t.Error("AppendVariable returned false")
	}

	// Verify the result
	value, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != "initial_appended" {
		t.Errorf("Expected 'initial_appended', got '%s'", value)
	}
}

func TestIntegration_ReplaceVariable(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_replace_%d", time.Now().UnixNano())

	// Set initial value
	_, err := client.SetVariable(ctx, testKey, "hello world")
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Replace "world" with "universe"
	success, err := client.ReplaceVariable(ctx, testKey, "world", "universe")
	if err != nil {
		t.Fatalf("ReplaceVariable failed: %v", err)
	}
	if !success {
		t.Error("ReplaceVariable returned false")
	}

	// Verify the result
	value, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != "hello universe" {
		t.Errorf("Expected 'hello universe', got '%s'", value)
	}
}

func TestIntegration_CompareAndSetVariable(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_cas_%d", time.Now().UnixNano())

	// Set initial value
	_, err := client.SetVariable(ctx, testKey, "initial")
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// CAS with correct expected value
	success, err := client.CompareAndSetVariable(ctx, testKey, "initial", "updated")
	if err != nil {
		t.Fatalf("CompareAndSetVariable failed: %v", err)
	}
	if !success {
		t.Error("CompareAndSetVariable should succeed with correct expected value")
	}

	// Verify the result
	value, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != "updated" {
		t.Errorf("Expected 'updated', got '%s'", value)
	}

	// CAS with wrong expected value should fail
	success, err = client.CompareAndSetVariable(ctx, testKey, "wrong", "should_not_set")
	if err != nil {
		t.Fatalf("CompareAndSetVariable failed: %v", err)
	}
	if success {
		t.Error("CompareAndSetVariable should fail with wrong expected value")
	}

	// Verify value didn't change
	value, err = client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != "updated" {
		t.Errorf("Expected 'updated', got '%s'", value)
	}
}

func TestIntegration_GetAndAddVariable(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_counter_%d", time.Now().UnixNano())

	// Set initial value
	_, err := client.SetVariable(ctx, testKey, "100")
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Get and add
	oldValue, err := client.GetAndAddVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetAndAddVariable failed: %v", err)
	}
	if oldValue != 100 {
		t.Errorf("Expected old value 100, got %d", oldValue)
	}

	// Verify the new value
	value, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}
	if value != "101" {
		t.Errorf("Expected '101', got '%s'", value)
	}
}

func TestIntegration_ConcurrentOperations(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx := context.Background()

	testKey := fmt.Sprintf("test_counter_%d", time.Now().UnixNano())

	// Set initial value
	_, err := client.SetVariable(ctx, testKey, "0")
	if err != nil {
		t.Fatalf("SetVariable failed: %v", err)
	}

	// Run concurrent GetAndAdd operations
	numGoroutines := 10
	var wg sync.WaitGroup
	results := make([]int64, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			oldVal, err := client.GetAndAddVariable(ctx, testKey)
			if err != nil {
				t.Errorf("GetAndAddVariable failed: %v", err)
				return
			}
			results[idx] = oldVal
		}(i)
	}

	wg.Wait()

	// Verify all results are unique (linearizability)
	seen := make(map[int64]bool)
	for _, val := range results {
		if seen[val] {
			t.Errorf("Duplicate value found: %d (linearizability violated)", val)
		}
		seen[val] = true
	}

	// Verify final value
	finalValue, err := client.GetVariable(ctx, testKey)
	if err != nil {
		t.Fatalf("GetVariable failed: %v", err)
	}

	var finalValueInt int64
	fmt.Sscanf(finalValue, "%d", &finalValueInt)

	// If anything fails here, the final value is not 10. They are returned lineralizably but not necessarily not dropped. (gaps may exist)
	if finalValueInt < int64(numGoroutines) {
		t.Errorf("Expected final value '10', got '%s'", finalValue)
	}
}

func TestIntegration_ContextCancellation(t *testing.T) {
	shouldRunIntegrationTests(t)

	client := createIntegrationClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	testKey := fmt.Sprintf("test_cancel_%d", time.Now().UnixNano())

	// Cancel immediately
	cancel()

	// Try to set variable with cancelled context
	_, err := client.SetVariable(ctx, testKey, "value")
	if err == nil {
		t.Error("Expected error with cancelled context")
	}
}

func TestIntegration_Timeout(t *testing.T) {
	shouldRunIntegrationTests(t)

	// Create client with very short timeout
	client, err := NewVeritasClient([]string{"localhost:8080"}, 1*time.Nanosecond, 3, 2, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()
	testKey := fmt.Sprintf("test_timeout_%d", time.Now().UnixNano())

	// This should timeout
	_, err = client.SetVariable(ctx, testKey, "value")
	if err == nil {
		t.Error("Expected timeout error")
	}
}
