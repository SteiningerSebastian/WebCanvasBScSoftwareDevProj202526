use reqwest::{Client, Error as ReqwestError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchCommand {
    pub command: String,
    pub parameters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateNotification {
    pub command: String,
    pub key: String,
    pub new_value: String,
}

#[derive(Debug)]
pub enum VeritasError {
    RequestError(ReqwestError),
    WebSocketError(String),
    ParseError(String),
}

impl From<ReqwestError> for VeritasError {
    fn from(err: ReqwestError) -> Self {
        VeritasError::RequestError(err)
    }
}

pub struct VeritasClient {
    base_url: String,
    client: Client,
}

impl VeritasClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: Client::new(),
        }
    }

    /// Get a variable by name (linearizable read through leader)
    pub async fn get_variable(&self, name: &str) -> Result<String, VeritasError> {
        let url = format!("{}/get/{}", self.base_url, name);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        let value = response.text().await?;
        Ok(value)
    }

    /// Get a variable with eventual consistency (faster, reads from quorum)
    pub async fn get_variable_eventual(&self, name: &str) -> Result<String, VeritasError> {
        let url = format!("{}/get_eventual/{}", self.base_url, name);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        let value = response.text().await?;
        Ok(value)
    }

    /// Peek at a variable's raw value (local read, no consistency guarantee)
    pub async fn peek_variable(&self, name: &str) -> Result<String, VeritasError> {
        let url = format!("{}/peek/{}", self.base_url, name);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        let value = response.text().await?;
        Ok(value)
    }

    /// Set a variable (linearizable write through leader)
    pub async fn set_variable(&self, name: &str, value: &str) -> Result<bool, VeritasError> {
        let url = format!("{}/set/{}", self.base_url, name);
        let value_str = value.to_string();
        
        let response = self.client
            .put(&url)
            .body(value_str)
            .send()
            .await?;
        
        let result = response.text().await?;
        Ok(result.to_lowercase() == "true")
    }

    /// Append to a variable (linearizable write through leader)
    pub async fn append_variable(&self, name: &str, value: &str) -> Result<bool, VeritasError> {
        let url = format!("{}/append/{}", self.base_url, name);
        let value_str = value.to_string();
        
        let response = self.client
            .put(&url)
            .body(value_str)
            .send()
            .await?;
        
        let result = response.text().await?;
        Ok(result.to_lowercase() == "true")
    }

    /// Replace old value with new value in a variable (linearizable write through leader)
    pub async fn replace_variable(&self, name: &str, old_value: &str, new_value: &str) -> Result<bool, VeritasError> {
        let url = format!("{}/replace/{}", self.base_url, name);
        let body = format!("{};{}{}", old_value.len(), old_value, new_value);
        
        let response = self.client
            .put(&url)
            .body(body)
            .send()
            .await?;
        
        let result = response.text().await?;
        Ok(result.to_lowercase() == "true")
    }

    /// Compare and set a variable (linearizable write through leader)
    pub async fn compare_and_set_variable(&self, name: &str, expected_value: &str, new_value: &str) -> Result<bool, VeritasError> {
        let url = format!("{}/compare_set/{}", self.base_url, name);
        let body = format!("{};{}{}", expected_value.len(), expected_value, new_value);
        
        let response = self.client
            .put(&url)
            .body(body)
            .send()
            .await?;
        
        let result = response.text().await?;
        Ok(result.to_lowercase() == "true")
    }

    /// Get and increment a variable atomically (linearizable operation through leader)
    pub async fn get_and_add_variable(&self, name: &str) -> Result<i64, VeritasError> {
        let url = format!("{}/get_add/{}", self.base_url, name);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        let value = response.text().await?;
        let num = value.parse::<i64>()
            .map_err(|e| VeritasError::ParseError(e.to_string()))?;
        Ok(num)
    }

    /// Watch for variable changes via WebSocket
    /// Returns a receiver channel that will receive update notifications
    pub async fn watch_variables(&self, variables: Vec<String>) -> Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError> {
        let ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://");
        let url = Url::parse(&format!("{}/ws", ws_url))
            .map_err(|e| VeritasError::ParseError(e.to_string()))?;
        
        let (ws_stream, _) = connect_async(url.as_str())
            .await
            .map_err(|e| VeritasError::WebSocketError(e.to_string()))?;
        
        let (mut write, mut read) = ws_stream.split();
        let (tx, rx) = mpsc::unbounded_channel();

        // Send watch commands for each variable
        for var in variables {
            let watch_cmd = WatchCommand {
                command: "watch".to_string(),
                parameters: vec![var],
            };
            
            let msg = serde_json::to_string(&watch_cmd)
                .map_err(|e| VeritasError::ParseError(e.to_string()))?;
            
            write.send(Message::Text(msg.into()))
                .await
                .map_err(|e| VeritasError::WebSocketError(e.to_string()))?;
        }

        // Spawn task to handle incoming messages
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(notification) = serde_json::from_str::<UpdateNotification>(&text) {
                            if notification.command == "update" {
                                if tx.send(notification).is_err() {
                                    break; // Channel closed
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        Ok(rx)
    }

    /// Watch a single variable for changes
    pub async fn watch_variable(&self, name: String) -> Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError> {
        self.watch_variables(vec![name]).await
    }

    /// Watch all variables for changes
    pub async fn watch_all_variables(&self) -> Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError> {
        self.watch_variables(vec![]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use wiremock::{MockServer, Mock, ResponseTemplate};
    use wiremock::matchers::{method, path, body_string};

    #[tokio::test]
    async fn test_client_creation() {
        let client = VeritasClient::new("http://localhost:8080".to_string());
        assert_eq!(client.base_url, "http://localhost:8080");
    }

    #[tokio::test]
    async fn test_set_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("PUT"))
            .and(path("/set/test_key"))
            .and(body_string("test_value"))
            .respond_with(ResponseTemplate::new(200).set_body_string("true"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.set_variable("test_key", "test_value").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[tokio::test]
    async fn test_get_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("GET"))
            .and(path("/get/test_key"))
            .respond_with(ResponseTemplate::new(200).set_body_string("12345;test_value"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.get_variable("test_key").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "12345;test_value");
    }

    #[tokio::test]
    async fn test_get_variable_eventual() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("GET"))
            .and(path("/get_eventual/test_key"))
            .respond_with(ResponseTemplate::new(200).set_body_string("test_value"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.get_variable_eventual("test_key").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_value");
    }

    #[tokio::test]
    async fn test_peek_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("GET"))
            .and(path("/peek/test_key"))
            .respond_with(ResponseTemplate::new(200).set_body_string("123;peeked_value"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.peek_variable("test_key").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "123;peeked_value");
    }

    #[tokio::test]
    async fn test_append_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("PUT"))
            .and(path("/append/test_key"))
            .and(body_string("_appended"))
            .respond_with(ResponseTemplate::new(200).set_body_string("true"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.append_variable("test_key", "_appended").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[tokio::test]
    async fn test_replace_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("PUT"))
            .and(path("/replace/test_key"))
            .and(body_string("3;oldnew"))
            .respond_with(ResponseTemplate::new(200).set_body_string("true"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.replace_variable("test_key", "old", "new").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[tokio::test]
    async fn test_compare_and_set_variable_success() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("PUT"))
            .and(path("/compare_set/test_key"))
            .and(body_string("8;expectedupdated"))
            .respond_with(ResponseTemplate::new(200).set_body_string("true"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.compare_and_set_variable("test_key", "expected", "updated").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[tokio::test]
    async fn test_compare_and_set_variable_failure() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("PUT"))
            .and(path("/compare_set/test_key"))
            .respond_with(ResponseTemplate::new(200).set_body_string("false"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.compare_and_set_variable("test_key", "wrong", "updated").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[tokio::test]
    async fn test_get_and_add_variable() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("GET"))
            .and(path("/get_add/counter"))
            .respond_with(ResponseTemplate::new(200).set_body_string("42"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.get_and_add_variable("counter").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_error_handling_network_failure() {
        let client = VeritasClient::new("http://invalid-host-that-does-not-exist:9999".to_string());
        let result = client.get_variable("test_key").await;

        assert!(result.is_err());
        match result {
            Err(VeritasError::RequestError(_)) => {},
            _ => panic!("Expected RequestError"),
        }
    }

    #[tokio::test]
    async fn test_parse_error_on_get_and_add() {
        let mock_server = MockServer::start().await;
        
        Mock::given(method("GET"))
            .and(path("/get_add/counter"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not_a_number"))
            .mount(&mock_server)
            .await;

        let client = VeritasClient::new(mock_server.uri());
        let result = client.get_and_add_variable("counter").await;

        assert!(result.is_err());
        match result {
            Err(VeritasError::ParseError(_)) => {},
            _ => panic!("Expected ParseError"),
        }
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::env;

    /// Helper to check if integration tests should run
    /// Set VERITAS_INTEGRATION_TEST=1 to enable
    fn should_run_integration_tests() -> bool {
       true// env::var("VERITAS_INTEGRATION_TEST").unwrap_or_default() == "1"
    }

    /// Helper to get the integration test endpoint
    /// Set VERITAS_ENDPOINT or defaults to http://localhost:8080
    fn get_test_endpoint() -> String {
        env::var("VERITAS_ENDPOINT").unwrap_or_else(|_| "http://localhost:8080".to_string())
    }

    #[tokio::test]
    async fn integration_test_set_and_get() {
        if !should_run_integration_tests() {
            println!("Skipping integration test. Set VERITAS_INTEGRATION_TEST=1 to run.");
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("test_key_{}", chrono::Utc::now().timestamp_millis());
        
        // Set a value
        let set_result = client.set_variable(&test_key, "integration_test_value").await;
        assert!(set_result.is_ok(), "Failed to set variable: {:?}", set_result.err());
        assert_eq!(set_result.unwrap(), true);

        // Get the value back
        let get_result = client.get_variable(&test_key).await;
        assert!(get_result.is_ok(), "Failed to get variable: {:?}", get_result.err());
        
        let value = get_result.unwrap();
        assert!(value.contains("integration_test_value"), "Expected value to contain 'integration_test_value', got: {}", value);
    }

    #[tokio::test]
    async fn integration_test_append() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("append_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Set initial value
        client.set_variable(&test_key, "Hello").await.unwrap();
        
        // Append to it
        let append_result = client.append_variable(&test_key, " World").await;
        assert!(append_result.is_ok());
        assert_eq!(append_result.unwrap(), true);

        // Verify the appended value
        let get_result = client.get_variable(&test_key).await.unwrap();
        assert!(get_result.contains("Hello World"), "Expected 'Hello World', got: {}", get_result);
    }

    #[tokio::test]
    async fn integration_test_replace() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("replace_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Set initial value
        client.set_variable(&test_key, "old_value").await.unwrap();
        
        // Replace part of it
        let replace_result = client.replace_variable(&test_key, "old_value", "new_value").await;
        assert!(replace_result.is_ok());
        assert_eq!(replace_result.unwrap(), true);

        // Verify the replaced value
        let get_result = client.get_variable(&test_key).await.unwrap();
        assert!(get_result.contains("new_value"), "Expected 'new_value', got: {}", get_result);
    }

    #[tokio::test]
    async fn integration_test_compare_and_set_success() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("cas_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Set initial value
        client.set_variable(&test_key, "expected").await.unwrap();
        
        // Compare and set with correct expected value
        let cas_result = client.compare_and_set_variable(&test_key, "expected", "updated").await;
        assert!(cas_result.is_ok());
        assert_eq!(cas_result.unwrap(), true);

        // Verify the updated value
        let get_result = client.get_variable(&test_key).await.unwrap();
        assert!(get_result.contains("updated"), "Expected 'updated', got: {}", get_result);
    }

    #[tokio::test]
    async fn integration_test_compare_and_set_failure() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("cas_fail_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Set initial value
        client.set_variable(&test_key, "actual").await.unwrap();
        
        // Compare and set with wrong expected value
        let cas_result = client.compare_and_set_variable(&test_key, "wrong", "updated").await;
        assert!(cas_result.is_ok());
        assert_eq!(cas_result.unwrap(), false);

        // Verify the value didn't change
        let get_result = client.get_variable(&test_key).await.unwrap();
        assert!(get_result.contains("actual"), "Expected 'actual', got: {}", get_result);
    }

    #[tokio::test]
    async fn integration_test_get_and_add() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("counter_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Initialize counter
        client.set_variable(&test_key, "0").await.unwrap();
        
        // Get and add
        let result1 = client.get_and_add_variable(&test_key).await;
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), 0);

        // Get and add again
        let result2 = client.get_and_add_variable(&test_key).await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), 1);

        // Verify final value
        let final_value = client.get_variable(&test_key).await.unwrap();
        assert!(final_value.contains("2"), "Expected counter to be 2, got: {}", final_value);
    }

    #[tokio::test]
    async fn integration_test_eventual_consistency() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("eventual_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Set a value
        client.set_variable(&test_key, "eventual_value").await.unwrap();
        
        // Read with eventual consistency
        let eventual_result = client.get_variable_eventual(&test_key).await;
        assert!(eventual_result.is_ok());
        assert!(eventual_result.unwrap().contains("eventual_value"));

        // Peek should also work
        let peek_result = client.peek_variable(&test_key).await;
        assert!(peek_result.is_ok());
    }

    #[tokio::test]
    async fn integration_test_websocket_watch() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("watch_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Start watching
        let mut rx = client.watch_variable(test_key.clone()).await.unwrap();
        
        // Give websocket time to connect
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Set a value (should trigger notification)
        client.set_variable(&test_key, "watched_value").await.unwrap();
        
        // Wait for notification with timeout
        let notification = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            rx.recv()
        ).await;
        
        assert!(notification.is_ok(), "Timeout waiting for WebSocket notification");
        let notification = notification.unwrap();
        assert!(notification.is_some(), "Expected notification but channel closed");
        
        let update = notification.unwrap();
        assert_eq!(update.key, test_key);
        assert!(update.new_value.contains("watched_value"));
    }

    #[tokio::test]
    async fn integration_test_concurrent_operations() {
        if !should_run_integration_tests() {
            return;
        }

        let client = VeritasClient::new(get_test_endpoint());
        let test_key = format!("concurrent_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Initialize counter
        client.set_variable(&test_key, "0").await.unwrap();
        
        // Spawn multiple concurrent get_and_add operations
        let mut handles = vec![];
        for _ in 0..10 {
            let client = VeritasClient::new(get_test_endpoint());
            let key = test_key.clone();
            let handle = tokio::spawn(async move {
                client.get_and_add_variable(&key).await
            });
            handles.push(handle);
        }
        
        // Wait for all operations
        let results: Vec<_> = futures::future::join_all(handles).await;
        
        // All should succeed
        for result in results {
            assert!(result.is_ok());
        }
        
        // Final value should be 10
        let final_value = client.get_variable(&test_key).await.unwrap();
        assert!(final_value.contains("10"), "Expected counter to be 10, got: {}", final_value);
    }
}
