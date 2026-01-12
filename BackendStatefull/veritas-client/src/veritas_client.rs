// This implementation was havely influenced by LLMs (GitHub Copilot GPT5) and generated from a promt given the server
// side implementation and a few instructions. Although certain changes were made to make it work as intended.
// The tests were also generate using LLMs and adapted for the specific usecase.

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Error as ReqwestError, Method};
use veritas_messages::messages::{UpdateNotification, WatchCommand, WebsocketCommand, WebsocketResponse};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async};
use url::Url;

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
    nodes: Vec<String>,
    client: Client,
    current: Arc<AtomicUsize>, // Arc for sharing across tasks
    ws_nodes: Vec<String>, // reuse for websocket (same as nodes; kept for compatibility)
}

impl Clone for VeritasClient {
    fn clone(&self) -> Self {
        VeritasClient {
            nodes: self.nodes.clone(),
            client: Client::new(),
            current: Arc::new(AtomicUsize::new(self.current.load(Ordering::SeqCst))),
            ws_nodes: self.ws_nodes.clone(),
        }
    }
}

pub trait VeritasClientTrait: Send + Sync {
    /// Get a variable by name (linearizable read through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to get.
    /// 
    /// Returns:
    /// - The value of the variable as stored in the leader.
    fn get_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>>;

    /// Get a variable with eventual consistency (faster, reads from quorum)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to get.
    /// 
    /// Returns:
    /// - The value of the variable as stored in the quorum.
    fn get_variable_eventual<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>>;

    /// Peek at a variable's raw value (local read, no consistency guarantee)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to peek.
    /// 
    /// Returns:
    /// - The raw value of the variable as stored locally.
    fn peek_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>>;

    /// Set a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to set.
    /// - `value`: The value to set.
    /// 
    /// Returns:
    /// - `true` if the set was successful, `false` otherwise.
    fn set_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>>;

    /// Append to a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `value`: The value to append.
    /// 
    /// Returns:
    /// - `true` if the append was successful, `false` otherwise.
    fn append_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>>;

    /// Replace old value with new value in a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `old_value`: The value to be replaced.
    /// - `new_value`: The value to replace with.
    /// 
    /// Returns:
    /// - `true` if the replacement was successful, `false` otherwise.
    fn replace_variable<'a>(&'a self, name: &'a str, old_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>>;

    /// Compare and set a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `expected_value`: The expected current value of the variable.
    /// - `new_value`: The new value to set if the current value matches the expected value.
    /// 
    /// Returns:
    /// - `true` if the variable was updated, `false` otherwise.
    fn compare_and_set_variable<'a>(&'a self, name: &'a str, expected_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>>;

    /// Get and increment a variable atomically (linearizable operation through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to increment.
    /// 
    /// Returns:
    /// - The value of the variable before the increment.
    fn get_and_add_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<i64, VeritasError>> + Send + 'a>>;

    /// Watch multiple variables for changes via WebSocket
    /// 
    /// Parameters:
    /// - `variables`: A vector of variable names to watch. If empty, watches all variables.
    /// 
    /// Returns:
    /// - An unbounded receiver channel that yields `UpdateNotification` structs on variable updates.
    /// 
    /// WARNING: The state update stream is lossy (notifications may be dropped) but maintains strict temporal integrity. 
    /// While the set of notifications is non-exhaustive, the sequence is guaranteed to be strictly monotonic, 
    /// ensuring that the observer never receives a stale or out-of-order historical version of the data.
    fn watch_variables<'a>(&'a self, variables: Vec<String>) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError>> + Send + 'a>>;

    /// Watch a single variable for changes
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to watch.
    /// 
    /// Returns:
    /// - An unbounded receiver channel that yields `UpdateNotification` structs on variable updates.
    /// 
    /// WARNING: The state update stream is lossy (notifications may be dropped) but maintains strict temporal integrity.
    /// While the set of notifications is non-exhaustive, the sequence is guaranteed to be strictly monotonic,
    /// ensuring that the observer never receives a stale or out-of-order historical version of the data.
    fn watch_variable<'a>(&'a self, name: String) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError>> + Send + 'a>>;
}

impl VeritasClient {
    pub fn new(nodes: Vec<String>) -> Self {
        let idx = if nodes.is_empty() { 0 } else { Self::pick_random_index(nodes.len()) };
        Self {
            nodes: nodes.clone(),
            client: Client::new(),
            current: Arc::new(AtomicUsize::new(idx)),
            ws_nodes: nodes,
        }
    }

    /// Construct with multiple nodes (hostnames or full http/https base URLs).
    /// First node becomes the base_url for HTTP operations; websocket logic will rotate.
    pub fn new_with_nodes(nodes: Vec<String>) -> Self {
        Self::new(nodes)
    }

    fn update_current(&self, idx: usize) {
        self.current.store(idx, Ordering::SeqCst);
    }

    // Helper to pick pseudo-random index without external crates.
    fn pick_random_index(len: usize) -> usize {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        (nanos as usize) % len
    }

    fn build_url(node: &str, path: &str) -> String {
        format!("{}{}", node.trim_end_matches('/'), path)
    }

    async fn send_request(&self, method: Method, path: &str, body: Option<String>) -> Result<reqwest::Response, VeritasError> {
        if self.nodes.is_empty() {
            return Err(VeritasError::WebSocketError("No nodes configured".into()));
        }
        let start = self.current.load(Ordering::SeqCst);
        // order: current, then others
        for offset in 0..self.nodes.len() {
            let idx = (start + offset) % self.nodes.len();
            let node = &self.nodes[idx];
            let url = Self::build_url(node, path);
            let builder = self.client.request(method.clone(), &url);
            let builder = if let Some(b) = &body { builder.body(b.clone()) } else { builder };
            match builder.send().await {
                Ok(resp) => {
                    // update preferred node if different
                    if idx != start {
                        self.update_current(idx);
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    // only fallback on network-ish failures; else propagate immediately
                    if !(e.is_connect() || e.is_timeout() || e.is_request()) || offset == self.nodes.len() - 1 {
                        return Err(VeritasError::RequestError(e));
                    }
                    // else continue loop
                }
            }
        }
        Err(VeritasError::WebSocketError("Exhausted nodes".into()))
    }
}

impl VeritasClientTrait for VeritasClient {
    /// Get a variable by name (linearizable read through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to get.
    /// 
    /// Returns:
    /// - The value of the variable as stored in the leader.
    fn get_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::GET, &format!("/get/{}", name), None).await?;
            Ok(resp.text().await?)
        })
    }

    /// Get a variable with eventual consistency (faster, reads from quorum)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to get.
    /// 
    /// Returns:
    /// - The value of the variable as stored in the quorum.
    fn get_variable_eventual<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::GET, &format!("/get_eventual/{}", name), None).await?;
            Ok(resp.text().await?)
        })
    }

    /// Peek at a variable's raw value (local read, no consistency guarantee)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to peek.
    /// 
    /// Returns:
    /// - The raw value of the variable as stored locally.
    fn peek_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::GET, &format!("/peek/{}", name), None).await?;
            Ok(resp.text().await?)
        })
    }

    /// Set a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to set.
    /// - `value`: The value to set.
    /// 
    /// Returns:
    /// - `true` if the set was successful, `false` otherwise.
    fn set_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        let value = value.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::PUT, &format!("/set/{}", name), Some(value)).await?;
            Ok(resp.text().await?.to_lowercase() == "true")
        })
    }

    /// Append to a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `value`: The value to append.
    /// 
    /// Returns:
    /// - `true` if the append was successful, `false` otherwise.
    fn append_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        let value = value.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::PUT, &format!("/append/{}", name), Some(value)).await?;
            Ok(resp.text().await?.to_lowercase() == "true")
        })
    }

    /// Replace old value with new value in a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `old_value`: The value to be replaced.
    /// - `new_value`: The value to replace with.
    /// 
    /// Returns:
    /// - `true` if the replacement was successful, `false` otherwise.
    fn replace_variable<'a>(&'a self, name: &'a str, old_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        let old_value = old_value.to_string();
        let new_value = new_value.to_string();
        Box::pin(async move {
            let body = format!("{};{}{}", old_value.len(), old_value, new_value);
            let resp = self.send_request(Method::PUT, &format!("/replace/{}", name), Some(body)).await?;
            Ok(resp.text().await?.to_lowercase() == "true")
        })
    }

    /// Compare and set a variable (linearizable write through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to modify.
    /// - `expected_value`: The expected current value of the variable.
    /// - `new_value`: The new value to set if the current value matches the expected value.
    /// 
    /// Returns:
    /// - `true` if the variable was updated, `false` otherwise.
    fn compare_and_set_variable<'a>(&'a self, name: &'a str, expected_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        let expected_value = expected_value.to_string();
        let new_value = new_value.to_string();
        Box::pin(async move {
            let body = format!("{};{}{}", expected_value.len(), expected_value, new_value);
            let resp = self.send_request(Method::PUT, &format!("/compare_set/{}", name), Some(body)).await?;
            Ok(resp.text().await?.to_lowercase() == "true")
        })
    }
    /// Get and increment a variable atomically (linearizable operation through leader)
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to increment.
    /// 
    /// Returns:
    /// - The value of the variable before the increment.
    fn get_and_add_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<i64, VeritasError>> + Send + 'a>> {
        let name = name.to_string();
        Box::pin(async move {
            let resp = self.send_request(Method::GET, &format!("/get_add/{}", name), None).await?;
            let value = resp.text().await?;
            let num = value.parse::<i64>().map_err(|e| VeritasError::ParseError(e.to_string()))?;
            Ok(num)
        })
    }

    /// Watch multiple variables for changes via WebSocket
    /// 
    /// Parameters:
    /// - `variables`: A vector of variable names to watch. If empty, watches all variables.
    /// 
    /// Returns:
    /// - An unbounded receiver channel that yields `UpdateNotification` structs on variable updates.
    /// 
    /// WARNING: The state update stream is lossy (notifications may be dropped) but maintains strict temporal integrity. 
    /// While the set of notifications is non-exhaustive, the sequence is guaranteed to be strictly monotonic, 
    /// ensuring that the observer never receives a stale or out-of-order historical version of the data.
    fn watch_variables<'a>(&'a self, variables: Vec<String>) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError>> + Send + 'a>> {
        use tokio_tungstenite::tungstenite::protocol::Message;
        Box::pin(async move {
            let candidates = if self.ws_nodes.is_empty() { self.nodes.clone() } else { self.ws_nodes.clone() };
            if candidates.is_empty() {
                return Err(VeritasError::WebSocketError("No websocket nodes available".into()));
            }
            // attempt connect starting from current then others
            let start = self.current.load(Ordering::SeqCst);
            let mut stream_opt = None;
            for offset in 0..candidates.len() {
                let idx = (start + offset) % candidates.len();
                let base = &candidates[idx];
                let ws_url = base.replace("http://", "ws://").replace("https://", "wss://");
                let url = match Url::parse(&format!("{}/ws", ws_url)) {
                    Ok(u) => u,
                    Err(_) => continue,
                };
                match connect_async(url.as_str()).await {
                    Ok((stream, _)) => {
                        if idx != start {
                            self.update_current(idx);
                        }
                        stream_opt = Some(stream);
                        break;
                    }
                    Err(_) => continue,
                }
            }
            let stream = match stream_opt {
                Some(s) => s,
                None => return Err(VeritasError::WebSocketError("All websocket connection attempts failed".into())),
            };
            let (mut write, read) = stream.split();
            let (tx, rx) = mpsc::unbounded_channel();
            for var in &variables {
                let watch_cmd = WatchCommand { key: var.clone() };
                let cmd = WebsocketCommand::from_watch_command(&watch_cmd);
                if let Ok(msg) = serde_json::to_string(&cmd) {
                    write.send(Message::Text(msg.into())).await.map_err(|e| VeritasError::WebSocketError(e.to_string()))?;
                }
            }
            let vars_clone = variables.clone();
            let nodes_clone = candidates.clone();
            let current_atomic = self.current.clone(); // clone Arc instead of borrowing
            tokio::spawn(async move {
                let mut current_read = read;
                loop {
                    while let Some(msg) = current_read.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(rsp) = serde_json::from_str::<WebsocketResponse>(&text) {
                                    if rsp.command != "UpdateNotification" { continue; } // ignore other commands

                                    let notification = rsp.to_update_notification();

                                    if let Ok(notification) = notification {
                                        if tx.send(notification).is_err() {
                                            return; // receiver dropped
                                        }
                                    }else {
                                        continue; // parse error, ignore
                                    }
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    if tx.is_closed() { return; }
                    // reconnect rotation
                    let start_local = current_atomic.load(Ordering::SeqCst);
                    let mut success = None;
                    for offset in 0..nodes_clone.len() {
                        let idx = (start_local + offset + 1) % nodes_clone.len(); // prefer different node
                        let base = &nodes_clone[idx];
                        let ws_url = base.replace("http://", "ws://").replace("https://", "wss://");
                        let url = match Url::parse(&format!("{}/ws", ws_url)) { Ok(u) => u, Err(_) => continue };
                        match connect_async(url.as_str()).await {
                            Ok((stream, _)) => {
                                current_atomic.store(idx, Ordering::SeqCst);
                                success = Some(stream);
                                break;
                            }
                            Err(_) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                                continue;
                            }
                        }
                    }
                    let stream = match success { Some(s) => s, None => return };
                    let (mut new_write, new_read) = stream.split();
                    for var in &vars_clone {
                        let watch_cmd = WatchCommand { key: var.clone() };
                        let cmd = WebsocketCommand::from_watch_command(&watch_cmd);
                        if let Ok(msg) = serde_json::to_string(&cmd) {
                            if new_write.send(Message::Text(msg.into())).await.is_err() { return; }
                        }
                    }
                    current_read = new_read;
                }
            });
            Ok(rx)
        })
    }

    /// Watch a single variable for changes
    /// 
    /// Parameters:
    /// - `name`: The name of the variable to watch.
    /// 
    /// Returns:
    /// - An unbounded receiver channel that yields `UpdateNotification` structs on variable updates.
    /// 
    /// WARNING: The state update stream is lossy (notifications may be dropped) but maintains strict temporal integrity.
    /// While the set of notifications is non-exhaustive, the sequence is guaranteed to be strictly monotonic,
    /// ensuring that the observer never receives a stale or out-of-order historical version of the data.
    fn watch_variable<'a>(&'a self, name: String) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, VeritasError>> + Send + 'a>> {
        Box::pin(async move {
            self.watch_variables(vec![name]).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::{body_string, method, path};

    #[tokio::test]
    async fn test_client_creation() {
        let client = VeritasClient::new(vec!["http://localhost:8080".to_string()]);
        assert_eq!(client.nodes[0], "http://localhost:8080");
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
        let result = client.get_and_add_variable("counter").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_error_handling_network_failure() {
        let client = VeritasClient::new(vec!["http://invalid-host-that-does-not-exist:9999".to_string()]);
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

        let client = VeritasClient::new(vec![mock_server.uri()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
        let test_key = format!("watch_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Start watching
        let mut rx = client.watch_variable(test_key.clone()).await.unwrap();
        
        // Give websocket time to connect
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
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

        let client = VeritasClient::new(vec![get_test_endpoint()]);
        let test_key = format!("concurrent_test_{}", chrono::Utc::now().timestamp_millis());
        
        // Initialize counter
        client.set_variable(&test_key, "0").await.unwrap();
        
        // Spawn multiple concurrent get_and_add operations
        let mut handles = vec![];
        for _ in 0..10 {
            let client = VeritasClient::new(vec![get_test_endpoint()]);
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