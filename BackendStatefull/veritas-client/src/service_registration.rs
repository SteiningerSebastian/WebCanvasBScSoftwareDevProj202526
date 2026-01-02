use crate::veritas_client::{self, VeritasClient, VeritasClientTrait};
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex};
use std::{hash::Hash, pin::Pin, sync::Arc};

#[derive(Debug)]
pub enum Error {
    ServiceNotFound,
    ServiceRegistrationFailed,
    ServiceRegistrationChanged,
    EndpointRegistrationFailed,
    VeritasClientError(veritas_client::VeritasError),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ServiceEndpoint {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub timestamp: DateTime<Utc>,
}

impl PartialEq for ServiceEndpoint {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.port == other.port
    }
}

impl Eq for ServiceEndpoint {}

impl Hash for ServiceEndpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.port.hash(state);
        self.id.hash(state);
        self.timestamp.hash(state);
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ServiceRegistration {
    pub id: String,
    pub endpoints: Vec<ServiceEndpoint>,
    pub meta: std::collections::HashMap<String, String>,
}

#[derive(Clone)]
pub struct ServiceEndpointsUpdate {
    pub added_endpoints: Vec<ServiceEndpoint>,
    pub removed_endpoints: Vec<ServiceEndpoint>,
}

pub trait ServiceRegistrationTrait {
    /// Registers a new service or updates an existing one with the service registry.
    /// 
    /// Parameters:
    /// - service: The ServiceRegistration object containing service details.
    /// - old_service: The previous ServiceRegistration object for updates.
    /// 
    /// Returns:
    /// - Result<(), Error>: Ok(()) if the registration was successful, Err(Error) otherwise.
    fn register_or_update_service<'a>(&'a mut self, service: &'a ServiceRegistration, old_service: Option<&'a ServiceRegistration>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Registers a new service endpoint.
    /// 
    /// Parameters:
    /// - endpoint: A reference to the ServiceEndpoint to be registered.
    /// 
    /// Returns:
    /// - Result<(), Error>: Ok(()) if the registration was successful, Err(Error) otherwise.
    fn register_or_update_endpoint<'a>(&'a mut self, endpoint: &'a ServiceEndpoint) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

    /// Adds a listener for service registration updates.
    /// The callback will be called whenever there is a change in service registrations.
    /// The callback receives the updated ServiceRegistration as a parameter.
    /// 
    /// Parameters:
    /// - callback: A boxed function or closure that takes a ServiceRegistration and returns nothing.
    /// 
    /// Returns:
    /// - Result<(), Error>: Ok(()) if the listener was added successfully, Err(Error) otherwise.
    fn add_listener(&mut self, callback: Box<dyn Fn(ServiceRegistration) + Send + Sync>) -> Result<(), Error>;

    /// Resolves the current service registration information.
    ///
    /// Returns:
    /// - Result<ServiceRegistration, Error>: Ok(ServiceRegistration) if the resolution was successful, Err(Error) otherwise.
    fn resolve_service<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<ServiceRegistration, Error>> + Send + 'a>>;

    /// Adds a listener for endpoint updates.
    /// The callback will be called whenever there is a change in service endpoints.
    /// 
    /// Parameters:
    /// - callback: A boxed function or closure that takes a ServiceEndpointsUpdate and returns nothing.
    /// 
    /// Returns:
    /// - Result<(), Error>: Ok(()) if the listener was added successfully, Err(Error) otherwise.
    fn add_endpoint_listener(&mut self, callback: Box<dyn Fn(ServiceEndpointsUpdate) + Send + Sync>) -> Result<(), Error>;
}

pub struct ServiceRegistrationHandler {
    client: Box<dyn VeritasClientTrait>,
    service_name: String,
    listeners: Arc<Mutex<Vec<Box<dyn Fn(ServiceRegistration) + Send + Sync>>>>,
    endpoint_listeners: Arc<Mutex<Vec<Box<dyn Fn(ServiceEndpointsUpdate) + Send + Sync>>>>,
    current_registration: Arc<Mutex<Option<ServiceRegistration>>>,
    watch_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ServiceRegistrationHandler {
    pub async fn new(client: VeritasClient, service_name: &str) -> Result<Self, Error> {
        let watcher = client.watch_variable(service_name.to_string()).await
        .map_err(Error::VeritasClientError)?;

        let mut me = ServiceRegistrationHandler {
            client: Box::new(client), 
            service_name: service_name.to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        let listener_clone = me.listeners.clone();
        let endpoint_listener_clone = me.endpoint_listeners.clone();
        let current_registration_clone = me.current_registration.clone();

        let handle = tokio::spawn(async move {
            let mut watcher = watcher;
            while let Some(update) = watcher.recv().await {
                let json = update.new_value;
                let service_registration: ServiceRegistration = match serde_json::from_str(&json) {
                    Ok(reg) => reg,
                    Err(e) => {
                        eprintln!("Failed to parse service registration JSON: {:?}", e);
                        continue;
                    }
                };

                let mut current_reg_lock = current_registration_clone.lock().await;
                let listeners_lock = listener_clone.lock().await;
                let endpoint_listeners_lock = endpoint_listener_clone.lock().await;
                
                // Notify all listeners about the update
                for listener in &*listeners_lock {
                    listener(service_registration.clone());
                }

                // Determine added and removed endpoints
                if let Some(old_registration) = &*current_reg_lock {
                    let old_endpoints: std::collections::HashSet<_> = old_registration.endpoints.iter().cloned().collect();
                    let new_endpoints: std::collections::HashSet<_> = service_registration.endpoints.iter().cloned().collect();

                    let added_endpoints: Vec<_> = new_endpoints.difference(&old_endpoints).cloned().collect();
                    let removed_endpoints: Vec<_> = old_endpoints.difference(&new_endpoints).cloned().collect();

                    if !added_endpoints.is_empty() || !removed_endpoints.is_empty() {
                        let update = ServiceEndpointsUpdate {
                            added_endpoints,
                            removed_endpoints,
                        };

                        for endpoint_listener in &*endpoint_listeners_lock {
                            endpoint_listener(update.clone());
                        }
                    }
                }

                *current_reg_lock = Some(service_registration);
            }
        });

        me.watch_handle = Some(handle);

        Ok(me)
    }
}

impl ServiceRegistrationTrait for ServiceRegistrationHandler {
   //                                                                                                                     Pin<Box<dyn Future<Output = Result<String, VeritasError>> + Send + 'a>>
    fn register_or_update_service<'a>(&'a mut self, service: &'a ServiceRegistration, old_service: Option<&'a ServiceRegistration>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>{
        let future = async move {
            // Conver ServiceRegistration to json
            let json = serde_json::to_string(&service).map_err(|_| Error::ServiceRegistrationFailed)?;
            
            let old_json = if let Some(old) = old_service {
                serde_json::to_string(&old).map_err(|_| Error::ServiceRegistrationFailed)?
            }else { "".to_string() };

            // Use compare_and_set_variable to update the service registration atomically
            self.client.compare_and_set_variable(&self.service_name, &old_json, &json).await
                .map_err(|_| Error::ServiceRegistrationFailed)?;

            let mut reg_lock = self.current_registration.lock().await;
            *reg_lock = Some(service.clone());

            // Implementation to register service using VeritasClient
            Ok(())
        };
        Box::pin(future)
    }

    fn register_or_update_endpoint<'a>(&'a mut self, endpoint: &'a ServiceEndpoint) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        let future = async move {
            // Get current service registration
            let current: ServiceRegistration = self.resolve_service().await?;

            // Updated service registration
            let mut updated = current.clone();

            // Check if endpoint already exists
            if current.endpoints.iter().any(|e| e.id == endpoint.id) {
                let pos =current.endpoints.iter().position(|e| e.id == endpoint.id).ok_or(Error::EndpointRegistrationFailed)?;
                updated.endpoints[pos] = endpoint.clone();
            } else {
                updated.endpoints.push(endpoint.clone());
            }

            // Convert to json
            let json = serde_json::to_string(&updated).map_err(|_| Error::EndpointRegistrationFailed)?;
            let old_json = serde_json::to_string(&current).map_err(|_| Error::EndpointRegistrationFailed)?;

            // Use compare_and_set_variable to update the service registration atomically
            self.client.compare_and_set_variable(&self.service_name, &old_json, &json).await
                .map_err(|_| Error::EndpointRegistrationFailed)?;

            let mut reg_lock = self.current_registration.lock().await;
            *reg_lock = Some(updated);

            Ok(())
        };
        Box::pin(future)
    }

    fn add_listener(&mut self, callback: Box<dyn Fn(ServiceRegistration) + Send + Sync>) -> Result<(), Error> {
        let listeners_clone = self.listeners.clone();
        tokio::spawn(async move {
            let mut listeners_lock = listeners_clone.lock().await;
            listeners_lock.push(callback);
        });
        
        // Implementation to add listener using VeritasClient
        Ok(())
    }

    fn resolve_service<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<ServiceRegistration, Error>> + Send + 'a>> {
        let future = async move {
            let reg: ServiceRegistration = self.client.get_variable(&self.service_name).await
                .map_err(Error::VeritasClientError)
                .and_then(|json| {
                    serde_json::from_str(&json).map_err(|_| Error::ServiceNotFound)
                })?;
        
            Ok(reg)
        };
        Box::pin(future)
    }

    fn add_endpoint_listener(&mut self, callback: Box<dyn Fn(ServiceEndpointsUpdate) + Send + Sync>) -> Result<(), Error> {
        let endpoint_listeners_clone = self.endpoint_listeners.clone();
        tokio::spawn(async move {
            let mut endpoint_listeners_lock = endpoint_listeners_clone.lock().await;
            endpoint_listeners_lock.push(callback);
        });
        
        // Implementation to add endpoint listener using VeritasClient
        Ok(())
    }
}

impl Drop for ServiceRegistrationHandler {
    fn drop(&mut self) {
        if let Some(handle) = self.watch_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::future::Future;
    use tokio::sync::mpsc;
    use veritas_messages::messages::UpdateNotification;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    // Mock VeritasClient for testing
    struct MockVeritasClient {
        storage: Arc<TokioMutex<HashMap<String, String>>>,
        watch_channels: Arc<TokioMutex<Vec<mpsc::UnboundedSender<UpdateNotification>>>>,
    }

    impl MockVeritasClient {
        fn new() -> Self {
            Self {
                storage: Arc::new(TokioMutex::new(HashMap::new())),
                watch_channels: Arc::new(TokioMutex::new(Vec::new())),
            }
        }

        async fn _trigger_watch_update(&self, key: String, new_value: String) {
            let channels = self.watch_channels.lock().await;
            let notification = UpdateNotification {
                key,
                new_value,
                old_value: "".to_string(),
            };
            for sender in channels.iter() {
                let _ = sender.send(notification.clone());
            }
        }
    }

    impl VeritasClientTrait for MockVeritasClient {
        fn get_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let storage = self.storage.lock().await;
                storage.get(name)
                    .cloned()
                    .ok_or_else(|| veritas_client::VeritasError::ParseError("Variable not found".to_string()))
            })
        }

        fn get_variable_eventual<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, veritas_client::VeritasError>> + Send + 'a>> {
            self.get_variable(name)
        }

        fn peek_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<String, veritas_client::VeritasError>> + Send + 'a>> {
            self.get_variable(name)
        }

        fn set_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let mut storage = self.storage.lock().await;
                storage.insert(name.to_string(), value.to_string());
                Ok(true)
            })
        }

        fn append_variable<'a>(&'a self, name: &'a str, value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let mut storage = self.storage.lock().await;
                let current = storage.get(name).cloned().unwrap_or_default();
                storage.insert(name.to_string(), format!("{}{}", current, value));
                Ok(true)
            })
        }

        fn replace_variable<'a>(&'a self, name: &'a str, old_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let mut storage = self.storage.lock().await;
                if let Some(current) = storage.get(name) {
                    let updated = current.replace(old_value, new_value);
                    storage.insert(name.to_string(), updated);
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }

        fn compare_and_set_variable<'a>(&'a self, name: &'a str, expected_value: &'a str, new_value: &'a str) -> Pin<Box<dyn Future<Output = Result<bool, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let mut storage = self.storage.lock().await;
                let current = storage.get(name).map(|s| s.as_str()).unwrap_or("");
                if current == expected_value {
                    storage.insert(name.to_string(), new_value.to_string());
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }

        fn get_and_add_variable<'a>(&'a self, name: &'a str) -> Pin<Box<dyn Future<Output = Result<i64, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let mut storage = self.storage.lock().await;
                let current: i64 = storage.get(name)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                storage.insert(name.to_string(), (current + 1).to_string());
                Ok(current)
            })
        }

        fn watch_variables<'a>(&'a self, _variables: Vec<String>) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let (tx, rx) = mpsc::unbounded_channel();
                let mut channels = self.watch_channels.lock().await;
                channels.push(tx);
                Ok(rx)
            })
        }

        fn watch_variable<'a>(&'a self, _name: String) -> Pin<Box<dyn Future<Output = Result<mpsc::UnboundedReceiver<UpdateNotification>, veritas_client::VeritasError>> + Send + 'a>> {
            Box::pin(async move {
                let (tx, rx) = mpsc::unbounded_channel();
                let mut channels = self.watch_channels.lock().await;
                channels.push(tx);
                Ok(rx)
            })
        }
    }

    #[tokio::test]
    async fn test_resolve_service() {
        let mock_client = MockVeritasClient::new();
        
        // Setup initial service registration
        let service = ServiceRegistration {
            id: "test-service".to_string(),
            endpoints: vec![
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8080,
                    id: "endpoint-1".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            meta: HashMap::new(),
        };
        
        let json = serde_json::to_string(&service).unwrap();
        mock_client.set_variable("test-service", &json).await.unwrap();

        // Create handler with boxed mock client
        let handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Test resolve_service
        let resolved = handler.resolve_service().await.unwrap();
        assert_eq!(resolved.id, "test-service");
        assert_eq!(resolved.endpoints.len(), 1);
        assert_eq!(resolved.endpoints[0].address, "127.0.0.1");
        assert_eq!(resolved.endpoints[0].port, 8080);
    }

    #[tokio::test]
    async fn test_update_service() {
        let mock_client = MockVeritasClient::new();
        
        // Setup initial service registration
        let old_service = ServiceRegistration {
            id: "test-service".to_string(),
            endpoints: vec![
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8080,
                    id: "endpoint-1".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            meta: HashMap::new(),
        };
        
        let old_json = serde_json::to_string(&old_service).unwrap();
        mock_client.set_variable("test-service", &old_json).await.unwrap();

        // Create handler
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(Some(old_service.clone()))),
            watch_handle: None,
        };

        // Update service
        let new_service = ServiceRegistration {
            id: "test-service".to_string(),
            endpoints: vec![
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8080,
                    id: "endpoint-2".to_string(),
                    timestamp: Utc::now(),
                },
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8081,
                    id: "endpoint-3".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            meta: HashMap::new(),
        };

        handler.register_or_update_service(&new_service, Some(&old_service)).await.unwrap();

        // Verify update
        let resolved = handler.resolve_service().await.unwrap();
        assert_eq!(resolved.endpoints.len(), 2);
    }

    #[tokio::test]
    async fn test_register_endpoint() {
        let mock_client = MockVeritasClient::new();
        
        // Setup initial service registration
        let service = ServiceRegistration {
            id: "test-service".to_string(),
            endpoints: vec![
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8080,
                    id: "endpoint-1".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            meta: HashMap::new(),
        };
        
        let json = serde_json::to_string(&service).unwrap();
        mock_client.set_variable("test-service", &json).await.unwrap();

        // Create handler
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Register new endpoint
        let new_endpoint = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 9090,
            id: "endpoint-2".to_string(),
            timestamp: Utc::now(),
        };
        handler.register_or_update_endpoint(&new_endpoint).await.unwrap();

        // Verify endpoint was added
        let resolved = handler.resolve_service().await.unwrap();
        assert_eq!(resolved.endpoints.len(), 2);
        assert!(resolved.endpoints.iter().any(|e| e.port == 9090));
    }

    #[tokio::test]
    async fn test_add_listener() {
        let mock_client = MockVeritasClient::new();
        
        // Create handler
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Add listener
        handler.add_listener(Box::new(move |_service| {
            // Listener callback
        })).unwrap();

        // Wait a bit for the spawn to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify listener was added
        let listeners = handler.listeners.lock().await;
        assert_eq!(listeners.len(), 1);
    }

    #[tokio::test]
    async fn test_add_endpoint_listener() {
        let mock_client = MockVeritasClient::new();
        
        // Create handler
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Add endpoint listener
        handler.add_endpoint_listener(Box::new(move |_update| {
            // Listener callback
        })).unwrap();

        // Wait a bit for the spawn to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify listener was added
        let listeners = handler.endpoint_listeners.lock().await;
        assert_eq!(listeners.len(), 1);
    }

    #[tokio::test]
    async fn test_service_endpoint_equality() {
        let endpoint1 = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 8080,
            id: "endpoint-1".to_string(),
            timestamp: Utc::now(),
        };
        
        let endpoint2 = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 8080,
            id: "endpoint-2".to_string(),
            timestamp: Utc::now(),
        };
        
        let endpoint3 = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 9090,
            id: "endpoint-3".to_string(),
            timestamp: Utc::now(),
        };

        assert_eq!(endpoint1, endpoint2);
        assert_ne!(endpoint1, endpoint3);
    }

    #[tokio::test]
    async fn test_service_endpoint_hash() {
        use std::collections::HashSet;
        
        let now = Utc::now();
        let endpoint1 = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 8080,
            id: "endpoint-1".to_string(),
            timestamp: now,
        };
        
        let endpoint2 = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 8080,
            id: "endpoint-1".to_string(),
            timestamp: now,
        };
        
        let mut set = HashSet::new();
        set.insert(endpoint1);
        set.insert(endpoint2);
        
        // Should only have one element since they're equal
        assert_eq!(set.len(), 1);
    }

    #[tokio::test]
    async fn test_update_service_without_old_service() {
        let mock_client = MockVeritasClient::new();
        
        // Create handler without initial service
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "test-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Create new service
        let new_service = ServiceRegistration {
            id: "test-service".to_string(),
            endpoints: vec![
                ServiceEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 8080,
                    id: "endpoint-1".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            meta: HashMap::new(),
        };

        // Update service without old service
        handler.register_or_update_service(&new_service, None).await.unwrap();

        // Verify service was created
        let resolved = handler.resolve_service().await.unwrap();
        assert_eq!(resolved.id, "test-service");
        assert_eq!(resolved.endpoints.len(), 1);
    }

    #[tokio::test]
    async fn test_register_endpoint_error_when_service_not_found() {
        let mock_client = MockVeritasClient::new();
        
        // Create handler without setting up initial service
        let mut handler = ServiceRegistrationHandler {
            client: Box::new(mock_client),
            service_name: "nonexistent-service".to_string(),
            listeners: Arc::new(Mutex::new(Vec::new())),
            endpoint_listeners: Arc::new(Mutex::new(Vec::new())),
            current_registration: Arc::new(Mutex::new(None)),
            watch_handle: None,
        };

        // Try to register endpoint
        let new_endpoint = ServiceEndpoint {
            address: "127.0.0.1".to_string(),
            port: 9090,
            id: "endpoint-2".to_string(),
            timestamp: Utc::now(),
        };
        
        let result = handler.register_or_update_endpoint(&new_endpoint).await;
        assert!(result.is_err());
    }
}
