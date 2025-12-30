use std::{collections::HashMap, env, path::Path, process, time::Duration};

use chrono::Utc;
use tonic::{transport::Server};
use tonic_health::server::health_reporter;
use tracing::{Level, debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;

use general::{concurrent_file_key_value_store::{ConcurrentFileKeyValueStore, ConcurrentKeyValueStore}};
use veritas_client::{service_registration::{ServiceEndpoint, ServiceRegistration, ServiceRegistrationHandler, ServiceRegistrationTrait}, veritas_client::{VeritasClient, VeritasClientTrait}};

use crate::{canvasdb::CanvasDB, server::{MyDatabaseServer, noredb}};

mod server;
mod canvasdb;

const CONFIG_PATH: &str = "/data/noredb.config";
const UNIQUE_ID_KEY: &str = "noredb.unique_id";
const SERVICE_REGISTRATION_KEY: &str = "service-noredb";
const PORT : u16 = 50051;
const REGISTRATION_ATTEMPTS: usize = 4; // number of attempts to register service
const REGISTRATION_BACKOFF_MS: u64 = 200; // milliseconds between attempts
const REGISTRATION_BACKOFF_MULTIPLIER: u64 = 5; // exponential backoff multiplier
const REGISTRATION_INTERVAL_MS: u64 = 10_000; // interval between registration refreshes

const CANVAS_WIDTH: usize = 3840;
const CANVAS_HEIGHT: usize = 2160;
const CANVAS_DB_PATH: &str = "/data/canvasdb";
const WRITE_AHEAD_LOG_SIZE: usize = 1024;
const NUM_WORKER_THREADS: usize = 4;


/// Parse a comma-separated list of hostnames/IPs (VERITAS_NODES) into a Vec<String>
/// (preserve the original token so we can resolve DNS at connect time).
pub fn parse_node_names(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

// Loads the unique id from ./data/noredb.config. If missing, requests a new
// unique id from Veritas via get_and_add on the shared counter key, then persists it.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_level = match env::var("LOG_LEVEL") {
        Ok(v) => {
            match v.to_uppercase().as_str() {
                "ERROR" => Level::ERROR,
                "WARN" => Level::WARN,
                "INFO" => Level::INFO,
                "DEBUG" => Level::DEBUG,
                "TRACE" => Level::TRACE,
                _ => {
                    eprintln!("Invalid LOG_LEVEL '{}', defaulting to TRACE", v);
                    Level::TRACE
                }
            }
        },
        Err(_) => {
            eprintln!("Environment variable LOG_LEVEL is not set");
            if cfg!(debug_assertions) {
                eprintln!("Defaulting LOG_LEVEL to DEBUG for debug build");
                Level::TRACE
            } else {
                eprintln!("Defaulting LOG_LEVEL to WARN for release build");
                Level::WARN
            }
        }
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
    info!("Log level set to {:?}", log_level);

    // For debug builds, set VERITAS_NODES to a default value if not set
    if cfg!(debug_assertions) {
        let debug_nodes = "localhost,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1";
        unsafe {
            env::set_var("VERITAS_NODES", debug_nodes);
        }
        debug!("VERITAS_NODES set to {}", debug_nodes);
    }

    // Get the VERITAS_NODES environment variable
    let raw = match env::var("VERITAS_NODES") {
        Ok(v) => v,
        Err(_) => {
            error!("Environment variable VERITAS_NODES is not set");
            process::exit(1);
        }
    };

    // Keep tokens, may be hostnames or literal IPs
    let nodes_tokens = parse_node_names(&raw);

    if nodes_tokens.len() < 3 || nodes_tokens.len() > 15 {
        error!(
            "VERITAS_NODES must contain between 3 and 15 node tokens (hostnames or IPs), found {}",
            nodes_tokens.len()
        );
        process::exit(1);
    }

    debug!("VERITAS_NODES parsed ({}):", nodes_tokens.len());
    for t in &nodes_tokens {
        trace!("{}", t);
    }

    let nodes_tokens = nodes_tokens.into_iter().map(|s| format!("http://{}:80", s)).collect();

	// Ensure parent directory exists
	if let Some(parent) = Path::new(CONFIG_PATH).parent() {
		let _ = std::fs::create_dir_all(parent);
	}

	let store = match ConcurrentFileKeyValueStore::new(CONFIG_PATH) {
		Ok(s) => s,
		Err(e) => {
			panic!("Failed to open config store: {}", e);
		}
	};

	// Build Veritas client targeting default endpoint or env override
	let client = VeritasClient::new(nodes_tokens);

    let unique_id: u32 = if let Some(existing) = store.get(UNIQUE_ID_KEY) {
        info!("Found existing unique id in config store: {}", existing);
		existing.parse::<u32>().unwrap_or(0)
	}else {
        // Request a globally unique id by incrementing a shared counter
        // Server should maintain the "unique_id" counter.
        match client.get_and_add_variable(UNIQUE_ID_KEY).await {
            Ok(id) => {
                let id_str = id.to_string();
                trace!("Obtained unique id from Veritas: {}", id_str);
                if let Err(e) = store.set(UNIQUE_ID_KEY, &id_str) {
                    error!("Failed to persist unique id: {}", e);
                }
                id as u32
            }
            Err(e) => {
                panic!("Failed to obtain unique id from Veritas: {:?}", e);
            }
        }
    };

    info!("NoReDB unique id: {}", unique_id);

    let unique_id_clone = unique_id.clone();
    tokio::spawn( async move{    // Attempt to register service with retries and exponential backoff
        let mut registration_handler = ServiceRegistrationHandler::new(client, SERVICE_REGISTRATION_KEY).await.unwrap();
    
        let mut service = ServiceRegistration {
            id: SERVICE_REGISTRATION_KEY.to_string(),
            endpoints: Vec::new(),
            meta: HashMap::<String, String>::new(),
        };

        let mut service_endpoint = 
            ServiceEndpoint {
                address: format!("{}", gethostname::gethostname().to_string_lossy()),
                port: PORT,
                id: unique_id_clone.to_string(),
                timestamp: Utc::now(),
            };

        service.endpoints.push(service_endpoint.clone());
            
        for i in 0..REGISTRATION_ATTEMPTS {
            let old_service = registration_handler.resolve_service().await; 
            if let Ok(s) = old_service {
                // Service already registered
                service = s;
            } else {
                if let Ok(_) = registration_handler.register_or_update_service(&service, None).await {
                    info!("Service registered successfully on attempt {}", i + 1);
                    break;
                }else {
                    if i == REGISTRATION_ATTEMPTS - 1 {
                        error!("Failed to register service after {} attempts, exiting", REGISTRATION_ATTEMPTS);
                    }
                }
            };

            tokio::time::sleep(Duration::from_millis(REGISTRATION_BACKOFF_MS * REGISTRATION_BACKOFF_MULTIPLIER.pow(i as u32))).await;
        }

        loop {
            // Update timestamp
            service_endpoint.timestamp = Utc::now();

            if let Err(e) = registration_handler.register_or_update_endpoint(&service_endpoint).await {
                warn!("Failed to refresh endpoint registration: {:?}", e);
            } else {
                debug!("Endpoint registration refreshed");
            }

            // Periodically refresh registration
            tokio::time::sleep(Duration::from_millis(REGISTRATION_INTERVAL_MS)).await; 
        }
    });

    let addr = format!("0.0.0.0:{}", PORT).parse()?;

    // Initialize database server components
    let canvas_db = CanvasDB::new(CANVAS_WIDTH, CANVAS_HEIGHT, CANVAS_DB_PATH, WRITE_AHEAD_LOG_SIZE);
    canvas_db.start_worker_threads(NUM_WORKER_THREADS);

    let database = MyDatabaseServer::new(canvas_db);

    // Set up health reporting
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<noredb::database_server::DatabaseServer<MyDatabaseServer>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(noredb::database_server::DatabaseServer::new(database))
        .serve(addr)
        .await?;

    Ok(())
}