use std::{env, path::Path, process};

use tonic::{transport::Server};
use tracing::{info, debug, error, trace, Level};
use tracing_subscriber::FmtSubscriber;

use general::concurrent_file_key_value_store::{ConcurrentFileKeyValueStore, ConcurrentKeyValueStore};
use veritas_client::veritas_client::VeritasClient;

use crate::server::{MyDatabase, noredb};

mod server;

const CONFIG_PATH: &str = "/data/noredb.config";
const UNIQUE_ID_KEY: &str = "noredb.unique_id";

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

    // TODO: Register the NoReDB instance with Veritas


    // HERE THE REGISTRATION >.....


    let addr = "[::1]:50051".parse()?;
    let database = MyDatabase::default();

    Server::builder()
        .add_service(noredb::database_server::DatabaseServer::new(database))
        .serve(addr)
        .await?;

    Ok(())
}
