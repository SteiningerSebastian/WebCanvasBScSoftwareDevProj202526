use std::{env, net::SocketAddr, path::Path, process};

use tonic::{transport::Server};
use tracing::{Level, debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;

use general::concurrent_file_key_value_store::{ConcurrentFileKeyValueStore, ConcurrentKeyValueStore};
use veritas_client::veritas_client::VeritasClient;

use crate::server::{MyDatabase, noredb};

mod server;

const CONFIG_PATH: &str = "/data/noredb.config";
const UNIQUE_ID_KEY: &str = "noredb.unique_id";
const SERVICE_REGISTRATION_KEY: &str = "noredb.registered_instances";
const PORT : u16 = 50051;
const REGISTRATION_ATTEMPTS: usize = 4; // number of attempts to register service
const REGISTRATION_BACKOFF_MS: u64 = 200; // milliseconds between attempts
const REGISTRATION_BACKOFF_MULTIPLIER: u64 = 5; // exponential backoff multiplier

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

    // Register this instance in the shared set of registered instances
    // First lets look up existing registered instances id:ip;id:ip;...
    let mut delay = REGISTRATION_BACKOFF_MS;
    for _ in 0..REGISTRATION_ATTEMPTS {
        if try_register_instance(&client, unique_id).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        delay *= REGISTRATION_BACKOFF_MULTIPLIER;
    }

    let addr = format!("0.0.0.0:{}", PORT).parse()?;
    let database = MyDatabase::new(store);

    Server::builder()
        .add_service(noredb::database_server::DatabaseServer::new(database))
        .serve(addr)
        .await?;

    Ok(())
}

async fn try_register_instance(client: &VeritasClient, unique_id: u32) -> Result<(), &'static str> {
    let current_value;
    let mut registered_instances = match client.get_variable(SERVICE_REGISTRATION_KEY).await {
        Ok(val) => {
            current_value = val.clone(); // keep a copy of current value
            let instances: Vec<(usize, SocketAddr)> = if val.is_empty() {
                Vec::new()
            } else {
                val.split(';')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.split(':').map(|x| x.to_string()).collect::<Vec<String>>())
                    .map(|s: Vec<String>| (s[0].parse::<usize>().unwrap(), format!("{}:{}", s[1], PORT).parse::<SocketAddr>().unwrap()))
                    .collect()
            };
            instances
        }
        Err(e) => {
            panic!("Failed to look up registered instances: {:?}", e);
        }
    };

    // Look for our id in the existing set
    registered_instances.iter().for_each(|(id, addr)| {
        info!("Registered instance - id: {}, addr: {}", id, addr);
    });

    let mut existing_registration: Option<&mut (usize, SocketAddr)> = registered_instances.iter_mut().find(|(id, _)| *id == unique_id as usize);

    // If not found, add ourselves
    if let None = existing_registration {
        registered_instances.push((unique_id as usize, format!("0.0.0.0:{}", PORT).parse().unwrap()));
        existing_registration = registered_instances.iter_mut().find(|(id, _)| *id == unique_id as usize);
    }

    // Get our own ip address
    let my_ip = local_ip_address::local_ip();

    if let Err(e) = my_ip {
        panic!("Failed to obtain local IP address: {}", e);
    }

    let my_ip = my_ip.unwrap().to_string();

    if let Some((_, addr)) = existing_registration {
        *addr = format!("{}:{}", my_ip, PORT).parse().unwrap();
    }else{
        panic!("Failed to register this instance, could not find own registration after adding.");
    }

    // Persist updated registered instances back to Veritas
    let new_value = registered_instances.iter()
        .map(|(id, addr)| format!("{}:{}", id, addr.ip()))
        .collect::<Vec<String>>()
        .join(";");

    if let Ok(has_been_set) = client.compare_and_set_variable(SERVICE_REGISTRATION_KEY, 
    &current_value, 
    &new_value).await {
        if has_been_set {
            info!("Successfully registered instance with id {} at {}", unique_id, my_ip);
            return Ok(()); // success
        } else {
            warn!("Compare-and-set failed, another instance may have modified the registered instances concurrently.");
        }
    }

    return Err("Failed to register instance after compare-and-set attempts");
}