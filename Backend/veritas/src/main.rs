use std::sync::Arc;
use std::{env};
use std::process;
use std::time::{Duration};
use actix_web::rt::System;
use tracing::{info, debug, error, trace, Level};
use tracing_subscriber::FmtSubscriber;

use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;

mod veritas_controller;
use veritas_controller::VeritasController;

static PATH_TO_KV_STORE: &str = "/data/veritas_kv_store.db";

/// Parse a comma-separated list of hostnames/IPs (VERITAS_NODES) into a Vec<String>
/// (preserve the original token so we can resolve DNS at connect time).
pub fn parse_node_names(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn main() {
    // Get the log level from the environment variable LOG_LEVEL
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
                Level::DEBUG
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
        let debug_nodes = "localhost,127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4";
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

    // TTL for cached DNS entries in seconds; default 5s. Can be overridden with VERITAS_DNS_TTL.
    let dns_ttl = env::var("VERITAS_DNS_TTL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(5));


    // Get the id for this node from the environment ORDINAL_NUMBER as a u32
    let ordinal_number = match env::var("ORDINAL_NUMBER") {
        Ok(v) => v,
        Err(_) => {
            if cfg!(debug_assertions) {
                // For debug builds, default to 0
                debug!("Environment variable ORDINAL_NUMBER is not set, defaulting to 0 for debug build");
                "0".to_string()
            } else {
                error!("Environment variable ORDINAL_NUMBER is not set");
                process::exit(1);
            }
        }
    };

    let id: usize = match ordinal_number.parse() {
        Ok(num) => num,
        Err(_) => {
            error!("Failed to parse ORDINAL_NUMBER '{}' as u32", ordinal_number);
            process::exit(1);
        }
    };

    // Initialize the key-value store
    let kv_store = ConcurrentFileKeyValueStore::new(PATH_TO_KV_STORE).unwrap_or_else(|e| {
        panic!("Failed to initialize key-value store at '{}': {}", PATH_TO_KV_STORE, e);
    });

    let veritas_controller = VeritasController::<ConcurrentFileKeyValueStore>::new(id, kv_store);
    let arc_vc = Arc::new(veritas_controller);

    // Start the Actix system and serve HTTP requests
    let sys = System::new();

    let arc_vc_clone = Arc::clone(&arc_vc);
    sys.runtime().spawn(async move {
        arc_vc_clone.start_ticking(nodes_tokens, Duration::from_secs(2), dns_ttl).await;
    });

    sys.block_on(async {
        // Panic on failure - as the microservice is not functional without it
        arc_vc.start_serving_http().await.unwrap();
    });
    
}