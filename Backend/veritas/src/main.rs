use std::{env, thread};
use std::process;
use std::time::{Duration};
use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;
use general::dns::DNSLookup;

mod veritas_controller;
use veritas_controller::VeritasController;

static PATH_TO_KV_STORE: &str = "veritas_kv_store.db";

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
    if cfg!(debug_assertions) {
        // keep tokens (hostnames or IPs) here for debug runs
        let debug_nodes = "127.0.0.1,127.0.0.2,localhost";
        unsafe {
            env::set_var("VERITAS_NODES", debug_nodes);
        }
        eprintln!("debug: VERITAS_NODES set to {}", debug_nodes);
    }

    let raw = match env::var("VERITAS_NODES") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Environment variable VERITAS_NODES is not set");
            process::exit(1);
        }
    };

    // Keep tokens, may be hostnames or literal IPs
    let nodes_tokens = parse_node_names(&raw);

    if nodes_tokens.len() < 3 || nodes_tokens.len() > 15 {
        eprintln!(
            "VERITAS_NODES must contain between 3 and 15 node tokens (hostnames or IPs), found {}",
            nodes_tokens.len()
        );
        process::exit(1);
    }

    println!("VERITAS_NODES parsed ({}):", nodes_tokens.len());
    for t in &nodes_tokens {
        println!("{}", t);
    }

    // TTL for cached DNS entries in seconds; default 5s. Can be overridden with VERITAS_DNS_TTL.
    let dns_ttl = env::var("VERITAS_DNS_TTL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(5));

    eprintln!("Starting ticker thread; DNS TTL = {}s", dns_ttl.as_secs());
    eprintln!("Starting HTTP server on 0.0.0.0:80 (GET /tick) using tiny_http");

    let dns_lookup = DNSLookup::new(dns_ttl);

    // Spawn ticker thread that resolves token -> IpAddr before each connect (with cache).
    {
        let tokens = nodes_tokens.clone();
        let dns_lookup = dns_lookup.clone();
        thread::spawn(move || {
            loop {
                for token in &tokens {
                    match dns_lookup.resolve_node(token) {
                        Ok(ip) => {
                            let sock = std::net::SocketAddr::new(ip, 80);
                            match std::net::TcpStream::connect_timeout(&sock, Duration::from_secs(2)) {
                                Ok(mut stream) => {
                                    let req = format!(
                                        "GET /tick HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                                        token
                                    );
                                    let _ = std::io::Write::write_all(&mut stream, req.as_bytes());
                                    // read a small amount of the response to allow the peer to close cleanly
                                    let mut buf = [0u8; 256];
                                    let _ = std::io::Read::read(&mut stream, &mut buf);
                                }
                                Err(e) => {
                                    eprintln!("ticker: failed to connect to {} ({}): {}", token, sock, e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("ticker: failed to resolve '{}': {}", token, e);
                        }
                    }
                }

                // Wait before the next round
                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }
    // Initialize the key-value store
    let kv_store = ConcurrentFileKeyValueStore::new(PATH_TO_KV_STORE).unwrap_or_else(|e| {
        panic!("Failed to initialize key-value store at '{}': {}", PATH_TO_KV_STORE, e);
    });

    let veritas_controller = VeritasController::new(kv_store, 80);
    match veritas_controller {
        Err(e) => {
            panic!("Failed to create VeritasController: {}", e);
        }
        Ok(vc) => {
            vc.start_http_serving();
        }
    }
}