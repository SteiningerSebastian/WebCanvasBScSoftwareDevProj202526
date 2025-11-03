use std::f32::consts::E;
use std::io::BufReader;
use std::{env, thread};
use std::net::{IpAddr, ToSocketAddrs};
use std::process;
use std::sync::{Arc, atomic, OnceLock, RwLock};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;

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

/// Resolve a single token (literal IP or hostname) to an IpAddr right when it's needed.
/// Returns an io::Result so callers can decide how to handle failures.
pub fn resolve_node_once(token: &str) -> std::io::Result<IpAddr> {
    // If it's a literal IP, return it immediately.
    if let Ok(ip) = token.parse::<IpAddr>() {
        return Ok(ip);
    }

    // Otherwise perform a DNS lookup on demand (use port 80 as a hint).
    match (token, 80).to_socket_addrs() {
        Ok(mut addrs) => {
            if let Some(sock) = addrs.next() {
                Ok(sock.ip())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    format!("DNS lookup succeeded for '{}' but no addresses were returned", token),
                ))
            }
        }
        Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())),
    }
}

/// Resolve using a shared cache with TTL. If cached and not expired, return cached IP.
/// Otherwise perform a fresh lookup, update the cache, and return the result.
fn resolve_node_cached(
    token: &str,
    cache: &RwLock<HashMap<String, (IpAddr, Instant)>>,
    ttl: Duration,
) -> std::io::Result<IpAddr> {
    // First try read lock to check cache quickly.
    {
        let r = cache.read().unwrap();
        if let Some((ip, ts)) = r.get(token) {
            if ts.elapsed() < ttl {
                return Ok(*ip);
            }
        }
    }

    // Cache miss or expired: resolve and update cache with write lock.
    let ip = resolve_node_once(token)?;
    let mut w = cache.write().unwrap();
    w.insert(token.to_string(), (ip, Instant::now()));
    Ok(ip)
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

    // DNS cache: token -> (IpAddr, time_of_resolution)
    let dns_cache: Arc<RwLock<HashMap<String, (IpAddr, Instant)>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // TTL for cached DNS entries in seconds; default 5s. Can be overridden with VERITAS_DNS_TTL.
    let dns_ttl = env::var("VERITAS_DNS_TTL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(5));

    eprintln!("Starting ticker thread; DNS TTL = {}s", dns_ttl.as_secs());
    eprintln!("Starting HTTP server on 0.0.0.0:80 (GET /tick) using tiny_http");

    // Spawn ticker thread that resolves token -> IpAddr before each connect (with cache).
    {
        let tokens = nodes_tokens.clone();
        let cache = dns_cache.clone();
        thread::spawn(move || {
            loop {
                for token in &tokens {
                    match resolve_node_cached(token, &cache, dns_ttl) {
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

    // This function will not return as it enters the request loop.
    start_http_serving();
}

fn start_http_serving(){
     let server = match tiny_http::Server::http("0.0.0.0:80") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to bind to 0.0.0.0:80: {}", e);
            process::exit(1);
        }
    };

    static POOL: OnceLock<Arc<threadpool::ThreadPool>> = OnceLock::new();
    let pool = POOL
        .get_or_init(|| {
            let threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(16);
            Arc::new(threadpool::ThreadPool::new(threads))
        })
        .clone();

    let counter = Arc::new(atomic::AtomicU64::new(0));
    let _new_kvs = ConcurrentFileKeyValueStore::new(PATH_TO_KV_STORE);
    
    // We must exit if we fail to initialize the key-value store.
    // Without it, we cannot server requests.
    if let Err(e) = _new_kvs {
        eprintln!("Failed to initialize key-value store at '{}': {}", PATH_TO_KV_STORE, e);
        panic!("Cannot start HTTP server without key-value store: {}", e);
    }

    let concurrent_kv_store = Arc::new(_new_kvs.unwrap());
    
    for mut request in server.incoming_requests() {
        let counter = counter.clone();

        let kvs = Arc::clone(&concurrent_kv_store);
        pool.execute(move || {
            let method = request.method().clone();
            let url = request.url().to_string();

            match method {
                tiny_http::Method::Get => {
                    match url.split_once("/") {
                        Some(("/tick", _)) => {
                            let n = counter.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                            let body = n.to_string();
                            let content_type =
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();

                            let response = tiny_http::Response::from_string(body).with_header(content_type);
                            let _ = request.respond(response);
                        },
                        Some(("/get", query)) => {
                            let key = query.trim();
                            // Think about how we do this to ensure linearizability and consistency between the nodes.

                            match kvs.get(key) {
                                Some(value)=> {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string(value)
                                        .with_status_code(200)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                },
                                None => {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("Key Not Found")
                                        .with_status_code(404)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                }
                            }
                        }
                        _ => {
                            let content_type =
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                            let response = tiny_http::Response::from_string("Not Found")
                                .with_status_code(404)
                                .with_header(content_type);
                            let _ = request.respond(response);
                        }
                    }
                },
                tiny_http::Method::Post => {
                    // Read the body - fail early if we cannot read it.
                    let mut body = String::new();
                    {
                        // limit the reader borrow to this inner scope
                        // wrap the trait object reader in a concrete BufReader so `.take()` is callable
                        let reader = request.as_reader();
                        if let Err(e) = reader.read_to_string(&mut body) {
                            let content_type =
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                            let response = tiny_http::Response::from_string(format!("Bad Request: {}", e))
                                .with_status_code(400)
                                .with_header(content_type);
                            // reader is dropped at end of this inner scope, but we can respond now too
                            let _ = request.respond(response);
                            eprintln!("Failed to read request body: {}", e);
                            return;
                        }
                    } // reader borrow ends here

                    match url.split_once("/") {
                        Some(("/set", query)) => {
                            let key = query.trim();

                            match kvs.set(key, &body) {
                                Ok(_) => {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("OK")
                                        .with_status_code(200)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                },
                                Err(e) => {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string(format!("Internal Server Error: {}", e))
                                        .with_status_code(500)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                }
                            }
                        },
                        _ => {
                            let content_type =
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                            let response = tiny_http::Response::from_string("Not Found")
                                .with_status_code(404)
                                .with_header(content_type);
                            let _ = request.respond(response);
                        }
                    }
                }
                _ => {
                    let content_type =
                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                    let response = tiny_http::Response::from_string("Not Found")
                        .with_status_code(404)
                        .with_header(content_type);
                    let _ = request.respond(response);
                }
            };
        });
    }
}