use std::{error::Error as StdError, fmt::{self, Display}, net::IpAddr, sync::{Arc, Mutex, PoisonError, atomic}, thread, time::Duration};

use general::{concurrent_file_key_value_store::ConcurrentFileKeyValueStore, dns::DNSLookup};

use std::net::SocketAddr;
use std::io::{Read, Write};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    TinyHttp(Box<dyn StdError + Send + Sync + 'static>),
    PoisonedMutex,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Initializing,
    Running,
    QuorumNotReached,
    Stopped,
}

enum Role {
    Leader,
    Follower,
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO Error: {}", e),
            Error::TinyHttp(e) => write!(f, "TinyHttp Error: {}", e),
            Error::PoisonedMutex => write!(f, "Mutex poisoned"),
        }
    }
}

pub struct VeritasController{
    id: usize,
    leader_id: Arc<atomic::AtomicIsize>, // None if no leader elected yet
    server: tiny_http::Server,
    kv_store: ConcurrentFileKeyValueStore,
    counter: Arc<atomic::AtomicU64>,
    state: Arc<Mutex<State>>,
    role: Arc<Mutex<Role>>,
}

/// Create a new VeritasController binding to the specified key-value store.
impl VeritasController {
    /// Create a new VeritasController binding to the specified key-value store.
    /// Binds to the specified port.
    pub fn new(id:usize, kv_store: ConcurrentFileKeyValueStore, port: u16) -> Result<Self, Error> {
        let server = match tiny_http::Server::http(format!("0.0.0.0:{}", port)) {
            Ok(s) => s,
            Err(e) => {
                // Try to downcast the boxed error into a concrete std::io::Error so we can
                // inspect the error kind; if that fails, return the original boxed error.
                match e.downcast::<std::io::Error>() {
                    Ok(io_err_box) => {
                        let io_err = *io_err_box;
                        match io_err.kind() {
                            std::io::ErrorKind::AddrInUse => {
                                eprintln!("Port 80 already in use: {}", io_err);
                                return Err(Error::Io(io_err));
                            }
                            std::io::ErrorKind::PermissionDenied => {
                                eprintln!("Permission denied binding to 0.0.0.0:80: {}", io_err);
                                return Err(Error::Io(io_err));
                            }
                            _ => {
                                eprintln!("Failed to bind to 0.0.0.0:80: {}", io_err);
                                return Err(Error::TinyHttp(Box::new(io_err)));
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to bind to 0.0.0.0:80: {}", e);
                        return Err(Error::TinyHttp(e));
                    }
                }
            }
        };

        let counter = kv_store.get("counter").unwrap_or("0".to_string());
        let counter: u64 = counter.parse().unwrap_or(0);

        Ok(VeritasController{
            id,
            leader_id: Arc::new(atomic::AtomicIsize::new(-1)), // No leader elected yet - id = -1
            server,
            kv_store,
            counter: Arc::new(atomic::AtomicU64::new(counter)),
            state: Arc::new(Mutex::new(State::Initializing)),
            role: Arc::new(Mutex::new(Role::Follower)), // Default role is Follower
        })
    }
}

impl VeritasController{
    /// Start the ticker thread that periodically resolves node tokens and sends /tick requests.
    /// This function will return immediately, and the ticker will run in a separate thread.
    /// Parameters:
    /// - `tokens`: A vector of node tokens (hostnames or IPs) to tick.
    /// - `dns_ttl`: The TTL duration for DNS caching.
    /// - `tick_interval`: The interval duration between each tick round.
    /// 
    /// # Examples
    /// ``` ignore
    /// let tokens = vec!["node1.example.com".to_string(), "node2.example.com".to_string()];
    /// controller.start_ticking(tokens, Duration::from_secs(5), Duration::from_secs(10));
    /// ```
    /// This will start a ticker that resolves the tokens with a DNS TTL of 5 seconds
    pub fn start_ticking(&mut self, tokens: Vec<String>, dns_ttl: Duration, tick_interval: Duration) -> Result<(), Error> {
        let dns_lookup = DNSLookup::new(dns_ttl);
        let counter = Arc::clone(&self.counter);

        // Initially set state to QuorumNotReached until we verify connectivity
        let state_arc = Arc::clone(&self.state);
        {
            let mut s = state_arc.lock().map_err(|_| Error::PoisonedMutex)?;
            *s = State::QuorumNotReached;
        }
        
        // Move a clone of the Arc into the spawned thread so it can update state.
        let state_arc = Arc::clone(&self.state);
        let id = self.id;
        let leader_id_arc = Arc::clone(&self.leader_id);

        thread::spawn(move || {
            loop {
                let mut reachable_nodes = 1; // Count self as reachable
                let mut leader_proposed_id = id;
                for i in 0..tokens.len() {
                    if i == id {
                        continue; // Skip self, as we are always reachable to ourselves
                    }

                    let token = &tokens[i];
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
                                    let res  = std::io::Read::read(&mut stream, &mut buf);

                                    match res {
                                        Ok(n) if n > 0 => {
                                            let s = String::from_utf8_lossy(&buf[..n]);
                                            // Try to extract body after the header-body separator; fall back to whole response.
                                            let body = s.split("\r\n\r\n").nth(1).unwrap_or(&s).trim();
                                            match body.parse::<u64>() {
                                                Ok(tick_val) => {
                                                    // Successfully parsed tick value
                                                    eprintln!("ticker: parsed tick value {} from {}", tick_val, token);
                                                    counter.store(tick_val, atomic::Ordering::SeqCst);
                                                    reachable_nodes += 1;

                                                    // Record proposed leader ID - select the node with the lowest ID
                                                    if i < leader_proposed_id {
                                                        leader_proposed_id = i;
                                                    }
                                                }
                                                Err(e) => {
                                                    eprintln!("ticker: failed to parse tick value from {}: {} (body: {:?})", token, e, body);
                                                }
                                            }
                                        }
                                        Ok(_) => {
                                            eprintln!("ticker: empty response from {}", token);
                                        }
                                        Err(e) => {
                                            eprintln!("ticker: failed to read response from {}: {}", token, e);
                                            continue;
                                        }
                                    }
                                    

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

                // Determine if quorum is reachable based on reachable nodes
                if reachable_nodes >= (tokens.len() / 2) + 1 {
                    match state_arc.lock() {
                        Ok(mut s) => { *s = State::Running; }
                        Err(_) => { eprintln!("ticker: state mutex poisoned when setting Running"); }
                    }
                } else {
                    match state_arc.lock() {
                        Ok(mut s) => { *s = State::QuorumNotReached; }
                        Err(_) => { eprintln!("ticker: state mutex poisoned when setting QuorumNotReached"); }
                    }
                }

                let state = match state_arc.lock() {
                    Ok(s) => s,
                    Err(_) => {
                        eprintln!("ticker: state mutex poisoned when reading state");
                        // Wait before next attempt
                        std::thread::sleep(tick_interval);
                        continue;
                    }
                };

                if *state == State::Running {
                    eprintln!("ticker: quorum reached ({} out of {} nodes reachable)", reachable_nodes, tokens.len());
                    // Select the node with the lowest ID as leader
                    eprintln!("ticker: proposed leader is node {}", leader_proposed_id);

                    // TODO: Before changing the leader, allow for current requests from leader to finish as they are timed before the change in leadership occurred.
                    // Sleep a bit here to allow in-flight requests to complete
                    std::thread::sleep(std::time::Duration::from_millis(100));

                    // Update the current leader ID
                    leader_id_arc.store(leader_proposed_id as isize, atomic::Ordering::SeqCst);
                } else {
                    eprintln!("ticker: quorum NOT reached ({} out of {} nodes reachable)", reachable_nodes, tokens.len());
                }

                // Wait before the next round
                std::thread::sleep(tick_interval);
            }
        });

        Ok(())
    }

    /// Start serving HTTP requests.
    /// This function will block the current thread.
    pub fn start_http_serving(&self){
        let thread_pool = general::thread_pool::ThreadPool::new(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        );
        
        for mut request in self.server.incoming_requests() {
            // Clone everything we need from `self` so the spawned closure doesn't capture `&self`.
            let kvs = self.kv_store.clone();
            let counter = Arc::clone(&self.counter);
            let state = Arc::clone(&self.state);
            let leader = self.get_leader_id();
            thread_pool.execute(move || {
                let method = request.method().clone();
                let url = request.url().to_string();

                match method {
                    tiny_http::Method::Get => {
                        let url = url.split("/").collect::<Vec<&str>>();
                        match url[0] {
                            "tick" => {
                                // use the cloned Arc<AtomicU64> instead of borrowing self
                                //Self::handle_tick_request(counter, request);
                            },
                            "get" => {
                                let query = &url[1..].join("/");
                                let linearizable = true;
                                if url.len() > 1 {
                                    let linearizable = url[1] != "false";
                                }
                                //Self::handle_get_request(kvs, linearizable, query, request);

                            }
                            "request_vote" => {
                                let query = &url[1..].join("/");
                                let sender: usize = query.parse().unwrap_or(0);

                                // Vote for the candidate we have chosen as our leader.
                                let vote = sender == self.get_leader_id() as usize;

                                // Respond with vote result
                                let content_type =
                                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                let response = tiny_http::Response::from_string(if vote {
                                    "True"
                                } else {
                                    "False"
                                })
                                .with_status_code(if vote { 200 } else { 403 })
                                .with_header(content_type);
                                let _ = request.respond(response);
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

                        let url = url.split("/").collect::<Vec<&str>>();
                        match url[0] {
                            // This is called by the leader of the cluster to set a key on followers.
                            // After verifying the sender is the current leader, the set is authoritively applied without further checks.
                            "leader_set" => {
                                // Check current state before proceeding
                                let state = match state.lock() {
                                    Ok(s) => s,
                                    // If we cannot acquire the lock, respond with 503 Service Unavailable
                                    Err(_) => {
                                        let content_type =
                                            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                        let response = tiny_http::Response::from_string("Service Unavailable: internal error")
                                            .with_status_code(503)
                                            .with_header(content_type);
                                        let _ = request.respond(response);
                                        eprintln!("Rejected leader_set request: state mutex poisoned");
                                        return;
                                    }
                                };

                                // If not running, reject the request
                                if *state != State::Running {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("Service Unavailable: quorum not reached")
                                        .with_status_code(503)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                    eprintln!("Rejected leader_set request: quorum not reached");
                                    return;
                                }

                                // Expecting URL format: /leader_set/{sender_id}/{key}
                                if url.len() < 3 {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("Bad Request: missing sender ID or query")
                                        .with_status_code(400)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                    eprintln!("Missing sender ID or query in leader_set request");
                                    return;
                                }

                                // Split sender ID and query
                                let (sender, key) = (url[1], url[2..].join("/"));
                                let sender = sender.parse::<usize>();
                                // Validate sender ID
                                if sender.is_err() {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("Bad Request: invalid sender ID")
                                        .with_status_code(400)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                    eprintln!("Invalid sender ID in leader_set request: {}", url[1]);
                                    return;
                                }

                                // Verify that the sender is the current leader
                                let sender_id = sender.unwrap();
                                if sender_id != leader as usize {
                                    let content_type =
                                        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
                                    let response = tiny_http::Response::from_string("Forbidden: not leader")
                                        .with_status_code(403)
                                        .with_header(content_type);
                                    let _ = request.respond(response);
                                    eprintln!("Rejected leader_set request from non-leader node {}", sender_id);
                                    return;
                                }

                                Self::handle_leader_set(kvs, &key, body.as_bytes(), request);
                            },
                            "set" => {
                                unimplemented!();
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

    /// Handle a /tick request by incrementing and returning the counter.
    fn handle_tick_request(counter: Arc<atomic::AtomicU64>, request: tiny_http::Request) {
        let n = counter.fetch_add(1, atomic::Ordering::SeqCst) + 1;
        let body = n.to_string();
        let content_type =
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();

        let response = tiny_http::Response::from_string(body).with_header(content_type);
        let _ = request.respond(response);
    }

    /// Handle a /get request by retrieving the value for the specified key.
    fn handle_get_request(kvs: ConcurrentFileKeyValueStore, linearizable: bool, query: &str, request: tiny_http::Request) {
        unimplemented!()
    }

    fn handle_set_request_as_leader(query: &str, body: &[u8], request: tiny_http::Request) {
        // call /set_leader on all followers

        
    }

    /// Handle a /leader_set request by setting the value for the specified key.
    fn handle_leader_set(kvs: ConcurrentFileKeyValueStore, key: &str, body: &[u8], request: tiny_http::Request) {
        let value = String::from_utf8_lossy(body).to_string();
        kvs.set(key, &value); // TODO: Do I need a TIMESTAMP here? I think so! Because a majority vote may not pass.

        Self::respond_ok(request);
    }

    /// Respond with a simple "OK" message.
    fn respond_ok(request: tiny_http::Request) {
        let content_type =
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap();
        let response = tiny_http::Response::from_string("OK")
            .with_status_code(200)
            .with_header(content_type);
        let _ = request.respond(response);
    }

    /// Get the current leader ID, or -1 if no leader elected yet.
    fn get_leader_id(&self) -> isize {
        self.leader_id.load(atomic::Ordering::SeqCst)
    }

    fn post_set_to_leader(leader: IpAddr, key: &str, value: &str) -> Result<(), Error> {
        let sock = SocketAddr::new(leader, 80);
        let mut stream = match std::net::TcpStream::connect_timeout(&sock, Duration::from_secs(2)) {
            Ok(s) => s,
            Err(e) => return Err(Error::Io(e)),
        };

        // Set a read timeout so we don't block indefinitely waiting for the leader response.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));

        let body = value.as_bytes();
        let req = format!(
            "POST /set/{} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            key,
            leader,
            body.len()
        );

        if let Err(e) = stream.write_all(req.as_bytes()).and_then(|()| stream.write_all(body)) {
            return Err(Error::Io(e));
        }

        let mut resp_buf = Vec::new();
        match stream.read_to_end(&mut resp_buf) {
            Ok(_) => {
                let resp_str = String::from_utf8_lossy(&resp_buf);
                // Parse status code from status line "HTTP/1.1 200 OK"
                if let Some(status_line) = resp_str.lines().next() {
                    let parts: Vec<&str> = status_line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(code) = parts[1].parse::<u16>() {
                            if (200..300).contains(&code) {
                                return Ok(());
                            } else {
                                let err = std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("leader returned HTTP {}", code),
                                );
                                return Err(Error::TinyHttp(Box::new(err)));
                            }
                        }
                    }
                }
                let err = std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "malformed HTTP response from leader",
                );
                Err(Error::TinyHttp(Box::new(err)))
            }
            Err(e) => Err(Error::Io(e)),
        }
    }
}