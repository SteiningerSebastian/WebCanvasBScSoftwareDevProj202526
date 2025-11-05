use std::{error::Error as StdError, fmt::Display, fmt, sync::{Arc, atomic}};

use general::concurrent_file_key_value_store::ConcurrentFileKeyValueStore;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    TinyHttp(Box<dyn StdError + Send + Sync + 'static>),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO Error: {}", e),
            Error::TinyHttp(e) => write!(f, "TinyHttp Error: {}", e),
        }
    }
}

pub struct VeritasController{
    server: tiny_http::Server,
    kv_store: ConcurrentFileKeyValueStore,
    counter: Arc<atomic::AtomicU64>,
}

/// Create a new VeritasController binding to the specified key-value store.
impl VeritasController {
    /// Create a new VeritasController binding to the specified key-value store.
    /// Binds to the specified port.
    pub fn new(kv_store: ConcurrentFileKeyValueStore, port: u16) -> Result<Self, Error> {
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

        Ok(VeritasController{
            server,
            kv_store,
            counter: Arc::new(atomic::AtomicU64::new(0)),
        })
    }
}

impl VeritasController{
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
            thread_pool.execute(move || {
                let method = request.method().clone();
                let url = request.url().to_string();

                match method {
                    tiny_http::Method::Get => {
                        match url.split_once("/") {
                            Some(("/tick", _)) => {
                                // use the cloned Arc<AtomicU64> instead of borrowing self
                                Self::handle_tick_request(counter, request);
                            },
                            Some(("/get", query)) => {
                                Self::handle_get_request(kvs, query, request);
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
    fn handle_get_request(kvs: ConcurrentFileKeyValueStore, query: &str, request: tiny_http::Request) {
        let key = query.trim();
        // TODO: Think about how we do this to ensure linearizability and consistency between the nodes.

        match kvs.get(key) {
            Some(value) => {
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
}
    