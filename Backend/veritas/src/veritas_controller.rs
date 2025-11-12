use actix_web::{App, HttpRequest, HttpResponse, HttpServer, ResponseError, http::{StatusCode, header::ContentType}, web};
use general::{concurrent_file_key_value_store::{self, ConcurrentKeyValueStore}, dns::{self, DNSLookup}};
use regex::Regex;
use reqwest::Client;
use tokio::{task::JoinSet};
use tracing::{debug, info, trace, warn};
use std::{fmt::Display, sync::{RwLock, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::Duration};

#[derive(Debug)]
pub enum Error {
    FileKeyValueStoreError(concurrent_file_key_value_store::Error),
    QuorumNotReached,
    ForwardRequestError(reqwest::Error),
    DNSLookupError(std::io::Error),
    TransactionBeginError(concurrent_file_key_value_store::Error),
    TransactionCommitError(concurrent_file_key_value_store::Error),
    UnauthorizedSet,
    LockPoisoned,
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::html())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::QuorumNotReached => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileKeyValueStoreError(e) => write!(f, "File key-value store error: {}", e),
            Error::QuorumNotReached => write!(f, "Quorum not reached"),
            Error::ForwardRequestError(e) => write!(f, "Failed to forward request: {}", e),
            Error::LockPoisoned => write!(f, "Lock poisoned error"),
            Error::UnauthorizedSet => write!(f, "Unauthorized set request"),
            Error::TransactionBeginError(e) => write!(f, "Failed to begin transaction: {}", e),
            Error::TransactionCommitError(e) => write!(f, "Failed to commit transaction: {}", e),
            Error::DNSLookupError(e) => write!(f, "DNS lookup error: {}", e),
        }
    }
}

impl From<concurrent_file_key_value_store::Error> for Error {
    fn from(e: concurrent_file_key_value_store::Error) -> Self {
        Error::FileKeyValueStoreError(e)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Initializing,
    Running,
    QuorumNotReached,
}

impl Clone for State {
    fn clone(&self) -> Self {
        match self {
            State::Initializing => State::Initializing,
            State::Running => State::Running,
            State::QuorumNotReached => State::QuorumNotReached,
        }
    }
}
impl Copy for State {}

pub struct AppState<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    time: AtomicU64,
    kv_store: F,
    state: RwLock<State>,
    candidate_id: AtomicUsize,
    leader_id: AtomicUsize,
    id: usize,
    node_tokens: Vec<String>,
    dns_lookup: dns::DNSLookup,

}

pub struct VeritasController<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    state: web::Data<AppState<F>>,
}

impl<F> VeritasController<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    /// Create a new VeritasController with the given ID and key-value store
    /// Parameters:
    /// - id: Unique identifier for this VeritasController instance
    /// - kv_store: Shared ConcurrentFileKeyValueStore for data storage
    /// # Examples
    /// ```ignore
    /// let kv_store = ConcurrentFileKeyValueStore::new(...);
    /// let controller = VeritasController::new(1, kv_store);
    /// ```
    pub fn new(id: usize, kv_store: F, node_tokens: Vec<String>, dns_ttl: Duration) -> Self {
        VeritasController {
            state: web::Data::new(AppState {
                id,
                time: AtomicU64::new(0),
                kv_store: kv_store,
                state: RwLock::new(State::Initializing),
                candidate_id: AtomicUsize::new(id),
                leader_id: AtomicUsize::new(usize::MAX), // Use the last id as no leader - quorum unreachable
                node_tokens: node_tokens,
                dns_lookup: dns::DNSLookup::new(dns_ttl),
            }),
        }
    }

    /// Check if other veritas nodes can be reached and update internal state accordingly
    ///
    /// This function will periodically check the health of other Veritas nodes
    /// and update the internal state of the controller accordingly.
    /// 
    /// Parameters:
    /// - node_tokens: A vector of node identifiers (hostnames or IPs) to check
    /// - period: Duration between each tick/check
    /// - dns_ttl: Duration for DNS cache TTL
    pub async fn start_ticking(&self, period: std::time::Duration) {
        // Placeholder for ticking logic
        trace!("VeritasController {} started ticking.", self.state.id);

        loop {
            let mut n_available_nodes: usize = 1; // Count self as available
            let mut lowest_candidate_id: usize = self.state.id; // Start with self as candidate

            // Check the health of other nodes
            for i in 0..self.state.node_tokens.len() {
                if i == self.state.id {
                    continue; // Skip self
                }

                let token = &self.state.node_tokens[i];
                trace!("VeritasController {} checking node: {}", self.state.id, token);

                match self.state.dns_lookup.resolve_node(token) {
                    Ok(ip) => {
                        trace!("Resolved node '{}' to IP: {}", token, ip);

                        let current_time = self.state.time.load(Ordering::SeqCst);
                        // Tick the node and wait for response
                        let (success, returned_time) = Self::send_tick_to_node(ip, current_time).await;
                        if success {
                            trace!("Successfully ticked node '{}' at IP: {}. Returned time: {}", token, ip, returned_time);

                            // Making sure that we are not to far behind the leader that we ellect
                            let time = self.state.time.load(Ordering::SeqCst);
                            if time < returned_time {
                                self.state.time.fetch_add(returned_time - time, Ordering::SeqCst);
                            }

                            // Increment available nodes count
                            n_available_nodes += 1;

                            // Update the candidate id - elect the lowest id among available nodes
                            lowest_candidate_id = lowest_candidate_id.min(i);

                        } else {
                            debug!("Failed to tick node '{}' at IP: {}", token, ip);
                        }
                    }
                    Err(e) => {
                        trace!("Failed to resolve node '{}': {}", token, e);
                    }
                }
            }

            trace!("VeritasController {} found {} available nodes.", self.state.id, n_available_nodes);

            // Update internal state based on available nodes
            {
                let mut state_guard = self.state.state.write().unwrap();
                if n_available_nodes * 2 > self.state.node_tokens.len() {
                    // Quorum reached
                    if *state_guard != State::Running {
                        trace!("VeritasController {} transitioning to Running state.", self.state.id);
                    }
                    *state_guard = State::Running;
                } else {
                    // Quorum not reached
                    if *state_guard != State::QuorumNotReached {
                        warn!("VeritasController {} transitioning to QuorumNotReached state.", self.state.id);
                    } 
                    *state_guard = State::QuorumNotReached;
                }
            }

            self.state.candidate_id.store(lowest_candidate_id, Ordering::SeqCst);

            if self.state.id == lowest_candidate_id {
                info!("VeritasController {} is the current leader candidate.", self.state.id);
                // Ask other nodes to elect me as leader
                let n_votes = self.ask_for_votes().await + 1; // Count self vote
                trace!("VeritasController {} received {} votes.", self.state.id, n_votes);
                if n_votes * 2 > self.state.node_tokens.len() {
                    trace!("VeritasController {} has been elected as leader with {} votes.", self.state.id, n_votes);

                    if self.state.leader_id.load(Ordering::SeqCst) != self.state.id {
                        info!("VeritasController {} is now the new leader. Wait out the current period - for current leader to step down.", self.state.id);
                        tokio::time::sleep(period).await;

                        // After wainting for the old leader to step down - ask for votes again to make sure we are still the leader.
                        let n_votes = self.ask_for_votes().await + 1;
                        if n_votes * 2 <= self.state.node_tokens.len() {
                            warn!("VeritasController {} failed to be elected as leader after waiting for old leader to step down, only received {} votes.", self.state.id, n_votes);
                            continue;
                        }

                        info!("VeritasController {} has taken over as leader.", self.state.id);
                    }

                    // I am now the leader
                    self.state.leader_id.store(self.state.id, Ordering::SeqCst);
                } else {
                    warn!("VeritasController {} failed to be elected as leader, only received {} votes.", self.state.id, n_votes);
                }
            } else {
                trace!("VeritasController {} recognizes {} as the current leader candidate.", self.state.id, lowest_candidate_id);
            }

            // Tick the other nodes
            trace!("VeritasController {} tick.", self.state.id);
            tokio::time::sleep(period).await;
        }
    }

    /// Send a tick to another node and await its response
    async fn send_tick_to_node(ip: std::net::IpAddr, time: u64) -> (bool, u64) {
        // Placeholder for sending tick logic
        trace!("Sending tick to node at IP: {} with time: {}", ip, time);

        let rsp = reqwest::Client::new()
            .get(&format!("http://{}:80/tick/{}", ip, time))
            .timeout(Duration::from_secs(1))// We are in a local network, so timeout can be short
            .send()
            .await;

        if let Err(e) = rsp {
            debug!("Failed to send tick to node at IP {}: {}", ip, e);
            return (false, 0);
        }
        let rsp = rsp.unwrap();

        let success = rsp.status().is_success();
        let returned_time = rsp.text().await;

        // Check if the response text is valid
        if let Err(e) = returned_time {
            warn!("Failed to read response text from node at IP {}: {}", ip, e);
            return (success, 0);
        }
        let returned_time = returned_time.unwrap().parse::<u64>().unwrap_or(0);

        (success, returned_time)
    }

    /// Broadcast a request to all other nodes and collect their responses
    /// 
    /// Parameters:
    /// - path: The request path to broadcast
    /// - node_tokens: A vector of node identifiers (hostnames or IPs) to send the request to
    /// - dns_lookup: DNSLookup instance for resolving node addresses
    /// - timeout: Duration to wait for each request
    /// - method: HTTP method to use for the request
    /// - body: Optional body to include in the request
    /// 
    /// Returns:
    /// - Vec<Option<String>>: A vector of optional responses from each node
    /// 
    /// # Examples
    /// ```ignore
    /// let responses = controller.broadcast_and_collect_responses("/status", node_tokens, dns_lookup).await;
    /// ```
    async fn broadcast_and_collect_responses(id:usize, path: &str, node_tokens: &Vec<String>, dns_lookup: &DNSLookup, timeout: Duration, method: reqwest::Method, body: Option<String>) -> JoinSet<Option<(usize, String)>> {
        let mut req_set = JoinSet::new();
        let path = path.to_string();

        // Broadcast the request to all nodes and collect responses
        for i in 0..node_tokens.len() {
            if i == id {
                continue; // Skip self
            }

            trace!("Broadcasting request to node: {}", i);

            let node = node_tokens[i].clone();
            let dns_lookup = dns_lookup.clone();
            let path = path.clone();
            let method = method.clone();
            let body = body.clone();
            let req = async move {
                match dns_lookup.resolve_node(&node) {
                    Ok(ip) => {
                        trace!("Resolved node '{}' to IP: {}", node, ip);
                        // Send request to the node and handle response
                        let rsp = reqwest::Client::new()
                            .request(method.clone(), &format!("http://{}:80{}", ip, path))
                            .timeout(timeout) // We are in a local network, so timeout can be short
                            .body(body.unwrap_or_default()) // Add body if provided
                            .send().await;

                        match rsp {
                            Ok(response) => {
                                if response.status().is_success() {
                                    match response.text().await {
                                        Ok(text) => Some((i ,text)),
                                        Err(e) => {
                                            debug!("Failed to read response text from node '{}': {}", node, e);
                                            None
                                        }
                                    }
                                } else {
                                    debug!("Non-success status code from node '{}': {}", node, response.status());
                                    None
                                }
                            }
                            Err(e) => {
                                debug!("Failed to send request to node '{}': {}", node, e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to resolve node '{}': {}", node, e);
                        None
                    }
                }  
            };
            req_set.spawn(req);
        }
        req_set
    }

    /// Ask other nodes for votes and count the number of granted votes
    /// 
    /// Returns:
    /// - usize: Number of granted votes
    /// 
    /// # Examples
    /// ```ignore
    /// let votes = controller.ask_for_votes(dns_lookup, node_tokens).await;
    /// ```
    async fn ask_for_votes(&self) -> usize{
        // Placeholder for asking votes logic
        let candidate_id = self.state.candidate_id.load(Ordering::SeqCst);

        // Only ask for votes if we are the candidate
        assert!(candidate_id == self.state.id);
        
        trace!("VeritasController {} asking for votes for candidate {}.", self.state.id, candidate_id);
        let rsp_set = Self::broadcast_and_collect_responses(
            self.state.id, 
            &format!("/vote/{}", candidate_id), 
            &self.state.node_tokens, 
            &self.state.dns_lookup, 
            Duration::from_secs(1),
            reqwest::Method::GET,
            None
        ).await;
        // Wait for all responses
        let responses = rsp_set.join_all().await;

        let votes = responses.iter().map(| e| {
            if let Some((i, vote)) = e {
                // Process the vote
                trace!("VeritasController {} received vote from {}: {}", self.state.id, i, vote);
                let vote_granted: bool = vote.to_lowercase() == "true";
                vote_granted
            }else{
                // Handle no vote received
                false
            }
        });

        // Count the number of granted votes
        votes.filter(|e| *e == true).count() as usize
    }

    /// An example handler that increments and returns a counter
    pub async fn tick(req: HttpRequest, data: web::Data<AppState<F>>) -> String {
        let time: u64 = req.match_info().get("time")
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);

        trace!("Tick handler called with time: {}", time);

        let current_time = data.time.load(Ordering::SeqCst);
        // Ensure atomic time is also updated -> sequencial consistency - can skip multiple ticks
        if time > current_time {
            data.time.fetch_add(time - current_time + 1, Ordering::SeqCst);
        }else {
            data.time.fetch_add(1, Ordering::SeqCst);
        }

        data.time.load(Ordering::SeqCst).to_string()
    }

    /// Handle the vote request from other nodes.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the vote request
    /// - data: Shared application state
    ///
    /// Returns:
    /// - String: The result of the vote request ("true" or "false")
    pub async fn vote(req: HttpRequest, data: web::Data<AppState<F>>) -> String {
        // For simplicity, always grant the vote
        trace!("Vote handler called.");

        let sender_id: usize = req.match_info().get("sender")
            .unwrap_or(usize::MAX.to_string().as_str())
            .parse()
            .unwrap_or(usize::MAX);

        let vote = if sender_id == data.candidate_id.load(Ordering::SeqCst) {
            debug!("Granting vote to sender ID: {}", sender_id);

            // Accepting the sender as the new leader - assuming they will get the necessary votes
            data.leader_id.store(sender_id, Ordering::SeqCst);

            "true".to_string()
        } else {
            debug!("Denying vote to sender ID: {}", sender_id);
            "false".to_string()
        };

        vote
    }

    /// Handle the peek request to retrieve a value from the key-value store.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the peek requestq
    /// - data: Shared application state
    /// Returns:
    /// - String: The value associated with the requested key, or an empty string if not found
    pub async fn peek(req: HttpRequest, data: web::Data<AppState<F>>) -> String {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        trace!("Peek handler called with key: {}", key);

        match data.kv_store.get(&key) {
            Some(value) => value,
            None => "".to_string(),
        }
    }

    /// Handle the get request to retrieve a value from a quorum of nodes.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the get request
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value associated with the requested key if quorum is reached, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.get(req, data).await?;
    /// ```
    pub async fn get_eventual(req: HttpRequest, data: web::Data<AppState<F>>) -> Result<String, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        trace!("Get handler called with key: {}", key);
        // Ask the quorum of nodes for the value
        let mut join_set = Self::broadcast_and_collect_responses(
            data.id, 
            &format!("/peek/{}", key), 
            &data.node_tokens, 
            &data.dns_lookup, 
            Duration::from_millis(100),
            reqwest::Method::GET,
            None
        ).await;

        let mut responses: Vec<String> = Vec::new();
        // Wait for a quorum of responses
        for _ in 0..data.node_tokens.len() {
            let next = join_set.join_next().await;
            if let Some(rsp) = next {
                if let Ok(res) = rsp {
                    if let Some((_, text)) = res {
                        responses.push(text);
                        // Break if we have a quorum
                        if responses.len() * 2 > data.node_tokens.len() {
                            break;
                        }
                    }
                }
            }
        }

        // Check if we reached a quorum - if not, return an error
        if responses.len() * 2 <= data.node_tokens.len() {
            return Err(Error::QuorumNotReached);
        }

        // Process responses to find the most recent value
        let responses = responses.iter().map(|i| {
            let time = i.split_once(";");
            if let Some((t, v)) = time {
                let t = t.parse::<u64>().unwrap_or(0);
                (t, v.to_string())
            }else{
                (0, "".to_string())
            }
        });

        let newest = responses.max_by_key(|i|i.0).unwrap_or((0, "".to_string())).1;
        Ok(newest)
    }
    
    /// Handle the set request to store a value in the key-value store from the leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the set request
    /// - data: Shared application state
    /// - bytes: The body of the incoming request
    /// 
    /// Returns:
    /// - Result<String, Error>: The new value associated with the requested key
    /// 
    /// # Examples
    /// ```ignore
    /// let response = controller.set_by_leader(req, data, bytes).await?;
    /// ```
    pub async fn set_by_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<String, Error> {
        // We only accept set requests from the current leader node
        // Others must be rejected and sent to the correct leader by the caller
        let sender: String = req.match_info().get("sender")
            .unwrap_or("")
            .to_string();

        let sender_id = sender.parse::<usize>().unwrap_or(usize::MAX);
        trace!("Set by leader handler called from sender ID: {}", sender_id);
        if sender_id != data.leader_id.load(Ordering::SeqCst) {
            warn!("Received set request from non-leader sender ID: {}. Current leader ID: {}", sender_id, data.leader_id.load(Ordering::SeqCst));
            return Err(Error::UnauthorizedSet);
        }
        
        // Process the set request
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        debug!("Set handler called with key: {} and value: {}", key, value);

        // Set the value in the local key-value store
        data.kv_store.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

        Ok(value)
    }

    /// Handle the set request when this node is the leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the set request
    /// - data: Shared application state
    /// - bytes: The body of the incoming request
    /// 
    /// Returns:
    /// - Result<String, Error>: The new value associated with the requested key
    /// 
    /// # Examples
    /// ```ignore
    /// let response = controller.set_for_leader(req, data, bytes).await?;
    /// ```
    async fn set_for_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<String, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        debug!("Set handler called with key: {} and value: {}", key, value);

        { // Making sure local set is atomic with transaction / order of set operations
            let transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
            
            // Prepend the current time to the value for versioning
            let value = format!("{};{}", data.time.load(Ordering::SeqCst), value);

            // Set the value in the local key-value store first do gurantee linearizability when reading from leader.
            data.kv_store.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

            // Commit the transaction
            transaction.commit().map_err(Error::TransactionCommitError)?;
        } // End of atomic block / transaction

        // Broadcast the set to other nodes to replicate the value
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();
        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        debug!("Broadcasting set request for key: {} to other nodes.", key);

        let responses = Self::broadcast_and_collect_responses(
            data.id, 
            &format!("/force_set/{}/{}", data.id, key), // Include sender id so the receiver can verify that this node is the leader
            &data.node_tokens, 
            &data.dns_lookup, 
            Duration::from_millis(100),
            reqwest::Method::POST,
            Some(value.clone())
        ).await;

        // Count successful replications
        let mut n_successful = 1; // Count self
        responses.join_all().await.iter().for_each(|e| {
            if let Some(_) = e {
                n_successful += 1;
            }
        });

        // If the quorum is not reached, return an error. The change may still be applied - but no guarantees.
        trace!("Set request for key: {} replicated to {} nodes.", key, n_successful);
        if n_successful * 2 <= data.node_tokens.len() {
            warn!("Failed to replicate set request for key: {} to a quorum of nodes.", key);
            return Err(Error::QuorumNotReached);
        }

        Ok(value)
    }

    /// Handle the get request when this node is the leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the get request
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value associated with the requested key
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.get_for_leader(req, data).await?;
    /// ```
    pub async fn get_for_leader(req: HttpRequest, data: web::Data<AppState<F>>) -> Result<String, Error> {
        Self::get_eventual(req, data).await
        //TODO: Read Repair - reach quorum before returning value to make sure no gurantees are broken.
        // (for read 2, write 2) we need to read 3 equal values to guarantee consistency.


    }

    /// Handle a request by either processing it as the leader or forwarding it to the current leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the incoming request
    /// - data: Shared application state
    /// - bytes: The body of the incoming request
    /// 
    /// Returns:
    /// - Result<String, Error>: The response from handling the request or forwarding it to the leader
    /// 
    /// # Examples
    /// ```ignore
    /// let response = controller.handle_by_leader(req, data, bytes).await?;
    /// ```
    pub async fn handle_by_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<String, Error> {
        let state = *(&data).state.read().map_err(|_|Error::LockPoisoned)?;
        
        if state == State::Running && data.leader_id.load(Ordering::SeqCst) == data.id {
            // Hey, it's me, the leader
            trace!("Handling request as leader. {}", req.path());
            
            // Handle the request locally - as we are the leader
            if Regex::new(r"^\/get\/([^\/]+)$").unwrap().is_match("/get/{key}") {
                return Self::get_for_leader(req, data).await;
            } else if Regex::new(r"^\/set\/([^\/]+)$").unwrap().is_match("/get/{key}") {
                return Self::set_for_leader(req, data, bytes).await;
            } else {
                warn!("Unhandled path: {}", req.path());
                Ok("UNHANDLED".to_string())
            }
        } else {
            let leader_id = data.leader_id.load(Ordering::SeqCst);

            debug!("Forwarding request to leader {}.", leader_id);

            let ip = data.dns_lookup.resolve_node(&data.node_tokens[leader_id]).map_err(|e| {
                debug!("Failed to resolve leader node: {}", e);
                Error::DNSLookupError(e)
            })?;

            let uri = format!("http://{}:80{}", ip, req.uri());
            trace!("Forwarding request to leader at URI: {}", uri);
            
            let client = Client::new();
            
            // Build the request to forward
            let mut request_builder = client.request(
                reqwest::Method::from_bytes(req.method().as_str().as_bytes())
                .unwrap_or(reqwest::Method::GET),
                uri
            );

            // Copyt the headers
            for (key, value) in req.headers().iter() {
                if let Ok(name) = key.as_str().parse::<reqwest::header::HeaderName>() {
                    if let Ok(val) = value.to_str() {
                        request_builder = request_builder.header(name, val);
                    }
                }
            }

            // Set the body
            request_builder = request_builder.body(bytes.to_vec());

            // Send the request and await the response (response timeout is 1 second - local network)
            let request = request_builder.timeout(Duration::from_secs(1)).send().await.map_err(Error::ForwardRequestError)?;
            let status = request.status();
            let body = request.text().await.map_err(Error::ForwardRequestError)?;

            trace!("Forwarded request to leader returned status: {}", status);
            Ok(body)
        }
    }

    /// Start serving HTTP requests
    pub async fn start_serving_http(&self) -> std::io::Result<()> {
        let shared = self.state.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(shared.clone())
                .route("/tick/{time}", web::get().to(VeritasController::<F>::tick))
                .route("/vote/{sender}", web::get().to(VeritasController::<F>::vote))
                .route("/force_set/{sender}/{key}", web::post().to(VeritasController::<F>::set_by_leader))
                .route("/peek/{key}", web::get().to(VeritasController::<F>::peek))
                .route("/get_eventual/{key}", web::get().to(VeritasController::<F>::get_eventual))
                .route("/get/{key}", web::get().to(VeritasController::<F>::handle_by_leader))
                .route("/set/{key}", web::post().to(VeritasController::<F>::handle_by_leader))
        });

        // Bind to all interfaces on port 80
        server.bind(("0.0.0.0", 80))?.run().await?;

        Ok(())
    }
}