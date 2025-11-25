use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError, http::{StatusCode, header::ContentType}, rt, web};
use general::{concurrent_file_key_value_store::{self, ConcurrentKeyValueStore}, dns::{self, DNSLookup}};
use regex::Regex;
use reqwest::Client;
use serde_json::Value;
use tokio::{task::JoinSet};
use tracing::{debug, error, info, trace, warn};
use std::{collections::HashMap, fmt::Display, sync::{Mutex, RwLock, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::Duration};
use lazy_regex::{Lazy, lazy_regex};
use actix_ws::{AggregatedMessage};

const VERITAS_TIME_KEY: &str = "VERITAS::TIME";
const CLUSTER_TIMEOUT_MS: u64 = 500;

#[derive(Debug)]
pub enum Error {
    FileKeyValueStoreError(concurrent_file_key_value_store::Error),
    QuorumNotReachedError,
    ForwardRequestError(reqwest::Error),
    DNSLookupError(std::io::Error),
    TransactionBeginError(concurrent_file_key_value_store::Error),
    TransactionCommitError(concurrent_file_key_value_store::Error),
    UnauthorizedSetError,
    LockPoisonedError,
    ParseIntError(std::num::ParseIntError),
    UnexpectedSettingValueError,
    WebSocketHandshakeError(actix_web::Error),
    JSONParseError(serde_json::Error),
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::plaintext())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::QuorumNotReachedError => StatusCode::INTERNAL_SERVER_ERROR,
            Error::UnauthorizedSetError => StatusCode::UNAUTHORIZED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileKeyValueStoreError(e) => write!(f, "FileKeyValueStoreError: File key-value store error: {}", e),
            Error::QuorumNotReachedError => write!(f, "QuorumNotReachedError: Quorum not reached"),
            Error::ForwardRequestError(e) => write!(f, "ForwardRequestError: Failed to forward request: {}", e),
            Error::LockPoisonedError => write!(f, "LockPoisonedError: Lock poisoned error"),
            Error::UnauthorizedSetError => write!(f, "UnauthorizedSetError: Unauthorized set request"),
            Error::TransactionBeginError(e) => write!(f, "TransactionBeginError: Failed to begin transaction: {}", e),
            Error::TransactionCommitError(e) => write!(f, "TransactionCommitError: Failed to commit transaction: {}", e),
            Error::DNSLookupError(e) => write!(f, "DNSLookupError: DNS lookup error: {}", e),
            Error::ParseIntError(e) => write!(f, "ParseIntError: Parse int error: {}", e),
            Error::UnexpectedSettingValueError => write!(f, "SettingValueError: Error setting value"),
            Error::WebSocketHandshakeError(e) => write!(f, "WebSocketHandshakeError: WebSocket handshake error: {}", e),
            Error::JSONParseError(e) => write!(f, "JSONParseError: JSON parse error: {}", e),
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

// Atomic counter for listener IDs
static LAST_ID: AtomicUsize = AtomicUsize::new(0);

// A listener gets the value of the new.
struct Listener {
    callback: Box<dyn Fn(usize, &str) + Send + Sync>,
    id: usize,
} 

impl Listener {
    fn new(callback: Box<dyn Fn(usize, &str) + Send + Sync>) -> Self {
        let id = LAST_ID.fetch_add(1, Ordering::SeqCst);

        Listener {
            callback,
            id,
        }
    }

    fn call(&self, new_value: &str) {
        (self.callback)(self.id, new_value);
    }
}

pub struct AppState<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    time: AtomicU64,
    kv_store: F,
    state: RwLock<State>,
    candidate_id: AtomicUsize,
    leader_id: AtomicUsize,
    id: usize,
    node_tokens: Vec<String>,
    dns_lookup: dns::DNSLookup,
    listeneres: Mutex<HashMap<String, Vec<Listener>>>,
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
        // Initialize time from kv_store or set to 0 if not present
        let time = kv_store.get(VERITAS_TIME_KEY);
        if let None = time {
            kv_store.set(VERITAS_TIME_KEY, "0").unwrap();
        }
        let time = kv_store.get(VERITAS_TIME_KEY).unwrap_or("0".to_string()).parse::<u64>().unwrap_or(0);
        VeritasController {
            state: web::Data::new(AppState {
                id,
                time: AtomicU64::new(time),
                kv_store: kv_store,
                state: RwLock::new(State::Initializing),
                candidate_id: AtomicUsize::new(id),
                leader_id: AtomicUsize::new(usize::MAX), // Use the last id as no leader - quorum unreachable
                node_tokens: node_tokens,
                dns_lookup: dns::DNSLookup::new(dns_ttl),
                listeneres: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Update the internal atomic time to be the maximum of the current time and the new_time
    /// 
    /// Parameters:
    /// - new_time: The new time to compare with the current time
    /// 
    /// Returns:
    /// - Result<u64, Error>: The updated time after the operation
    /// 
    /// # Examples
    /// ```ignore
    /// let updated_time = controller.update_time(42).unwrap();
    /// ```
    fn update_time(new_time: u64, time: &AtomicU64, kv_store: &dyn ConcurrentKeyValueStore) ->  Result<u64, Error> {
        {
            let mut transaction = kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;

            time.fetch_max(new_time, Ordering::SeqCst);

            let current = transaction.get(VERITAS_TIME_KEY).unwrap_or("0".to_string());
            let current = current.parse::<u64>().unwrap_or(0);
            
            // Only update the stored time if it is less than the current internal time - should never happen if implemented correctly
            // it is a safety check to prevent time regression in the kv_store
            if current < time.load(Ordering::SeqCst) {
                transaction.set(VERITAS_TIME_KEY, &time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;
            }

            transaction.commit().map_err(Error::TransactionCommitError)?;
        } // end transaction scope

        Ok(time.load(Ordering::SeqCst)) // Return the updated time - may be different to current and new_time
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
            let mut preferred_candidate_id: usize = self.state.id; // Start with self as candidate

            // Check if current leader is still available
            let mut current_leader_reachable: bool = false;
            let current_leader_id = self.state.leader_id.load(Ordering::SeqCst);

            // Check the health of other nodes
            for i in 0..self.state.node_tokens.len() {
                if i == self.state.id {
                    continue; // Skip self
                }

                let token: &String = &self.state.node_tokens[i];
                trace!("VeritasController {} checking node: {}", self.state.id, token);

                let (healthy, _) = self.check_node_health(i).await;

                if healthy {      
                    // Increment available nodes count
                    n_available_nodes += 1;

                    // Update the candidate id - elect the lowest id among available nodes
                    preferred_candidate_id = preferred_candidate_id.min(i);

                    // Remember if the current leader is reachable - if so we are loyal to it
                    if i == current_leader_id {
                        current_leader_reachable = true;
                    }
                }else{
                    trace!("VeritasController {} could not reach node: {}", self.state.id, token);
                }
            }

            trace!("VeritasController {} found {} available nodes.", self.state.id, n_available_nodes);

            // Update internal state based on available nodes
            self.update_state(n_available_nodes);

            // I can reach myself
            if current_leader_id == self.state.id {
                // I am the current leader - remain so
                current_leader_reachable = true;
            }

            // If current leader is reachable - prefer it as candidate - be loyal to it to avoid unnecessary leadership changes
            if current_leader_reachable {
                preferred_candidate_id = current_leader_id;
            } else {
                trace!("VeritasController {} could not reach current leader {}.", self.state.id, current_leader_id);
                
                // Failed to reach current leader - reset leader id
                self.state.leader_id.store(usize::MAX, Ordering::SeqCst); 
            }

            // Remember the candidate we wish to grant our vote to
            self.state.candidate_id.store(preferred_candidate_id, Ordering::SeqCst);

            if self.state.id == preferred_candidate_id {
                self.handle_election_campaign(period).await;
            } else {
                trace!("VeritasController {} recognizes {} as the current leader candidate.", self.state.id, preferred_candidate_id);
            }

            // Wait for the next tick period
            tokio::time::sleep(period).await;
        }
    }

    /// Update the internal state based on the number of available nodes
    ///     
    /// Parameters:
    /// - n_available_nodes: The number of nodes that are currently reachable
    /// 
    /// Updates the internal state to Running if quorum is reached, else QuorumNotReached
    /// 
    /// # Examples
    /// ```ignore
    /// controller.update_state(3);
    /// ```
    fn update_state(&self, n_available_nodes: usize) {
        // Update internal state based on available nodes
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

    /// Check the health of a single node by sending a tick and awaiting response
    async fn check_node_health(&self, node_index: usize) -> (bool, u64) {
        let token = &self.state.node_tokens[node_index];

        match self.state.dns_lookup.resolve_node(token) {
            Ok(ip) => {
                trace!("Resolved node '{}' to IP: {}", token, ip);

                let current_time = self.state.time.load(Ordering::SeqCst);
                // Tick the node and wait for response
                let (success, returned_time) = Self::send_tick_to_node(ip, current_time).await;
                if success {
                    trace!("Successfully ticked node '{}' at IP: {}. Returned time: {}", token, ip, returned_time);
                    
                    // Update internal time based on returned time - ensure we are not too far behind the leader
                    if let Err(e) = Self::update_time(returned_time+1, &self.state.time, &self.state.kv_store) {
                        // This is not critical - if we are NOT the elected leader- just log the error else panic as we must keep time updated
                        if self.state.id == self.state.leader_id.load(Ordering::SeqCst) {
                            panic!("VeritasController {} failed to update time after ticking node '{}': {}", self.state.id, token, e);
                        }else {
                            error!("VeritasController {} failed to update time after ticking node '{}': {}", self.state.id, token, e);
                        }
                    }
                    (true, returned_time)
                } else {
                    debug!("Failed to tick node '{}' at IP: {}", token, ip);
                    (false, 0)
                }
            }
            Err(e) => {
                trace!("Failed to resolve node '{}': {}", token, e);
                (false, 0)
            }
        }
    }

    /// Handle the campaingn to become the leader
    async fn handle_election_campaign(&self, period: Duration) {
        debug!("VeritasController {} is the current leader candidate.", self.state.id);
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
                    
                    // Failed to be elected as leader - lose the election
                    self.state.leader_id.store(usize::MAX, Ordering::SeqCst);
                    return;
                }

                info!("VeritasController {} has taken over as leader.", self.state.id);

                // I am now the leader
                self.state.leader_id.store(self.state.id, Ordering::SeqCst);
            }

            trace!("VeritasController {} broadcasting leadership to other nodes.", self.state.id);

            // Tell other nodes about the new leader
            Self::broadcast_and_collect_responses(
                self.state.id, 
                &format!("/election_won/{}", self.state.id), 
                &self.state.node_tokens, 
                &self.state.dns_lookup, 
                Duration::from_millis(CLUSTER_TIMEOUT_MS),
                reqwest::Method::GET,
                None
            ).await.join_all().await;
        } else {
            // Failed to be elected as leader - lose the election
            self.state.leader_id.store(usize::MAX, Ordering::SeqCst);
            warn!("VeritasController {} failed to be elected as leader, only received {} votes.", self.state.id, n_votes);
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
            Duration::from_millis(CLUSTER_TIMEOUT_MS),
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


        // Ignore the result - if it fails we assume the time is not critical
        let _ = Self::update_time(time + 1, &data.time, &data.kv_store);

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
            "true".to_string()
        } else {
            debug!("Denying vote to sender ID: {}", sender_id);
            "false".to_string()
        };

        vote
    }

    /// Handle the election won notification from other nodes.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the election won notification
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - String: Acknowledgment of the election won notification
    pub async fn election_won(req: HttpRequest, data: web::Data<AppState<F>>) -> String {
        let new_leader_id: usize = req.match_info().get("leader")
            .unwrap_or(usize::MAX.to_string().as_str())
            .parse()
            .unwrap_or(usize::MAX);

        trace!("Election won handler called. New leader ID: {}", new_leader_id);

        // Accept the election result
        data.leader_id.store(new_leader_id, Ordering::SeqCst);

        "True".to_string()
    }

    /// Peek into the key-value store to retrieve a value for a given key.
    /// 
    /// Parameters:
    /// - key: The key to look up in the key-value store
    /// - kv_store: The key-value store to query
    /// 
    /// Returns:
    /// - String: The value associated with the key, or an empty string if not found
    /// # Examples
    /// ```ignore
    /// let value = VeritasController::peek_into("my_key", &kv_store);
    /// ```
    pub fn peek(key: &str, kv_store: &dyn ConcurrentKeyValueStore) -> String {
        match kv_store.get(key) {
            Some(value) => value,
            None => "".to_string(),
        }
    }

    /// Handle the peek request to retrieve a value from the key-value store.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the peek requestq
    /// - data: Shared application state
    /// Returns:
    /// - String: The value associated with the requested key, or an empty string if not found
    pub async fn http_handle_peek(req: HttpRequest, data: web::Data<AppState<F>>) -> String {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        trace!("Peek handler called with key: {}", key);

        Self::peek(&key, &data.kv_store)
    }

    /// Retrieve a value for a given key from a quorum of nodes.
    /// 
    /// Parameters:
    /// - key: The key to look up in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value associated with the key if quorum is reached, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.get_eventual("my_key", data).await?;
    /// ```
    pub async fn get_eventual(key: &str, data: web::Data<AppState<F>>) -> Result<String, Error> {
        trace!("Get eventual called with key: {}", key);
        // Ask the quorum of nodes for the value
        let mut join_set = Self::broadcast_and_collect_responses(
            data.id, 
            &format!("/peek/{}", key), 
            &data.node_tokens, 
            &data.dns_lookup, 
            Duration::from_millis(CLUSTER_TIMEOUT_MS),
            reqwest::Method::GET,
            None
        ).await;

        let mut responses: Vec<String> = Vec::new();

        // Include self response
        if let Some(value) = data.kv_store.get(key) {
            trace!("Including local stored value {}: {}", key, value);
            responses.push(value);
        }

        // Wait for a quorum of responses
        while let Some(next) = join_set.join_next().await {
            if let Ok(res) = next {
                if let Some((_, text)) = res {
                    trace!("Received response: {}", text);
                    responses.push(text);
                    // Break if we have a quorum
                    if responses.len() * 2 > data.node_tokens.len() {
                        break;
                    }
                }
            }
        }

        // Check if we reached a quorum - if not, return an error
        if responses.len() * 2 <= data.node_tokens.len() {
            return Err(Error::QuorumNotReachedError);
        }

        // Process responses to find the most recent value
        let responses = responses.iter().map(|i| {
            let split_res = i.split_once(";");
            if let Some((t, v)) = split_res {
                let t = t.parse::<u64>().unwrap_or(0);
                trace!("Parsed response with time: {} and value: {}", t, v);
                (t, v.to_string())
            }else{
                (0, "".to_string())
            }
        });

        let newest = responses.max_by_key(|i|i.0).unwrap_or((0, "".to_string())).1;
        trace!("Most recent value for key {} is: {}", key, newest);
        Ok(newest)
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
    pub async fn http_handle_get_eventual(req: HttpRequest, data: web::Data<AppState<F>>) -> Result<String, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        trace!("Get eventual handler called with key: {}", key);
        Self::get_eventual(&key, data).await
    }

    /// Set a value for a given key in the key-value store if the new value is newer.
    /// 
    /// Parameters:
    /// - key: The key to set in the key-value store
    /// - value: The value to set in the key-value store
    /// - kv_store: The key-value store to update
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the value was set, otherwise false
    /// 
    /// # Examples
    /// ```ignore
    /// let success = VeritasController::set_if_newer("my_key", "42;my_value", &kv_store).unwrap();
    /// ```
    fn set_if_newer(key: &str, value: &str, kv_store: &dyn ConcurrentKeyValueStore) -> Result<bool, Error> {
        let (time_str, _) = value.split_once(";").unwrap_or(("0", ""));
        let new_time = time_str.parse::<u64>().map_err(Error::ParseIntError)?;

        {
            let mut transaction = kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;

            let current_value = transaction.get(key);
            if let Some(current_value) = current_value {
                let (current_time_str, _) = current_value.split_once(";").unwrap_or(("0", ""));
                let current_time = current_time_str.parse::<u64>().map_err(Error::ParseIntError)?;

                if new_time <= current_time {
                    warn!("Not updating key: {} as current time: {} is newer than new time: {}", key, current_time, new_time);
                    return Ok(false); // Do not update if the new time is not newer
                }
            }

            transaction.set(key, value).map_err(Error::FileKeyValueStoreError)?;
            transaction.commit().map_err(Error::TransactionCommitError)?;
        }

        Ok(true)
    }
    
    /// Handle the set request to store a value in the key-value store from the leader.
    /// 
    /// Parameters:
    /// - key: The key to set in the key-value store
    /// - value: The value to set in the key-value store
    /// - sender_id: The ID of the sender node
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.set_by_leader("my_key", "my_value", &sender_id, data).await?;
    /// ```
    pub async fn set_by_leader(key: &str, time_value: &str, sender_id: &usize, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        let (time, value) = time_value.split_once(";").unwrap_or(("0", ""));

        trace!("Parsed value: {} with time: {} from leader ID: {}", value, time, sender_id);

        let new_time = time.parse::<u64>().map_err(Error::ParseIntError)?;

        // Update internal time
        Self::update_time(new_time+1, &data.time, &data.kv_store)?;

        // Set the time;value in the local key-value store to be fetched later
        Self::set_if_newer(key, time_value, &data.kv_store)?;

        // Before the transaction ends, notify the listeners of the change
        Self::call_listeners(key, &value, data.clone());

        trace!("Set key: {} to value: {} from leader ID: {}", key, value, sender_id);

        Ok(true)
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
    /// let response = controller.http_handle_set_by_leader(req, data, bytes).await?;
    /// ```
    async fn http_handle_set_by_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<(), Error> {
        // We only accept set requests from the current leader node
        // Others must be rejected and sent to the correct leader by the caller
        let sender: String = req.match_info().get("sender")
            .unwrap_or("")
            .to_string();

        let sender_id = sender.parse::<usize>().unwrap_or(usize::MAX);
        trace!("Set by leader handler called from sender ID: {}", sender_id);
        if sender_id != data.leader_id.load(Ordering::SeqCst) {
            warn!("Received set request from non-leader sender ID: {}. Current leader ID: {}", sender_id, data.leader_id.load(Ordering::SeqCst));
            return Err(Error::UnauthorizedSetError);
        }
        
        // Process the set request
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        debug!("Set handler called by Leader {} with key: {} and value: {}", sender_id, key, value);

        let res = Self::set_by_leader(&key, &value, &sender_id, data).await?;
        if !res {
            warn!("Failed to set key: {} to value: {} from leader ID: {}", key, value, sender_id);
            return Err(Error::UnexpectedSettingValueError);
        }
        Ok(())
    }

    /// Broadcast the set request to all follower nodes and ensure quorum is reached.
    ///
    /// Parameters:
    /// - key: The key to set in the key-value store
    /// - time_value: The value to set in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.broadcast_set_to_followers("my_key", "my_value", data).await?;
    /// ```
    async fn broadcast_set_to_followers(key: &str, time_value: &str, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        trace!("Broadcasting set request for key: {} with value: {} to followers.", key, time_value);

        let mut rsp_set = Self::broadcast_and_collect_responses(
            data.id, 
            &format!("/force_set/{}/{}", data.id, key), 
            &data.node_tokens, 
            &data.dns_lookup, 
            Duration::from_millis(CLUSTER_TIMEOUT_MS),
            reqwest::Method::PUT,
            Some(time_value.to_string())
        ).await;

        let mut n_successful_sets: usize = 1; // Count self as successful

        // Wait for responses and count successful sets until quorum is reached
        while let Some(next) = rsp_set.join_next().await && 
        n_successful_sets * 2 <= data.node_tokens.len() {
            if let Ok(res) = next {
                if let Some((_, _)) = res {
                    n_successful_sets += 1;
                }
            }
        }

        trace!("Received {} successful set responses for key: {}.", n_successful_sets, key);

        // Check if we reached a quorum - if not, return an error
        if n_successful_sets * 2 <= data.node_tokens.len() {
            return Err(Error::QuorumNotReachedError);
        }

        Ok(true)
    }

    /// Handle the set request when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to set in the key-value store
    /// - time_value: The value to set in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.handle_set_for_leader("my_key", "my_value", data).await?;
    /// ```
    pub async fn set_for_leader(key: &str, time_value: &str, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        debug!("Set handler called with key: {} and value: {}", key, time_value);

        let mut transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
        trace!("Beginning transaction for set operation on key: {}", key);
        
        // Prepend the current time to the value for versioning
        let value = format!("{};{}", data.time.fetch_add(1, Ordering::SeqCst), time_value);

        trace!("Setting key: {} to value: {} in local store.", key, value);

        // Set the value in the local key-value store first do gurantee linearizability when reading from leader.
        transaction.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

        trace!("Updated key: {} to value: {} in local store.", key, value);

        // Update the stored time in the kv_store as well to persist the time and prevent regression on restart
        transaction.set(VERITAS_TIME_KEY, &data.time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;

        trace!("Updated VERITAS_TIME_KEY to value: {} in local store.", data.time.load(Ordering::SeqCst).to_string());

        trace!("Committing transaction for set operation on key: {}", key);

        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false{
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(false); // Only commit the new value if the set was possible 
        }

        // Before the transaction ends, notify the listeners of the change
        // The websocket does gurantee total order of messages so lineraliziability is preserved
        Self::call_listeners(key, &value, data.clone());

        // Commit the transaction
        transaction.commit().map_err(Error::TransactionCommitError)?;

        Ok(true)
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
    /// let response = controller.http_handle_set_for_leader(req, data, bytes).await?;
    /// ```
    async fn http_handle_set_for_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<bool, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        Self::set_for_leader(&key, &value, data).await
    }

    /// Handle the append request when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to append in the key-value store
    /// - value: The value to append in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.handle_append_for_leader("my_key", "my_value", data).await?;
    /// ```
    async fn append_for_leader(key: &str, value: &str, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        debug!("Append handler called with key: {} and value: {}", key, value);

        let mut transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
        trace!("Beginning transaction for append operation on key: {}", key);
        
        // Get the current value
        let current_value = transaction.get(key).unwrap_or("".to_string());

        // split of the time prefix
        let current_value = 
        if let Some((_, v)) = current_value.split_once(";") {
            v.to_string()
        } else {
            current_value
        };

        // Append the new value to the current value
        let new_value = if current_value.is_empty() {
            value.to_string()
        } else {
            format!("{}{}", current_value, value)
        };

        trace!("Appending value: {} to key: {} in local store. New value: {}", value, key, new_value);

        // Add the time prefix
        let value = format!("{};{}", data.time.fetch_add(1, Ordering::SeqCst), new_value);

        // Set the new appended value in the local key-value store first to guarantee linearizability when reading from leader.
        transaction.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

        trace!("Updated key: {} to new appended value: {} in local store.", key, value);

        // Update the stored time in the kv_store as well to persist the time and prevent regression on restart
        transaction.set(VERITAS_TIME_KEY, &data.time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;

        trace!("Committing transaction for append operation on key: {}", key);

        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false{
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(false); // Only commit the new value if the set was possible 
        }

        // Before the transaction ends, notify the listeners of the change
        Self::call_listeners(key, &value, data.clone());

        // Commit the transaction
        transaction.commit().map_err(Error::TransactionCommitError)?;

        Ok(true)
    }

    /// Append a value for a given key in the key-value store when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to append in the key-value store
    /// - value: The value to append in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.handle_append_for_leader("my_key", "my_value", data).await?;
    /// ```
    async fn http_handle_append_for_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<bool, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        let value = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        Self::append_for_leader(&key, &value, data).await
    }


    /// Handle a replace request for a given key when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to replace in the key-value store
    /// - old_value: The old value to be replaced
    /// - new_value: The new value to set
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.replace_for_leader("my_key", "old_value", "new_value", data).await?;
    /// ```
    async fn replace_for_leader(key: &str, old_value: &str, new_value: &str, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        debug!("Replace handler called with key: {}, old_value: {}, new_value: {}", key, old_value, new_value);

        let mut transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
        trace!("Beginning transaction for replace operation on key: {}", key);
        
        // Get the current value
        let current_value = transaction.get(key).unwrap_or("".to_string());

        // split of the time prefix
        let current_value = 
        if let Some((_, v)) = current_value.split_once(";") {
            v.to_string()
        } else {
            current_value
        };

        // Replace the given old_value with new_value in the current value
        let new_value = current_value.replace(old_value, new_value);
        
        trace!("Replacing value: {} to key: {} in local store. New value: {}", old_value, key, new_value);

        // Add the time prefix
        let value: String = format!("{};{}", data.time.fetch_add(1, Ordering::SeqCst), new_value);

        // Set the new appended value in the local key-value store first to guarantee linearizability when reading from leader.
        transaction.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

        trace!("Updated key: {} to new replaced value: {} in local store.", key, value);

        // Update the stored time in the kv_store as well to persist the time and prevent regression on restart
        transaction.set(VERITAS_TIME_KEY, &data.time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;

        trace!("Committing transaction for replace operation on key: {}", key);

        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false {
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(false); // Only commit the new value if the set was possible 
        }

        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false {
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(false); // Only commit the new value if the set was possible 
        }

        // Before the transaction ends, notify the listeners of the change
        Self::call_listeners(key, &value, data.clone());

        // Commit the transaction
        transaction.commit().map_err(Error::TransactionCommitError)?;

        Ok(true)
    }

    /// Handle the replace request when this node is the leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the replace request
    /// - data: Shared application state
    /// - bytes: The body of the incoming request
    ///
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// # Examples
    /// 
    /// ```ignore
    /// let success = controller.http_handle_replace_for_leader(req, data, bytes).await?;
    /// ```
    async fn http_handle_replace_for_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<bool, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        // The body contains len;<old_value><new_value>
        let body = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        // Parse the length, old_value and new_value
        let (length, old_new_value) = body.split_once(";").unwrap_or(("0", ""));
        let len = length.parse::<usize>().unwrap_or(0);

        // Split old_new_value into old_value and new_value
        let (old_value, new_value) = old_new_value.split_at(len);

        Self::replace_for_leader(&key, old_value, new_value, data).await
    }

    /// Handle a compare-and-set request for a given key when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to compare and set in the key-value store
    /// - expected_value: The expected current value
    /// - new_value: The new value to set if the expected value matches
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise false or an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.compare_and_set_for_leader("my_key", "expected_value", "new_value", data).await?;
    /// ```
    async fn compare_and_set_for_leader(key: &str, expected_value: &str, new_value: &str, data: web::Data<AppState<F>>) -> Result<bool, Error> {
        debug!("Compare-and-set handler called with key: {}, expected_value: {}, new_value: {}", key, expected_value, new_value);

        // We have to do a read-modify-write cycle here - the transaction ensures atomicity

        let mut transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
        trace!("Beginning transaction for compare-and-set operation on key: {}", key);
        
        // Get the current value
        let current_value = transaction.get(key).unwrap_or("".to_string());

        // split of the time prefix
        let current_value = 
        if let Some((_, v)) = current_value.split_once(";") {
            v.to_string()
        } else {
            current_value
        };

        // Check if the current value matches the expected value
        if current_value != expected_value {
            warn!("Current value: {} does not match expected value: {} for key: {}", current_value, expected_value, key);
            return Ok(false); // Do not update if the current value does not match the expected value
            // Transaction ends with the life of transaction variable
        }

        // Add the time prefix
        let value = format!("{};{}", data.time.fetch_add(1, Ordering::SeqCst), new_value);

        // Set the new value in the local key-value store first to guarantee linearizability when reading from leader.
        transaction.set(&key, &value).map_err(Error::FileKeyValueStoreError)?;

        trace!("Updated key: {} to new value: {} in local store.", key, value);

        // Update the stored time in the kv_store as well to persist the time and prevent regression on restart
        transaction.set(VERITAS_TIME_KEY, &data.time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;

        trace!("Committing transaction for compare-and-set operation on key: {}", key);
        
        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false {
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(false); // Only commit the new value if the set was possible 
        }

        // Before the transaction ends, notify the listeners of the change
        Self::call_listeners(key, &value, data.clone());

        // Commit the transaction
        transaction.commit()?;

        Ok(true)
    }


    /// Handle a compare-and-set request for a given key when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to compare and set in the key-value store
    /// - expected_value: The expected current value
    /// - new_value: The new value to set if the expected value matches
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<bool, Error>: True if the operation was successful, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let success = controller.compare_and_set_for_leader("my_key", "expected_value", "new_value", data).await?;
    /// ```
    async fn http_handle_compare_set_for_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<bool, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        // The body contains len;<expected_value><new_value>
        let body = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

        // Parse the length, expected_value and new_value
        let (length, expected_new_value) = body.split_once(";").unwrap_or(("0", ""));
        let len = length.parse::<usize>().unwrap_or(0);

        // Split expected_new_value into expected_value and new_value
        let (expected_value, new_value) = expected_new_value.split_at(len);

        Self::compare_and_set_for_leader(&key, expected_value, new_value, data).await
    }

    /// Handle the get-and-add request when this node is the leader.
    /// Increments the integer value associated with the key by 1 and returns the old value.
    /// 
    /// Parameters:
    /// - key: The key to look up in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value before the increment operation
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.get_add_for_leader("my_key", data).await?;
    /// ```
    async fn get_add_for_leader(key: &str, data: web::Data<AppState<F>>) -> Result<String, Error> {
        debug!("Get-and-add handler called with key: {}", key);

        // Ensure we have the latest value before incrementing
        // No need to do this inside the transaction as we just need to ensure we have the latest value
        // before we do the read-modify-write cycle. As the leader any concurrent writes will be serialized
        let current_value = Self::get_for_leader(key, data.clone()).await?;
        Self::set_if_newer(key, &current_value, &data.kv_store)?;

        let mut transaction = data.kv_store.begin_transaction().map_err(Error::TransactionBeginError)?;
        trace!("Beginning transaction for get-and-add operation on key: {}", key);

        // Get the current value
        let current_value = transaction.get(key).unwrap_or("0".to_string());

        // remove any time prefix if present
        let current_value = 
        if let Some((_, v)) = current_value.split_once(";") {
            v.to_string()
        } else {
            current_value
        };

        let old_value = current_value.parse::<i64>().map_err(Error::ParseIntError)?;

        // Increment the value
        let new_value = old_value + 1;

        trace!("Incrementing value: {} to key: {} in local store. New value: {}", old_value, key, new_value);
        // Add the time prefix
        let value = format!("{};{}", data.time.fetch_add(1, Ordering::SeqCst), new_value);

        // Set the new value in the local key-value store first to guarantee linearizability when reading from leader.
        transaction.set(key, &value).map_err(Error::FileKeyValueStoreError)?;

        // Update the stored time in the kv_store as well to persist the time and prevent regression on restart
        transaction.set(VERITAS_TIME_KEY, &data.time.load(Ordering::SeqCst).to_string()).map_err(Error::FileKeyValueStoreError)?;
        
        let res = Self::broadcast_set_to_followers(key, &value, data.clone()).await?;
        if  res == false {
            warn!("Failed to broadcast set request for key: {} to followers.", key);
            return Ok(old_value.to_string()); // Only commit the new value if the set was possible 
        }

        // Before the transaction ends, notify the listeners of the change
        Self::call_listeners(key, &value, data.clone());

        trace!("Committing transaction for get-and-add operation on key: {}", key);
        transaction.commit()?;


        Ok(old_value.to_string())
    }

    /// Handle the get-and-add request when this node is the leader.
    /// 
    /// Parameters:
    /// - req: HttpRequest containing the get-and-add request
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value associated with the requested key
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.http_handle_get_add_for_leader(req, data).await?;
    /// ```
    async fn http_handle_get_add_for_leader(req: HttpRequest, data: web::Data<AppState<F>>) -> Result<String, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        debug!("Get-and-add handler called with key: {}", key);

        Self::get_add_for_leader(&key, data).await
    }


    /// Retrieve a value for a given key from a quorum of nodes when this node is the leader.
    /// 
    /// Parameters:
    /// - key: The key to look up in the key-value store
    /// - data: Shared application state
    /// 
    /// Returns:
    /// - Result<String, Error>: The value associated with the key if quorum is reached, otherwise an error
    /// 
    /// # Examples
    /// ```ignore
    /// let value = controller.get_for_leader("my_key", data).await?;
    /// ```
    pub async fn get_for_leader(key: &str, data: web::Data<AppState<F>>) -> Result<String, Error> {
        trace!("Get handler called with key: {}", key);

        // Ask the quorum of nodes for the value
        let mut join_set = Self::broadcast_and_collect_responses(
            data.id, 
            &format!("/peek/{}", key), 
            &data.node_tokens, 
            &data.dns_lookup, 
            Duration::from_millis(CLUSTER_TIMEOUT_MS),
            reqwest::Method::GET,
            None
        ).await;

        let mut responses: Vec<String> = Vec::new();

        // Include self response
        if let Some(value) = data.kv_store.get(&key) {
            trace!("Including local stored value {}: {}", key, value);
            responses.push(value);
        }

        // Wait for a quorum of responses
        while let Some(next) = join_set.join_next().await {
            if let Ok(res) = next {
                if let Some((_, text)) = res {
                    trace!("Received response: {}", text);
                    responses.push(text);
                    // Break if we have a quorum
                    if responses.len() * 2 > data.node_tokens.len() {
                        break;
                    }
                }
            }
        }

        // Check if we reached a quorum - if not, return an error
        if responses.len() * 2 <= data.node_tokens.len() {
            return Err(Error::QuorumNotReachedError);
        }

        // Process responses to find the most recent value
        let responses = responses.iter().map(|i| {
            let split_res = i.split_once(";");
            if let Some((t, v)) = split_res {
                let t = t.parse::<u64>().unwrap_or(0);
                trace!("Parsed response with time: {} and value: {}", t, v);
                (t, v.to_string())
            }else{
                (0, "".to_string())
            }
        }).collect::<Vec<(u64, String)>>();

        let newest = responses.iter().max_by_key(|i|i.0).unwrap_or(&(0, "".to_string())).1.clone();
        trace!("Most recent value for key {} is: {}", key, newest);

        // AUTOHEAL: A quorum of nodes must agree on the value before returning it to keep linearizability guarantees
        
        // check if there are n/2 + 1 nodes that have the newest value
        let mut n_agree = 0;
        for response in responses {
            if response.1 == newest {
                n_agree += 1;
            }
        }

        // If not enough nodes agree, try to heal the cluster by setting the newest value to all nodes
        if n_agree * 2 <= data.node_tokens.len() {
            warn!("Not enough nodes agree on the newest value for key: {}. Healing the cluster by setting the newest value to all nodes.", key);
            let result = Self::set_for_leader(key, &newest, data).await?;
            if !result {
                warn!("Failed to heal the cluster for key: {}", key);
                return Err(Error::QuorumNotReachedError);
            }
        }

        Ok(newest)
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
    /// let value = controller.http_handle_get_for_leader(req, data).await?;
    /// ```
    async fn http_handle_get_for_leader(req: HttpRequest, data: web::Data<AppState<F>>) -> Result<String, Error> {
        let key: String = req.match_info().get("key")
            .unwrap_or("")
            .to_string();

        Self::get_for_leader(&key, data).await
    }

    /// Map Actix HTTP method to Reqwest HTTP method
    /// 
    /// Parameters:
    /// - actix_method: Reference to Actix HTTP method
    /// 
    /// Returns:
    /// - reqwest::Method: Corresponding Reqwest HTTP method
    fn map_method(actix_method: &actix_web::http::Method) -> reqwest::Method {
        match *actix_method {
            actix_web::http::Method::GET => reqwest::Method::GET,
            actix_web::http::Method::POST => reqwest::Method::POST,
            actix_web::http::Method::PUT => reqwest::Method::PUT,
            actix_web::http::Method::DELETE => reqwest::Method::DELETE,
            actix_web::http::Method::HEAD => reqwest::Method::HEAD,
            actix_web::http::Method::OPTIONS => reqwest::Method::OPTIONS,
            actix_web::http::Method::PATCH => reqwest::Method::PATCH,
            _ => reqwest::Method::GET, // Default to GET for unsupported methods
        }
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
    pub async fn handle_by_leader(req: HttpRequest, data: web::Data<AppState<F>>, bytes: web::Bytes) -> Result<impl Responder, impl ResponseError> {
        // Define regex patterns for GET and SET paths
        // Compile these regexes only once for efficiency
        static GET_REGEX: Lazy<Regex> = lazy_regex!(r"^\/get\/([^\/]+)$");
        static SET_REGEX: Lazy<Regex> = lazy_regex!(r"^\/set\/([^\/]+)$");
        static APPEND_REGEX: Lazy<Regex> = lazy_regex!(r"^\/append\/([^\/]+)$");
        static REPLACE_REGEX: Lazy<Regex> = lazy_regex!(r"^\/replace\/([^\/]+)$");
        static COMPARE_SET_REGEX: Lazy<Regex> = lazy_regex!(r"^\/compare_set\/([^\/]+)$");
        static GET_ADD_REGEX: Lazy<Regex> = lazy_regex!(r"^\/get_add\/([^\/]+)$");

        let state = *(&data).state.read().map_err(|_|Error::LockPoisonedError)?;
        
        if state == State::Running && data.leader_id.load(Ordering::SeqCst) == data.id {
            // Hey, it's me, the leader
            trace!("Handling request as leader. {}", req.path());
            
            // Handle the request locally - as we are the leader
            if GET_REGEX.is_match(req.path()) {
                trace!("Handling GET request as leader. {}", req.path());
                return Self::http_handle_get_for_leader(req, data).await;
            } else if SET_REGEX.is_match(req.path()) {
                trace!("Handling SET request as leader. {}", req.path());
                return Self::http_handle_set_for_leader(req, data, bytes).await
                    .map(|r| if r { "True" } else { "False" }.to_string());
            }else if APPEND_REGEX.is_match(req.path()) {
                trace!("Handling APPEND request as leader. {}", req.path());
                // Set the new appended value
                return Self::http_handle_append_for_leader(req, data, bytes).await
                    .map(|r| if r { "True" } else { "False" }.to_string());
            } else if REPLACE_REGEX.is_match(req.path()) {
                trace!("Handling REPLACE request as leader. {}", req.path());
                // Set the new replaced value
                return Self::http_handle_replace_for_leader(req, data, bytes).await
                    .map(|r| if r { "True" } else { "False" }.to_string());
            } else if COMPARE_SET_REGEX.is_match(req.path()) {
                trace!("Handling COMPARE_SET request as leader. {}", req.path());
                return Self::http_handle_compare_set_for_leader(req, data, bytes).await
                    .map(|r| if r { "True" } else { "False" }.to_string());
            } else if GET_ADD_REGEX.is_match(req.path()) {
                trace!("Handling GET_ADD request as leader. {}", req.path());
                return Self::http_handle_get_add_for_leader(req, data).await;
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
            
            // Map Actix method to Reqwest method
            let method = Self::map_method(req.method());

            // Build the request to forward
            let mut request_builder = client.request(
                method,
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

            // Send the request and await the response (response timeout is 3 second - local network)
            let request = request_builder
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .map_err(Error::ForwardRequestError)?;

            let status = request.status();
            let body = request.text().await.map_err(Error::ForwardRequestError)?;

            trace!("Forwarded request to leader returned status: {}", status);
            Ok(body)
        }
    }

    /// Add a listener for a given key in the application state.
    ///     
    /// Parameters:
    /// - key: The key to listen for changes
    /// - listener: The listener callback to be invoked on changes
    fn add_listener(key: String, listener: Listener, data: web::Data<AppState<F>>) {
        let mut listeners = data.listeneres.lock().unwrap();

        if listeners.contains_key(&key) {
            let at = listeners.get_mut(&key);
            if let Some(at) = at {
                at.push(listener);
                return;
            }
        }

        // If there does not exist a vector for the given key, create one with the new listener.
        listeners.insert(key, vec![listener]);
    }

    fn remove_listener(key: &str, listener: usize, data: web::Data<AppState<F>>) {
        let mut listeners = data.listeneres.lock().unwrap();

        if listeners.contains_key(key) {
            let at = listeners.get_mut(key);
            if let Some(at) = at {
                // Remove the listener by its id
                at.retain(|l| l.id != listener);
            }
        }
    }

    /// Call all listeners for a given key with the new value.
    ///     
    /// Parameters:
    /// - key: The key whose listeners should be called
    /// - new_value: The new value to pass to the listeners
    /// - data: Shared application state    
    fn call_listeners(key: &str, new_value: &str, data: web::Data<AppState<F>>) {
        let listeners = data.listeneres.lock().unwrap();

        if listeners.contains_key(key) {
            let at = listeners.get(key);
            if let Some(at) = at {
                for listener in at {
                    listener.call(new_value);
                }
            }
        }
    }

    async fn websocket(req: HttpRequest, stream: web::Payload, data: web::Data<AppState<F>>) -> Result<HttpResponse, Error> {
        let (res, mut session, stream) = actix_ws::handle(&req, stream)
            .map_err(Error::WebSocketHandshakeError)?;
        
        let mut stream = stream
            .aggregate_continuations()
            // aggregate continuation frames up to 1MiB
            .max_continuation_size(2_usize.pow(20));

        
        // start task but don't wait for it
        rt::spawn(async move {
            // receive messages from websocket
            while let Some(msg) = stream.recv().await {
                match msg {
                    Ok(AggregatedMessage::Text(text)) => {
                        let text = text.trim();
                        
                        // Expecting a Json in the following format:
                        /*
                        {
                            "command": "get",
                            "parameters": ["key"]
                        }

                        {
                            "command": "set",
                            "parameters": ["key", "value"]
                        }
                        
                        */
                        let parsed: Result<Value, Error> = serde_json::from_str(text).map_err(Error::JSONParseError);
                        
                        if let Err(e) = parsed {
                            warn!("Failed to parse JSON from websocket message: {}", e);
                            session.text("Error: Invalid JSON format").await.unwrap();
                            continue;
                        }

                        let parsed = parsed.unwrap();
                        let command = parsed["command"].as_str().unwrap_or("");
                        let parameters: Vec<String> = parsed["parameters"].as_array().unwrap_or(&vec![])
                            .iter()
                            .map(|v| v.as_str()
                            .unwrap_or("")
                            .to_string())
                            .collect();

                        if command == "watch"{
                            let key = parameters.get(0);
                            if key.is_none() {
                                session.text("Error: Missing key parameter for watch command").await.unwrap();
                                continue;   
                            }

                            let key  = key.unwrap();
                            let key_for_listener = key.to_string();
                            debug!("WebSocket watch command received for key: {}", key);

                            let session_for_listener = session.clone();
                            let data_for_listener = data.clone();

                            let listener = Listener::new(Box::new(move |id: usize, new_value: &str| {
                                let key = key_for_listener.clone();
                                let new_value = new_value.to_string();

                                let mut session = session_for_listener.clone();
                                let data = data_for_listener.clone();
                                rt::spawn(async move {
                                    let msg = format!("{{\"command\":\"update\",\"key\":\"{}\",\"new_value\":\"{}\"}}", key, new_value);
                                    let res = session.text(msg).await;

                                    if let Err(_) = res {
                                        debug!("WebSocket session closed, removing listener for key: {}", key);
                                        Self::remove_listener(&key, id, data);
                                    }
                                });
                            }));


                            Self::add_listener(key.clone(), listener, data.clone());
                        }   
                    }

                    Ok(AggregatedMessage::Binary(bin)) => {
                        // echo binary message
                        session.binary(bin).await.unwrap();
                    }

                    Ok(AggregatedMessage::Ping(msg)) => {
                        // respond to PING frame with PONG frame
                        session.pong(&msg).await.unwrap();
                    }

                    _ => {}
                }
            }
        });

        Ok(res)
    }

    /// Start serving HTTP requests
    pub async fn start_serving_http(&self) -> std::io::Result<()> {
        let shared = self.state.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(shared.clone())
                .route("/tick/{time}", web::get().to(VeritasController::<F>::tick))
                .route("/vote/{sender}", web::get().to(VeritasController::<F>::vote))
                .route("/election_won/{leader}", web::get().to(VeritasController::<F>::election_won))
                .route("/force_set/{sender}/{key}", web::put().to(VeritasController::<F>::http_handle_set_by_leader))
                .route("/peek/{key}", web::get().to(VeritasController::<F>::http_handle_peek))
                .route("/get_eventual/{key}", web::get().to(VeritasController::<F>::http_handle_get_eventual))
                // These requests must be handled by the leader to ensure linearizability
                .route("/get/{key}", web::get().to(VeritasController::<F>::handle_by_leader))
                .route("/set/{key}", web::put().to(VeritasController::<F>::handle_by_leader))
                .route("/append/{key}", web::put().to(VeritasController::<F>::handle_by_leader))
                .route("/replace/{key}", web::put().to(VeritasController::<F>::handle_by_leader))
                .route("/compare_set/{key}", web::put().to(VeritasController::<F>::handle_by_leader))
                .route("/get_add/{key}", web::get().to(VeritasController::<F>::handle_by_leader))
                .route("/ws", web::get().to(VeritasController::<F>::websocket))
        });

        // Bind to all interfaces on port 80
        server.bind(("0.0.0.0", 80))?.run().await?;

        Ok(())
    }
}