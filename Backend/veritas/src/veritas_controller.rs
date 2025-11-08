use actix_web::{App, HttpRequest, HttpServer, cookie::time::format_description::modifier::Period, web};
use general::{concurrent_file_key_value_store::ConcurrentKeyValueStore, dns::{self, DNSLookup}, file_key_value_store};
use tokio::task::JoinSet;
use tracing::{debug, info, trace, warn};
use std::{fmt::Display, ops::Add, path, sync::{RwLock, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::Duration};

#[derive(Debug)]
pub enum Error {
    FileKeyValueStoreError(file_key_value_store::Error),
    LockPoisoned,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileKeyValueStoreError(e) => write!(f, "File key-value store error: {}", e),
            Error::LockPoisoned => write!(f, "Lock poisoned"),
        }
    }
}

impl From<file_key_value_store::Error> for Error {
    fn from(e: file_key_value_store::Error) -> Self {
        Error::FileKeyValueStoreError(e)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Initializing,
    Running,
    QuorumNotReached,
    Stopped,
}

pub struct AppState<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    time: AtomicU64,
    kv_store: F,
    state: RwLock<State>,
    candidate_id: AtomicUsize,
    leader_id: AtomicUsize,
}

pub struct VeritasController<F> where F: ConcurrentKeyValueStore + Clone + Send + Sync + 'static {
    id: usize,
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
    pub fn new(id: usize, kv_store: F) -> Self {
        VeritasController {
            id,
            state: web::Data::new(AppState {
                time: AtomicU64::new(0),
                kv_store: kv_store,
                state: RwLock::new(State::Initializing),
                candidate_id: AtomicUsize::new(id),
                leader_id: AtomicUsize::new(id),
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
    pub async fn start_ticking(&self, node_tokens: Vec<String>, period: std::time::Duration, dns_ttl: Duration) {
        // Placeholder for ticking logic
        trace!("VeritasController {} started ticking.", self.id);

        let dns_lookup = dns::DNSLookup::new(dns_ttl);

        loop {
            let mut n_available_nodes: usize = 1; // Count self as available
            let mut candidate_id: usize = self.id; // Start with self as candidate

            // Check the health of other nodes
            for i in 0..node_tokens.len() {
                if i == self.id {
                    continue; // Skip self
                }

                let token = &node_tokens[i];
                trace!("VeritasController {} checking node: {}", self.id, token);

                match dns_lookup.resolve_node(token) {
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
                            candidate_id = candidate_id.min(i);

                        } else {
                            debug!("Failed to tick node '{}' at IP: {}", token, ip);
                        }
                    }
                    Err(e) => {
                        trace!("Failed to resolve node '{}': {}", token, e);
                    }
                }
            }

            trace!("VeritasController {} found {} available nodes.", self.id, n_available_nodes);

            // Update internal state based on available nodes
            {
                let mut state_guard = self.state.state.write().unwrap();
                if n_available_nodes * 2 > node_tokens.len() {
                    // Quorum reached
                    if *state_guard != State::Running {
                        trace!("VeritasController {} transitioning to Running state.", self.id);
                    }
                    *state_guard = State::Running;
                } else {
                    // Quorum not reached
                    if *state_guard != State::QuorumNotReached {
                        warn!("VeritasController {} transitioning to QuorumNotReached state.", self.id);
                    } 
                    *state_guard = State::QuorumNotReached;
                }
            }

            // Update the candidate_id
            self.state.candidate_id.store(candidate_id, Ordering::SeqCst);

            if self.id == candidate_id {
                info!("VeritasController {} is the current leader candidate.", self.id);
                // Ask other nodes to elect me as leader
                let n_votes = self.ask_for_votes(dns_lookup.clone()).await + 1; // Count self vote
                trace!("VeritasController {} received {} votes.", self.id, n_votes);
                if n_votes * 2 > node_tokens.len() {
                    trace!("VeritasController {} has been elected as leader with {} votes.", self.id, n_votes);

                    if self.state.leader_id.load(Ordering::SeqCst) != self.id {
                        info!("VeritasController {} is now the new leader. Wait out the current period - for current leader to step down.", self.id);
                        tokio::time::sleep(period).await;

                        // After wainting for the old leader to step down - ask for votes again to make sure we are still the leader.
                        let n_votes = self.ask_for_votes(dns_lookup.clone()).await + 1;
                        if n_votes * 2 <= node_tokens.len() {
                            warn!("VeritasController {} failed to be elected as leader after waiting for old leader to step down, only received {} votes.", self.id, n_votes);
                            continue;
                        }
                    }

                    // I am now the leader
                    self.state.leader_id.store(self.id, Ordering::SeqCst);
                    info!("VeritasController {} has taken over as leader.", self.id);
                } else {
                    warn!("VeritasController {} failed to be elected as leader, only received {} votes.", self.id, n_votes);
                }
            } else {
                trace!("VeritasController {} recognizes {} as the current leader candidate.", self.id, candidate_id);
            }

            // Tick the other nodes
            trace!("VeritasController {} tick.", self.id);
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
    /// 
    /// Returns:
    /// - Vec<Option<String>>: A vector of optional responses from each node
    /// 
    /// # Examples
    /// ```ignore
    /// let responses = controller.broadcast_and_collect_responses("/status", node_tokens, dns_lookup).await;
    /// ```
    async fn broadcast_and_collect_responses(&self, path: &str, node_tokens: Vec<String>, dns_lookup: DNSLookup) -> Vec<Option<String>> {
        // Initialize a vector to hold responses from each node
        let mut responses:Vec<Option<String>> = Vec::new();
        responses.reserve(node_tokens.len());
        for (i, _) in node_tokens.iter().enumerate() {
            if i == self.id {
                // Skip self
                responses.push(None);
                continue;
            }
        }

        let mut req_set = JoinSet::new();
        let path = path.to_string();

        // Broadcast the request to all nodes and collect responses
        for i in 0..node_tokens.len() {
            if i == self.id {
                continue; // Skip self
            }

            let node = node_tokens[i].clone();
            let dns_lookup = dns_lookup.clone();
            let path = path.clone();
            let req = async move {
                match dns_lookup.resolve_node(&node) {
                    Ok(ip) => {
                        trace!("Resolved node '{}' to IP: {}", node, ip);
                        // Send request to the node and handle response
                        let rsp = reqwest::Client::new()
                            .get(&format!("http://{}:80{}", ip, path))
                            .timeout(Duration::from_secs(1)) // We are in a local network, so timeout can be short
                            .send().await;

                        match rsp {
                            Ok(response) => {
                                if response.status().is_success() {
                                    match response.text().await {
                                        Ok(text) => Some(text),
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

        // Collect all responses
        req_set.join_all().await
    }

    /// Ask other nodes for votes and count the number of granted votes
    /// 
    /// Parameters:
    /// - dns_lookup: DNSLookup instance for resolving node addresses
    /// 
    /// Returns:
    /// - usize: Number of granted votes
    /// 
    /// # Examples
    /// ```ignore
    /// let votes = controller.ask_for_votes(dns_lookup).await;
    /// ```
    async fn ask_for_votes(&self, dns_lookup: DNSLookup) -> usize{
        // Placeholder for asking votes logic
        let candidate_id = self.state.candidate_id.load(Ordering::SeqCst);

        // Only ask for votes if we are the candidate
        assert!(candidate_id == self.id);
        
        trace!("VeritasController {} asking for votes for candidate {}.", self.id, candidate_id);
        let responses = self.broadcast_and_collect_responses("/vote", vec![], dns_lookup).await;
        let votes = responses.iter().enumerate().map(|(i, e)| {
            if let Some(vote) = e {
                // Process the vote
                trace!("VeritasController {} received vote from {}: {}", self.id, i, vote);
                let vote_granted: bool = vote.to_lowercase() == "true";
                vote_granted
            }else{
                // Handle no vote received
                false
            }
        });

        // Count the number of granted votes
        votes.filter(|e| *e).count() as usize
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
            "true".to_string()
        } else {
            debug!("Denying vote to sender ID: {}", sender_id);
            "false".to_string()
        };

        vote
    }

    /// Start serving HTTP requests
    pub async fn start_serving_http(&self) -> std::io::Result<()> {
        let shared = self.state.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(shared.clone())
                .route("/tick/{time}", web::get().to(VeritasController::<F>::tick))
                .route("/vote/{sender}", web::get().to(VeritasController::<F>::vote))
        });

        server.bind(("127.0.0.1", 80))?.run().await?;

        Ok(())
    }
}

