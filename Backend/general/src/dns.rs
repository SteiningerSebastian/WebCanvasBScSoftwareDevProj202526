use std::{collections::HashMap, net::{IpAddr, ToSocketAddrs}, sync::{Arc, RwLock}, time::{Duration, Instant}};

pub struct DNSLookup{
    // DNS cache: token -> (IpAddr, time_of_resolution)
    dns_cache: Arc<RwLock<HashMap<String, (IpAddr, Instant)>>>,
    ttl: Duration,
}

impl Clone for DNSLookup {
    fn clone(&self) -> Self {
        DNSLookup {
            dns_cache: Arc::clone(&self.dns_cache),
            ttl: self.ttl,
        }
    }
}

impl DNSLookup {
    pub fn new(ttl: Duration) -> Self {
        DNSLookup {
            dns_cache: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// Resolve a single token (literal IP or hostname) to an IpAddr right when it's needed.
    /// Returns an io::Result so callers can decide how to handle failures.
    fn resolve_node_once(token: &str) -> std::io::Result<IpAddr> {
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
        &self,
        token: &str,
        cache: &RwLock<HashMap<String, (IpAddr, Instant)>>,
    ) -> std::io::Result<IpAddr> {
        // First try read lock to check cache quickly.
        {
            let r = cache.read().unwrap();
            if let Some((ip, ts)) = r.get(token) {
                if ts.elapsed() < self.ttl {
                    return Ok(*ip);
                }
            }
        }

        // Cache miss or expired: resolve and update cache with write lock.
        let ip = DNSLookup::resolve_node_once(token)?;
        let mut w = cache.write().unwrap();
        w.insert(token.to_string(), (ip, Instant::now()));
        Ok(ip)
    }

    pub fn resolve_node(
        &self,
        token: &str,
    ) -> std::io::Result<IpAddr> {
        self.resolve_node_cached(token, &self.dns_cache)
    }
}