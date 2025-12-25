use std::{collections::HashMap, sync::{Arc, PoisonError, RwLock, RwLockReadGuard, atomic::AtomicUsize}};

use parking_lot::Mutex;

pub struct KeyedLock {  
    locks: Arc<Mutex<HashMap<usize, (Arc<std::sync::RwLock<()>>, Arc<AtomicUsize>)>>>,
}

pub struct KeyedLockGuard {
    key: usize,
    lock: Arc<std::sync::RwLock<()>>,
    locks: Arc<Mutex<HashMap<usize, (Arc<std::sync::RwLock<()>>, Arc<AtomicUsize>)>>>,
}

impl KeyedLockGuard {
    /// Acquires a read lock for the associated key.
    pub fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>, PoisonError<std::sync::RwLockReadGuard<'_, ()>>> {
        self.lock.read()
    }

    /// Acquires a write lock for the associated key.
    pub fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, ()>, PoisonError<std::sync::RwLockWriteGuard<'_, ()>>> {
        self.lock.write()
    }
}

impl Drop for KeyedLockGuard {
    fn drop(&mut self) {
        let mut locks = self.locks.lock();
        if let Some((_, count)) = locks.get(&self.key) {
            let prev_count = count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            if prev_count == 1 {
                // Last reference to this lock, remove it to keep the memory footprint small
                locks.remove(&self.key);
            }
        }
    }
}

impl KeyedLock {
    pub fn new() -> Self {
        KeyedLock {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retrieves the lock associated with the given key, creating it if it doesn't exist.
    pub fn lock(&self, key: usize) -> Result<KeyedLockGuard, PoisonError<RwLockReadGuard<'_, ()>>> {
        let (lock, _) = {
            let mut locks = self.locks.lock();
            let (lock, count) = locks.entry(key).or_insert_with(|| (Arc::new(RwLock::new(())), Arc::new(AtomicUsize::new(0))));
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (Arc::clone(lock), Arc::clone(&count))
        }; // release the mutex lock

        Ok(KeyedLockGuard {
            key,
            lock: lock,
            locks: Arc::clone(&self.locks),
        })
    }
}