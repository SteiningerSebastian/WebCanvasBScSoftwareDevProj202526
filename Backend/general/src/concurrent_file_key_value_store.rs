use std::{collections::HashMap, fmt::Display, sync::{Arc, Mutex}};

use arc_swap::ArcSwap;

use crate::file_key_value_store::{self, FileKeyValueStore, KeyValueStore};

#[derive(Debug)]
pub enum Error {
    FileStoreError(file_key_value_store::Error),
    MutexLockPoisoned,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::FileStoreError(e) => write!(f, "File store error: {}", e),
            Error::MutexLockPoisoned => write!(f, "Mutex lock poisoned"),
        }
    }
}

impl From<file_key_value_store::Error> for Error {
    fn from(e: file_key_value_store::Error) -> Self {
        Error::FileStoreError(e)
    }
}

pub trait ConcurrentKeyValueStore {
    /// Begins a new transaction, returning a TransactionRef that holds the write lock.
    ///
    /// This method acquires the write mutex, blocking other writers and transactions
    /// until the returned TransactionRef is dropped or committed.
    /// 
    /// # Returns
    /// - `Ok(TransactionRef)`: on successful acquisition of the write lock.
    /// - `Err(Error::MutexLockPoisoned)`: if the write lock is poisoned.
    /// 
    /// # Example
    /// ``` ignore
    /// let store = ConcurrentFileKeyValueStore::new("data.txt").unwrap();
    /// let mut txn = store.begin_transaction().unwrap();
    /// txn.set("key", "value");
    /// txn.commit().unwrap();
    /// ```
    fn begin_transaction(&'_ self) -> Result<TransactionRef<'_>, Error>;

    /// Returns a clone of the value associated with `key` from the store's committed snapshot.
    /// 
    /// This method performs a lock-free read: it accesses the atomic committed snapshot and does
    /// not acquire the write mutex, so it will not block concurrent writers. Because it reads
    /// the last committed state, it will not observe uncommitted changes made by an ongoing
    /// transaction.
    /// 
    /// # Parameters
    /// - `key`: The key to look up (borrowed `&str`).
    /// 
    /// # Returns
    /// - `Some(String)` with a cloned, owned value if the key exists in the committed snapshot.
    /// - `None` if the key is not present.
    /// 
    /// # Notes
    /// - The returned `String` is an owned clone; modifying it does not affect the store.
    /// - Lookup is typically O(1) (hash map lookup).
    /// - Safe for concurrent calls from multiple threads.
    /// - If you require a view that includes in-progress writes, perform the read
    ///  from within a transaction that has acquired the lock.
    /// 
    /// # Example
    /// ```ignore
    /// if let Some(value) = store.get("my_key") {
    ///    println!("value: {}", value);
    /// }
    /// ```
    fn get(&self, key: &str) -> Option<String>;

    /// Sets the value for `key` in the store, acquiring the write lock to ensure exclusive access.
    /// This method will block until the lock is acquired.
    /// 
    /// # Parameters:
    /// - `key`: The key to set (borrowed `&str`).
    /// - `value`: The value to associate with the key (borrowed `&str`).
    /// # Returns:
    /// - `Ok(())` on success.
    /// - `Err(Error::MutexLockPoisoned)` if the write lock is poisoned.
    /// - `Err(Error::FileStoreError)` if committing the change to the underlying store fails.
    /// 
    /// # Notes:
    /// - This method acquires the write mutex, blocking other writers and transactions.
    /// - After setting the value, it immediately commits the change to persist it.
    /// - For multiple related changes, consider using a transaction via `begin_transaction()`.
    /// 
    /// # Example:
    /// ``` ignore
    /// let store = ConcurrentFileKeyValueStore::new("data.txt").unwrap();
    /// store.set("key", "value").unwrap();
    /// let value = store.get("key").unwrap();
    /// assert_eq!(value, "value");
    /// ```
    fn set(&self, key: &str, value: &str) -> Result<(), Error>;
}

pub struct ConcurrentFileKeyValueStore {
    state: Arc<ArcSwap<HashMap<String, String>>>,
    write_lock: Arc<Mutex<FileKeyValueStore>>,
}

impl Clone for ConcurrentFileKeyValueStore {
    fn clone(&self) -> Self {
        ConcurrentFileKeyValueStore {
            state: Arc::clone(&self.state),
            write_lock: Arc::clone(&self.write_lock),
        }
    }
}

unsafe impl Send for ConcurrentFileKeyValueStore {}
unsafe impl Sync for ConcurrentFileKeyValueStore {}

pub struct TransactionRef<'a> {
    _transaction_mutex_guard: std::sync::MutexGuard<'a, FileKeyValueStore>,
    committed: bool,
}

impl ConcurrentFileKeyValueStore {
    /// Creates a new ConcurrentFileKeyValueStore backed by the file at `file_path`.
    ///
    /// Attempts to open or create the underlying FileKeyValueStore and initializes the
    /// internal committed snapshot used for lock-free reads. A mutex is created to
    /// serialize writers and transactions.
    ///
    /// # Parameters
    /// - `file_path`: Path to the file used for persistent storage.
    ///
    /// # Returns
    /// - `Ok(Self)`: on successful initialization.
    /// - `Err(Error::FileStoreError)`: if opening or initializing the underlying file store fails.
    ///
    /// # Notes
    /// - Readers use an atomic snapshot and do not acquire the write mutex.
    /// - Writers and transactions acquire the mutex to ensure exclusive access while
    ///   staging and committing changes.
    pub fn new(file_path: &str) -> Result<Self, Error> {
        let store = FileKeyValueStore::new(file_path).map_err(Error::FileStoreError)?;
        let a = store.get_committed_arc();
        let write_lock: Arc<Mutex<FileKeyValueStore>> = Arc::new(Mutex::new(store));

        Ok(Self {
            state: a,
            write_lock,
        })
    }
}

impl<'a> TransactionRef<'a> {
    // operate on the inner store while holding the exclusive lock
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Error> {
        self._transaction_mutex_guard.set(key, value);
        Ok(())
    }

    /// Returns the value currently staged in this transaction for the given `key`.
    ///
    /// Looks up `key` in the transaction's staging area (uncommitted changes) and
    /// returns an owned `String` if a value has been staged. If the key is not
    /// present in the transaction's staged changes, `None` is returned.
    ///
    /// The returned value is a copy and consuming it does not alter transaction state.
    /// This method does not commit or persist any changes to the underlying store and
    /// only observes the transaction-local staged state.
    pub fn get(&self, key: &str) -> Option<String> {
        self._transaction_mutex_guard.get_staged(key)
    }

    /// Commits the current transaction, persisting all changes made during the transaction
    /// to the underlying key-value store.
    ///
    /// This method consumes the `TransactionRef`, ensuring that the transaction cannot be
    /// committed more than once. It acquires a mutable reference to the underlying store
    /// via the write guard and calls its `commit` method.
    ///
    /// # Errors
    ///
    /// Returns a [`Error`] if the commit operation fails.
    pub fn commit(mut self) -> Result<(), Error> {
        self._transaction_mutex_guard.commit().map_err(Error::FileStoreError)?;
        self.committed = true;
        Ok(())
    }  
}

impl ConcurrentKeyValueStore for ConcurrentFileKeyValueStore {
    /// Returns a clone of the value associated with `key` from the store's committed snapshot.
    ///
    /// This method performs a lock-free read: it accesses the atomic committed snapshot and does
    /// not acquire the write mutex, so it will not block concurrent writers. Because it reads
    /// the last committed state, it will not observe uncommitted changes made by an ongoing
    /// transaction.
    ///
    /// Parameters
    /// - `key`: The key to look up (borrowed `&str`).
    ///
    /// Returns
    /// - `Some(String)` with a cloned, owned value if the key exists in the committed snapshot.
    /// - `None` if the key is not present.
    ///
    /// Notes
    /// - The returned `String` is an owned clone; modifying it does not affect the store.
    /// - Lookup is typically O(1) (hash map lookup).
    /// - Safe for concurrent calls from multiple threads.
    /// - If you require a view that includes in-progress writes, perform the read
    ///   from within a transaction that has acquired the lock.
    ///
    /// Example
    /// ```ignore
    /// if let Some(value) = store.get("my_key") {
    ///     println!("value: {}", value);
    /// }
    /// ```
    fn get(&self, key: &str) -> Option<String> {
        // As we swap the entire map on commit, we can read without locks.
        let committed_map = self.state.load();
        committed_map.get(key).cloned()
    }

    /// Sets the value for `key` in the store, acquiring the write lock to ensure exclusive access.
    /// This method will block until the lock is acquired.
    /// 
    /// Parameters:
    /// - `key`: The key to set (borrowed `&str`).
    /// - `value`: The value to associate with the key (borrowed `&str`).
    /// 
    /// Returns:
    /// - `Ok(())` on success.
    /// - `Err(Error::MutexLockPoisoned)` if the write lock is poisoned.
    /// - `Err(Error::FileStoreError)` if committing the change to the underlying store fails.
    /// 
    /// # Notes:
    /// - This method acquires the write mutex, blocking other writers and transactions.
    /// - After setting the value, it immediately commits the change to persist it.
    /// - For multiple related changes, consider using a transaction via `begin_transaction()`.
    /// 
    /// # Example:
    /// ``` ignore
    /// let store = ConcurrentFileKeyValueStore::new("data.txt").unwrap();
    /// store.set("key", "value").unwrap();
    /// let value = store.get("key").unwrap();
    /// assert_eq!(value, "value");
    /// ```
    fn set(&self, key: &str, value: &str) -> Result<(), Error> {
        let mut store = self.write_lock.lock()
            .map_err(|_| Error::MutexLockPoisoned)?;

        store.set(key, value);
        store.commit().map_err(Error::FileStoreError)?;

        Ok(())
    }

    /// Begins a new transaction by acquiring a write lock on the underlying key-value store.
    /// 
    /// This method returns a `TransactionRef` which holds the write lock for the duration of the transaction,
    /// allowing multiple operations to be performed atomically. Changes made within the transaction are
    /// staged in-memory and can be committed together. The transaction ensures that no other thread can
    /// write to the store until the transaction is completed or dropped.
    /// 
    /// # Returns
    ///
    /// A `Result<TransactionRef, Error>` that provides transactional access to the key-value store.
    ///
    /// # Example
    /// 
    /// ``` ignore
    /// let store = ConcurrentFileKeyValueStore::new("data.txt").unwrap();
    /// let mut txn = store.begin_transaction().unwrap();
    /// txn.set("key", "value");
    /// txn.commit().unwrap();
    /// ```
    fn begin_transaction(&'_ self) -> Result<TransactionRef<'_>, Error> {
        let _transaction_mutex_guard = self.write_lock.lock()
            .map_err(|_| Error::MutexLockPoisoned)?;
        Ok(TransactionRef { _transaction_mutex_guard, committed: false })
    }
}

impl<'a> Drop for TransactionRef<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // No changes are persisted to disk until commit, so clearing staged values is sufficient.
            // We need to panic if rollback fails in drop, as ignoring it would leave the store in an inconsistent 
            // potentially unsafe state.
            self.rollback().expect("Rollback in drop must not fail!");
        }
    }
}

impl<'a> TransactionRef<'a> {
    /// Rolls back the current transaction, discarding any staged changes.
    /// This is called automatically if the transaction is dropped without commit.
    pub fn rollback(&mut self) -> Result<(), Error> {
        self._transaction_mutex_guard.rollback();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};
    use tempfile::TempDir;

    use super::*;

    fn make_test_dir(test_name: &str) -> TempDir {
    tempfile::Builder::new()
        .prefix(&format!("tmp_test_{}_", test_name))
        .tempdir()
        .expect("create temp dir")
    }

    // --- Tests ---

    #[test]
    fn single_thread_basic_get_set_commit() {
        let tmp = make_test_dir("single_thread_basic_get_set_commit");
        let path = tmp.path().to_str().unwrap();

        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");

        // initially empty
        assert_eq!(store.get("a"), None);

        // set and commit via set()
        store.set("a", "1").expect("set should succeed");
        assert_eq!(store.get("a"), Some("1".to_owned()));

        // set again
        store.set("b", "xyz").expect("set should succeed");
        assert_eq!(store.get("b"), Some("xyz".to_owned()));

        // tmp drops here and directory is removed
    }

    #[test]
    fn transaction_keeps_exclusive_access_and_commits() {
        let tmp = make_test_dir("transaction_keeps_exclusive_access_and_commits");
        let path = tmp.path().to_str().unwrap();

        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");

        // Use a transaction to set multiple values
        {
            let mut tx = store.begin_transaction().expect("begin tx");
            tx.set("t1", "v1").expect("failed to set value");
            tx.set("t2", "v2").expect("failed to set value");

            // read via transaction while still holding lock
            assert_eq!(tx.get("t1"), Some("v1".to_owned()));
            assert_eq!(tx.get("t2"), Some("v2".to_owned()));

            // commit via transaction (this consumes tx and releases lock at end)
            tx.commit().expect("tx commit ok");
        }

        // After transaction, values should be visible
        assert_eq!(store.get("t1"), Some("v1".to_owned()));
        assert_eq!(store.get("t2"), Some("v2".to_owned()));

        // tmp drops here and directory is removed
    }

    #[test]
    fn concurrent_reads_are_allowed() {
        let tmp = make_test_dir("concurrent_reads_are_allowed");
        let path = tmp.path().to_str().unwrap();

        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");

        store.set("r", "readval").expect("set should succeed");

        // spawn a few reader threads, they should all see the value
        let shared = Arc::new(store);
        let mut handles = vec![];
        for _ in 0..8 {
            let s = Arc::clone(&shared);
            handles.push(thread::spawn(move || {
                // small sleep to increase concurrency overlap
                thread::sleep(Duration::from_millis(10));
                assert_eq!(s.get("r"), Some("readval".to_owned()));
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }

        // tmp drops here and directory is removed
    }

    #[test]
    fn concurrent_write_exclusion() {
        let tmp = make_test_dir("concurrent_write_exclusion");
        let path = tmp.path().to_str().unwrap();

        // This test checks that a long-running transaction excludes other writers
        // and readers for the duration of the exclusive lock held by the transaction.
        // We model a writer that holds the write lock for a bit and check ordering.

        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");
        let shared = Arc::new(store);

        // Start a thread that begins a transaction and sleeps while holding the lock.
        let s1 = Arc::clone(&shared);
        let t1 = thread::spawn(move || {
            let mut tx = s1.begin_transaction().expect("begin tx");
            tx.set("x", "in-tx").expect("failed to set value");
            // hold the lock for a short while
            thread::sleep(Duration::from_millis(200));
            // commit and release
            tx.commit().expect("commit");
            // After commit, value should be visible to others
        });

        // Give t1 a chance to take the lock
        thread::sleep(Duration::from_millis(20));

        // Start another thread that tries to set a value - it will block until the tx releases
        let s2 = Arc::clone(&shared);
        let t2 = thread::spawn(move || {
            // This set() will acquire write lock and should only complete after tx commits
            s2.set("y", "after-tx").expect("set after tx");
        });

        t1.join().expect("t1 join");
        t2.join().expect("t2 join");

        // check values: x from transaction and y from the second writer
        assert_eq!(shared.get("x"), Some("in-tx".to_owned()));
        assert_eq!(shared.get("y"), Some("after-tx".to_owned()));

        // tmp drops here and directory is removed
    }

    #[test]
    fn transaction_cannot_outlive_store_borrow() {
        let tmp = make_test_dir("transaction_cannot_outlive_store_borrow");
        let path = tmp.path().to_str().unwrap();

        // This test ensures the transaction's lifetime is tied to the &self used to create it.
        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");

        // create transaction in an inner scope
        {
            let mut tx = store.begin_transaction().expect("begin tx");
            tx.set("k", "v").expect("failed to set value");
            assert_eq!(tx.get("k").as_deref(), Some("v"));
            tx.commit().expect("commit");
        }

        // After the transaction scope ended, we can still use store as normal
        assert_eq!(store.get("k"), Some("v".to_owned()));

        // tmp drops here and directory is removed
    }

    #[test]
    fn transaction_rolls_back_on_drop() {
        let tmp = make_test_dir("transaction_rolls_back_on_drop");
        let path = tmp.path().to_str().unwrap();

        let store = ConcurrentFileKeyValueStore::new(path).expect("create store");

        // Initially no value committed
        assert_eq!(store.get("tmpkey"), None);
        {
            // Begin a transaction and set a key
            let mut tx = store.begin_transaction().expect("begin tx");
            tx.set("tmpkey", "tempval").expect("set in tx");

            // Transaction should see its own staged write
            assert_eq!(tx.get("tmpkey"), Some("tempval".to_owned()));
        }

        // Store (committed view) must NOT see the uncommitted value
        assert_eq!(store.get("tmpkey"), None);

        // After drop (rollback), the committed store must still not see the value.
        assert_eq!(store.get("tmpkey"), None);
    }
}