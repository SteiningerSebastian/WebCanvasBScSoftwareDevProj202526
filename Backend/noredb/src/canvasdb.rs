use std::{fmt::Display, process, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}};

use crossbeam::channel::RecvTimeoutError;
use general::{keyed_lock::{self, KeyedLock}, persistent_random_access_memory::{self, PersistentRandomAccessMemory, Pointer}, pram_btree_index::{self, BTreeIndex, BTreeIndexPRAM}, write_ahead_log::{self, WriteAheadLog, WriteAheadLogTrait}};
use tracing::{debug, error, info};

const PERSIST_BATCH_SIZE: usize = 8192; // Number of entries to process in each batch by worker threads.
const PERSIST_LATENCY_MS: u64 = 100; // Time to wait before persisting the write-ahead log and calling listeners. This is the maximum time a listener will wait before being called.
const GET_BATCH_SIZE: usize = 8192; // Number of entries to get from the write-ahead log in each batch. (Improves performance as we can read sequentially so fewer page faults occur.)
const RETRY_LIMIT: usize = 5; // Number of times to retry an operation before giving up.
const RETRY_BASE_DELAY_MS: u64 = 10; // Base delay in milliseconds for retrying operations.
const EXPONENTIAL_BACKOFF_FACTOR: u64 = 2; // Factor by which to multiply the delay for each retry.

#[derive(Debug)]
pub enum Error {
    PoisonedLock,
    MemoryError{
        address: u64,
        inner: persistent_random_access_memory::Error,
    },
    BTreeIndexError{
        inner: pram_btree_index::Error,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::PoisonedLock => write!(f, "Poisoned lock error"),
            Error::MemoryError { address, inner } => write!(f, "Memory error at address {}: {}", address, inner),
            Error::BTreeIndexError { inner } => write!(f, "B-tree index error: {}", inner),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeStamp {
    pub bytes: [u8; 16], // 16-byte timestamp representation
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Pixel{
    pub key: u32, // unique key for pixel (derived from x,y)
    pub color: [u8; 3], // RGB
}

#[repr(C)]
pub struct PixelEntry {
    pub pixel: Pixel,
    pub timestamp: TimeStamp,
}

impl Clone for PixelEntry {
    fn clone(&self) -> Self {
        PixelEntry {
            pixel: Pixel {
                key: self.pixel.key,
                color: self.pixel.color,
            },
            timestamp: TimeStamp {
                bytes: self.timestamp.bytes,
            },
        }
    }
}

pub trait CanvasDBTrait {
    /// Sets the pixel data in the CanvasDB.
    /// 
    /// Parameters:
    /// - `pixel`: The PixelEntry containing pixel data and timestamp to be set.
    /// - `listener`: An optional callback function to be invoked after setting the pixel.
    ///
    /// Info:
    /// This function appends the pixel entry to the write-ahead log for persistence.
    /// The listner is called once the pixel has been successfully and permanently stored.
    /// The actual update to the pixel data store is handled asynchronously by worker threads.
    fn set_pixel(&self, pixel: PixelEntry, listener: Option<Box<dyn FnOnce() + Send>>);

    /// Retrieves the pixel data and timestamp for the specified (x, y) coordinates.
    ///
    /// Parameters:
    /// - `x`: The x-coordinate of the pixel.
    /// - `y`: The y-coordinate of the pixel.
    /// 
    /// Returns:
    /// - `Some((Pixel, TimeStamp))` if the pixel exists, containing the pixel data and its timestamp.
    /// - `None` if the pixel does not exist.
    fn get_pixel(&self, key: u32) -> Option<(Pixel, TimeStamp)>;

    /// Returns an iterator over all pixels in the CanvasDB.
    /// 
    /// Warning: Due to performance considerations, this function may return stale data if there are ongoing updates.
    /// This is because only the indexed pixels in the B-tree are iterated over, and recent updates in the write-ahead log may not be reflected.
    /// Because the write-ahead log is not temporally ordered, merging the two sources during iteration would be memory and computationally intensive and slow.
    /// 
    /// Returns:
    /// - `Box<dyn Iterator<Item = Pixel>>`: An iterator that yields Pixel entries.
    fn iterate_pixels(&self) -> Result<Box<dyn Iterator<Item = Pixel> + Send>, Error>;
}

pub struct CanvasDB {
    // Fields representing the state of the CanvasDB
    /// Write-ahead log for recording pixel changes
    write_ahead_log: Arc<WriteAheadLog<PixelEntry>>,

    /// B-tree index for efficient pixel lookup
    btree_index: Arc<BTreeIndexPRAM>,

    /// Persistent random access memory for storing pixel data
    data_store: Arc<PersistentRandomAccessMemory>,

    /// Listeners for pixel set events
    listeners: Arc<Mutex<Vec<Box<dyn FnOnce() + Send>>>>,

    /// Flag to signal worker threads to stop
    shutdown: Arc<AtomicBool>,

    /// Handles to worker threads for joining on shutdown
    worker_handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
}

impl CanvasDB {
    pub fn new(
        width: usize,
        height: usize,
        path: &str,
        write_ahead_log_size: usize,
    ) -> Self {
        // +1024 for any necessary pointers, implementation overhead, etc.
        let wal_pram = PersistentRandomAccessMemory::new(write_ahead_log_size*std::mem::size_of::<PixelEntry>() + 1024,&format!("{}.wal.pram", path));
        // *32 for each btree entry there is a u64 (8bytes) key a u64 (8bytes) value and some overhead (+1024bytes and *32 not *16)
        // *2 for non edge nodes
        let btree_pram: Arc<PersistentRandomAccessMemory> = PersistentRandomAccessMemory::new((width as usize * height as usize * 32 * 2) + 1024, &format!("{}.index.pram", path));
        // *2 for alignment and heap fragmentation +1024 for any overhead
        let data_store: Arc<PersistentRandomAccessMemory> = PersistentRandomAccessMemory::new((width as usize * height as usize * std::mem::size_of::<PixelEntry>()*2) + 1024, &format!("{}.store.pram", path));

        // Initialize the write-ahead log, B-tree index, and data store
        let write_ahead_log = Arc::new(WriteAheadLog::new(wal_pram, write_ahead_log_size));
        let btree_index = Arc::new(BTreeIndexPRAM::new(btree_pram));

        CanvasDB {
            write_ahead_log,
            btree_index,
            data_store,
            listeners: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            worker_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Sets the pixel data if the new pixel entry has a newer timestamp than the existing one.
    /// 
    /// Parameters:
    /// - `old_pixel`: A pointer to the existing pixel entry in the data store.
    /// - `new_pixel`: The new pixel entry to be set.
    /// 
    /// Info:
    /// This function compares the timestamps of the existing and new pixel entries.
    /// If the new pixel entry has a newer timestamp, it updates the pixel data and timestamp
    /// in the data store.
    /// 
    /// Returns:
    /// - `Ok(())` if the operation is successful.
    /// - `Err(Box<dyn error::Error>)` if there is an error during dereferencing or setting the pixel data.
    fn set_pixel_if_newer(old_pixel: &Pointer<PixelEntry>, new_pixel: &PixelEntry) -> Result<(), Error> {
        // Get current timestamp
        let entry = old_pixel.deref().map_err(|e| Error::MemoryError { address: old_pixel.address, inner: e })?;
        if new_pixel.timestamp > entry.timestamp {
            // Update pixel data and timestamp
            old_pixel.set(&new_pixel).map_err(|e| Error::MemoryError { address: old_pixel.address, inner: e })?;
        }
        Ok(())
    }

    /// Atomically updates a pixel entry in the data store if the new pixel entry has a newer timestamp.
    ///
    /// Parameters:
    /// - `pixel`: The new pixel entry to be set.
    /// - `pointer`: A pointer to the existing pixel entry in the data store.
    /// - `keyed_lock`: An Arc to the KeyedLock for synchronizing access to pixel entries.
    /// 
    /// Info:
    /// This function acquires a lock for the pixel key to ensure no concurrent updates occur.
    /// It then checks if the new pixel entry has a newer timestamp than the existing one,
    /// and updates the pixel data if so.
    ///     
    /// Returns:
    /// - `Ok(())` if the operation is successful.
    /// - `Err(Box<dyn error::Error>)` if there is an error during the update process.
    fn atomic_update_pixel(pixel: &PixelEntry, pointer: &Pointer<PixelEntry>, keyed_lock: &Arc<KeyedLock>) -> Result<(), Error> {
        let key = pixel.pixel.key;

        // Aquire lock for this pixel key to ensure no concurrent updates as multiple raw pointers could exist to the same pixel entry.
        let lock_guard = keyed_lock.lock(key as usize).map_err(|_| Error::PoisonedLock)?;
        // aquire write access to the pixel data
        let _write_guard = lock_guard.write();
        Self::set_pixel_if_newer(pointer, pixel)?;
        Ok(())
    }

    /// Starts worker threads to process the write-ahead log and update the B-tree index and data store.
    /// 
    /// Parameters:
    /// - `num_threads`: The number of worker threads to start.
    /// 
    /// Info: 
    /// Each worker thread continuously processes entries from the write-ahead log, updating the B-tree index and data store accordingly.
    /// This function spawns the specified number of threads, each running an infinite loop to handle log processing, non-blockingly.
    pub fn start_worker_threads(&self, num_threads: usize) {
        // Start a separate thread for periodic persistence of the write-ahead log and call listeners.
        let wal_clone = Arc::clone(&self.write_ahead_log);
        let listeners_clone = Arc::clone(&self.listeners);
        let shutdown_clone = Arc::clone(&self.shutdown);
        let handles_clone = Arc::clone(&self.worker_handles);

        let handle = std::thread::spawn(move || {
            // Periodically call listeners after pixels have been persisted
            loop {
                if shutdown_clone.load(Ordering::Relaxed) {
                    debug!("Shutting down write-ahead log persistence thread.");
                    break;
                }                

                std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));

                // Call all listeners
                let v_listeners: Vec<Box<dyn FnOnce() + Send>> = {
                    let mut listeners = listeners_clone.lock();
                    match listeners {
                        Ok(ref mut guard) => {
                            guard.drain(..).collect()
                        }
                        Err(e) => {
                            error!("Failed to acquire lock on listeners: {}", e);
                            Vec::new() // Return empty vector on error - process the updates and accept we can't call listeners this time.
                        }
                    }
                };

                // Commit the write-ahead log to ensure all entries are persisted.
                let commit = wal_clone.commit();
                
                match commit {
                    Ok(_) => {
                        debug!("Successfully committed write-ahead log before calling listeners.");

                        let n = v_listeners.len();
                        // Call each listener -> data has been persisted
                        for listener in v_listeners {
                            listener();
                        }

                        debug!("Called {} listeners after persisting write-ahead log.", n);
                    }
                    Err(write_ahead_log::Error::FatalError(e)) => {
                        error!("Fatal error committing write-ahead log before calling listeners: {}. Aborting process to prevent data corruption.", e);
                        // Shut down immediately to prevent data corruption.
                        Self::shutdown(shutdown_clone.clone(), handles_clone.clone());
                        process::abort(); // Fatal error - we must abort to prevent data corruption.
                    }
                    Err(e) => {
                        error!("Failed to commit write-ahead log before calling listeners: {}", e);
                        continue; // try again in next iteration
                    }
                }
            }
        });

        {
            let mut handles = self.worker_handles.lock().unwrap();
            handles.push(handle);
        }

        let wal_clone = Arc::clone(&self.write_ahead_log);
        let btree_clone = Arc::clone(&self.btree_index);
        let data_store_clone = Arc::clone(&self.data_store);
        let shutdown_clone = Arc::clone(&self.shutdown);
        let handles_clone = Arc::clone(&self.worker_handles);

        let (tx_work, rx_work) = crossbeam::channel::unbounded::<PixelEntry>();
        let (tx_res, rx_res) = std::sync::mpsc::channel::<()>();

        // Start a separate thread for periodic persistence, datastructures handle their own consistency.
        let handle =std::thread::spawn(move || {
            // Periodically persist data structures to disk
            loop {
                if shutdown_clone.load(Ordering::Relaxed) {
                    debug!("Shutting down data structures persistence thread.");
                    break;
                }

                // Load the next batch of entries from the write-ahead log
                let entries = wal_clone.peek_many(PERSIST_BATCH_SIZE);
                if let Err(write_ahead_log::Error::PeekFailed) = entries {
                    debug!("Failed to peek entries from write-ahead log.");
                    // Wait before trying again
                    std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));
                    continue; // try again in next iteration
                }
                // Unwrap is safe here as we checked for error above
                let entries = entries.unwrap();

                let number_of_entries = entries.len();
                if entries.is_empty() {
                    // No entries to process, sleep for a while to avoid busy waiting.
                    std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));
                    continue;
                }

                // Process each entry in the batch
                for _entry in entries {
                    // Send entry to worker threads for processing
                    for i in 0..RETRY_LIMIT {
                        match tx_work.send(_entry.clone()) {
                            Ok(_) => break, // Successfully sent
                            Err(e) => {
                                error!("Failed to send pixel entry to worker threads: {}", e);

                                if i == RETRY_LIMIT - 1 {
                                    panic!("Failed to send pixel entry to worker threads after multiple attempts: {}", e);
                                }

                                // Retry sending after a short delay
                                std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(i as u32)));
                            }
                        }
                    }
                }

                // Wait for all entries in the batch to be processed
                for _ in 0..number_of_entries { 
                    match rx_res.recv() {
                        Ok(_) => {} // Successfully processed
                        Err(e) => {
                            error!("Failed to receive confirmation from worker threads: {}", e);
                            continue; // try again in next iteration
                        }
                    }
                }


                // Persist data structures
                // If we fail here it's not a big deal as we can always recover from the WAL on startup - the data store has not been persisted and therefore we can reapply the entries on startup.
                match data_store_clone.persist() {
                    Ok(_) => {
                        debug!("Successfully persisted data store.");
                    },
                    Err(persistent_random_access_memory::Error::FatalError(e)) => {
                        error!("Fatal error persisting data store: {}. Aborting process to prevent data corruption.", e);

                        Self::shutdown(shutdown_clone.clone(), handles_clone.clone());
                        process::abort(); // Fatal error - we must abort to prevent data corruption.
                    }, 
                    Err(e) => {
                        error!("Failed to persist data store: {}", e);
                        continue; // try again in next iteration
                    }
                }

                // If we fail here it's not a big deal as we can always recover from the WAL on startup - the B-Tree index has not been persisted and therefore we can reapply the entries on startup.

                match btree_clone.persist() {
                    Ok(_) => {
                        debug!("Successfully persisted B-tree index.");
                    },
                    Err(pram_btree_index::Error::FatalError(e)) => {
                        error!("Fatal error persisting B-tree index: {}. Aborting process to prevent data corruption.", e);
                        Self::shutdown(shutdown_clone.clone(), handles_clone.clone());
                        process::abort(); // Fatal error - we must abort to prevent data corruption.
                    }, 
                    Err(e) => {
                        error!("Failed to persist B-tree index: {}", e);
                        continue; // try again in next iteration
                    }
                }

                // Remove the peaked entries from the write-ahead log
                match wal_clone.pop_many(number_of_entries) {
                    Ok(_) => {},
                    Err(write_ahead_log::Error::FatalError(e)) => {
                        error!("Fatal error popping entries from write-ahead log: {}. Aborting process to prevent data corruption.", e);
                        // Shut down immediately to prevent data corruption.
                        Self::shutdown(shutdown_clone.clone(), handles_clone.clone());
                        process::abort(); // Fatal error - we must abort to prevent data corruption.
                    },
                    Err(e) => {
                        error!("Failed to pop entries from write-ahead log: {}", e);
                        continue; // try again in next iteration
                    }
                }

                // Successfully persisted all data structures
                debug!("Successfully persisted data store, B-tree index, and write-ahead log.");
            }
        });

        {
            let mut handles = self.worker_handles.lock().unwrap();
            handles.push(handle);
        }
        
        let keyed_lock = Arc::new(keyed_lock::KeyedLock::new());

        // Start worker threads to process the write-ahead log and update the B-tree index and data store
        for _ in 0..num_threads {
            let btree_clone = Arc::clone(&self.btree_index);
            let data_store_clone = Arc::clone(&self.data_store);
            let keyed_lock_clone: Arc<keyed_lock::KeyedLock> = Arc::clone(&keyed_lock);
            let rx = rx_work.clone();
            let tx_res = tx_res.clone();
            let shutdown_clone = Arc::clone(&self.shutdown);

            let handle = std::thread::spawn(move || {
                // Instead of panicking on errors, we should try to log them and recover where possible. 
                let mut retries = 0;
                loop {
                    if shutdown_clone.load(Ordering::Relaxed) {
                        debug!("Shutting down worker thread.");
                        break;
                    }

                    let pixel = rx.recv_timeout(std::time::Duration::from_millis(100));

                    if let Err(RecvTimeoutError::Timeout) = pixel {
                        // Timeout occurred, check for shutdown signal and continue
                        continue;
                    }

                    if let Err(e) = pixel {
                        error!("Worker thread failed to receive pixel entry: {}", e);
                        retries += 1;

                        // Retry logic with exponential backoff
                        if retries >= RETRY_LIMIT {
                            panic!("Worker thread failed to receive pixel entry after multiple attempts: {}", e);
                        }
                        std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(retries as u32)));
                        continue; // try again in next iteration
                    }
                    let pixel = pixel.unwrap();

                    // Update B-tree index and data store
                    let key = pixel.pixel.key;

                    // Using get here to check if the pixel already exists, this is faster than get_or_set as we avoid holding the write lock on the B-tree unnecessarily.
                    let pointer = btree_clone.get(key as u64);
                    if let Ok(pointer) = pointer {
                        // Update existing pixel
                        let pointer:Pointer<PixelEntry> = Pointer::from_address(pointer, data_store_clone.clone());
                        
                        for i in 0..RETRY_LIMIT {
                            let result = Self::atomic_update_pixel(&pixel, &pointer, &keyed_lock_clone);
                            match result {
                                Ok(_) => break, // Successfully updated
                                Err(e) => {
                                    error!("Failed to atomically update pixel entry: {:?}", e);

                                    // Retry updating after a short delay
                                    std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(i as u32)));
                                    retries += 1; // Global retry count to not get stuck at different operations and still detect vaults.

                                    if retries >= RETRY_LIMIT {
                                        panic!("Failed to atomically update pixel entry after multiple attempts: {:?}", e);
                                    }
                                }
                            }
                        }
                    } else {
                        let length = std::mem::size_of::<PixelEntry>();
                        // Insert new pixel
                        let new_pointer = data_store_clone.malloc::<PixelEntry>(length);
                        if let Ok(new_pointer) = new_pointer {
                            // Get or insert the pointer for this pixel in the B-tree index.
                            let contained_pointer= btree_clone.get_or_set(key as u64, new_pointer.address as u64);

                            // If we successfully inserted into or read from the B-tree index, write the pixel data to the data store.
                            if let Ok(contained_pointer) = contained_pointer {
                                // If another thread inserted the pixel before us, free the allocated memory
                                if new_pointer.address != contained_pointer {
                                    data_store_clone.free(new_pointer.address, length).unwrap();
                                }

                                let pointer:Pointer<PixelEntry> = Pointer::from_address(contained_pointer, data_store_clone.clone());
                               
                                for i in 0..RETRY_LIMIT {
                                    let result = Self::atomic_update_pixel(&pixel, &pointer, &keyed_lock_clone);
                                    match result {
                                        Ok(_) => break, // Successfully updated
                                        Err(e) => {
                                            error!("Failed to atomically update pixel entry: {:?}", e);

                                            // Retry updating after a short delay
                                            std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(i as u32)));
                                            retries += 1; // Global retry count to not get stuck at different operations and still detect vaults.

                                            if retries >= RETRY_LIMIT {
                                                panic!("Failed to atomically update pixel entry after multiple attempts: {:?}", e);
                                            }
                                        }
                                    }
                                }
                            } else {
                                retries += 1; // Global retry count to not get stuck at different operations and still detect vaults.

                                if retries >= RETRY_LIMIT {
                                    // Free the allocated memory if malloc failed
                                    let mem_free = data_store_clone.free(new_pointer.address, length);
                                    if let Err(e) = mem_free {
                                        error!("Failed to free memory for pixel entry after B-tree insertion failure: {}", e);
                                    }

                                    error!("Failed to insert new pixel into B-tree index.");
                                    panic!("Failed to insert new pixel into B-tree index.");
                                }
                                

                                std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(retries as u32)));
                                continue; // try again in next iteration
                            }                                    
                        } else{
                            retries += 1; // Global retry count to not get stuck at different operations and still detect vaults.

                            if retries >= RETRY_LIMIT {
                                error!("Failed to allocate memory for new pixel in data store.");

                                // A bit of debug info
                                dbg!(data_store_clone);

                                panic!("Failed to allocate memory for new pixel in data store. {}", new_pointer.err().unwrap());
                            }
                            std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(retries as u32)));
                            continue; // try again in next iteration
                        }
                    }
                    // Notify that the pixel has been processed
                    for i in 0..RETRY_LIMIT {
                        if let Ok(()) = tx_res.send(()){
                            break; // Successfully sent
                        } else {
                            error!("Failed to send confirmation to main thread from worker.");
                            retries += 1;

                            // Retry sending after a short delay
                            std::thread::sleep(std::time::Duration::from_millis(RETRY_BASE_DELAY_MS * EXPONENTIAL_BACKOFF_FACTOR.pow(i as u32)));
                        }
                    }

                    if retries >= RETRY_LIMIT {
                        panic!("Failed to send confirmation to main thread from worker after multiple attempts.");
                    }

                    // reset retries on success
                    retries = 0; 
                }
            });

            {
                let mut handles = self.worker_handles.lock().unwrap();
                handles.push(handle);
            }
        }
    }

    /// Shuts down the CanvasDB by signaling worker threads to stop and waiting for them to finish.
    /// 
    /// Info:
    /// This function sets the shutdown flag to true, signaling all worker threads to stop processing.
    /// It then waits for all threads to finish by joining on their handles.
    pub fn shutdown(shutdown: Arc<AtomicBool>, worker_handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>) {
        info!("Shutting down CanvasDB worker threads.");
        // Wait for all log messages to be processed
        std::thread::sleep(std::time::Duration::from_millis(100));

        shutdown.store(true, Ordering::Relaxed);
        
        // Wait for all threads to finish
        let mut handles = worker_handles.lock().unwrap();
        while let Some(handle) = handles.pop() {
            let _ = handle.join();
        }
        
        debug!("All worker threads have been shut down");
    }
}

impl Drop for CanvasDB {
    fn drop(&mut self) {
        Self::shutdown(self.shutdown.clone(), self.worker_handles.clone());
    }
}

impl CanvasDBTrait for CanvasDB {
    fn set_pixel(&self, pixel: PixelEntry, listener: Option<Box<dyn FnOnce() + Send>>) {
        // Append the pixel entry to the write-ahead log
        if let Err(e) = self.write_ahead_log.append(&pixel) {
            error!("Failed to append pixel to write-ahead log: {}", e);
            panic!("Failed to append pixel to write-ahead log: {}", e);
        }

        if let Some(callback) = listener {
            let locked = self.listeners.lock();
            if let Ok(mut guard) = locked {
                guard.push(callback);
            } else {
                error!("Failed to acquire lock on listeners to add new listener.");
            }
        }
    }

    fn get_pixel(&self, key: u32) -> Option<(Pixel, TimeStamp)> {
        // Search for the pixel in the Write-Ahead Log first - it may have more recent updates than the B-tree index and data store.
        let most_current_wal = self.write_ahead_log.iter(GET_BATCH_SIZE)
            .flatten()
            .filter(|e| {
                let e_key = e.pixel.key;
                e_key == key
            }).max_by(|a, b| a.timestamp.cmp(&b.timestamp));
        
        // Now check the B-tree index and data store (We can't be sure that updates have arrived in order, 
        // so we need to compare timestamps and also search the B-tree index even if we found an entry in the WAL.)
        let pointer_address = self.btree_index.get(key as u64);
        if let Ok(pointer_address) = pointer_address{
            let pointer:Pointer<PixelEntry> = Pointer::from_address(pointer_address, self.data_store.clone());
            if let Ok(entry) = pointer.deref() {
                // If we found an entry in the WAL, compare timestamps
                if let Some(wal_entry) = most_current_wal {
                    if wal_entry.timestamp > entry.timestamp {
                        return Some((wal_entry.pixel, wal_entry.timestamp));
                    }
                }
                return Some((entry.pixel, entry.timestamp));
            } else {
                error!("Failed to deref pixel pointer from data store.");
                return None;
            }
        } else if let Err(_) = pointer_address {

            // No entry in B-tree index, check WAL
            if let Some(wal_entry) = most_current_wal {
                return Some((wal_entry.pixel, wal_entry.timestamp));
            } else {
                // Pixel not found
                return None;
            }
        }
        None
    }
    
    fn iterate_pixels(&self) -> Result<Box<dyn Iterator<Item = Pixel> + Send>, Error> {
        // Iterate over all pixels in the B-tree index and data store
        let btree_iter = self.btree_index.iter().map_err(|e| Error::BTreeIndexError { inner: e })?;
        let data_store_clone = self.data_store.clone();

        let iter = btree_iter.filter_map(move |(_key, address)| {
            let pointer:Pointer<PixelEntry> = Pointer::from_address(address, data_store_clone.clone());
            match pointer.deref() {
                Ok(entry) => Some(entry.pixel),
                Err(e) => {
                    error!("Failed to deref pixel pointer during iteration: {}", e);
                    None
                }
            }
        });

        Ok(Box::new(iter))
    }
}