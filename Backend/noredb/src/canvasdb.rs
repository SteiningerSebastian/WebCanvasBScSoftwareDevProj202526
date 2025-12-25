use std::{error, sync::{Arc, Mutex}};

use general::{keyed_lock::{self, KeyedLock}, persistent_random_access_memory::{PersistentRandomAccessMemory, Pointer}, pram_btree_index::{BTreeIndex, BTreeIndexPRAM}, write_ahead_log::{self, WriteAheadLog, WriteAheadLogTrait}};
use tracing::{debug, error, info};

const PERSIST_BATCH_SIZE: usize = 8192; // Number of entries to process in each batch by worker threads.
const PERSIST_LATENCY_MS: u64 = 100; // Time to wait before persisting the write-ahead log and calling listeners. This is the maximum time a listener will wait before being called.

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeStamp {
    pub bytes: [u8; 16], // 16-byte timestamp representation
}

#[repr(C)]
pub struct Pixel{
    pub key: u32, // unique key for pixel (derived from x,y)
    pub color: [u8; 3], // RGB
}

#[repr(C)]
pub struct PixelEntry {
    pub pixel: Pixel,
    pub timestamp: TimeStamp,
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
}

impl CanvasDB {
    pub fn new(
        width: u16,
        height: u16,
        path: &str,
        write_ahead_log_size: usize,
    ) -> Self {
        // +1024 for any necessary pointers, implementation overhead, etc.
        let path = format!("{}.wal.pram", path);
        let wal_pram = PersistentRandomAccessMemory::new(write_ahead_log_size*std::mem::size_of::<PixelEntry>() + 1024,&path);
        // *32 for each btree entry there is a u64 (8bytes) key a u64 (8bytes) value and some overhead (+1024bytes and *32 not *16)
        let path = format!("{}.index.pram", path);
        let btree_pram = PersistentRandomAccessMemory::new((width as usize * height as usize * 32) + 1024, &path);
        let path = format!("{}.store.pram", path);
        let data_store = PersistentRandomAccessMemory::new((width as usize * height as usize * std::mem::size_of::<PixelEntry>()) + 1024, &path);

        // Initialize the write-ahead log, B-tree index, and data store
        let write_ahead_log = Arc::new(WriteAheadLog::new(wal_pram, write_ahead_log_size));
        let btree_index = Arc::new(BTreeIndexPRAM::new(btree_pram));

        CanvasDB {
            write_ahead_log,
            btree_index,
            data_store,
            listeners: Arc::new(Mutex::new(Vec::new())),
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
    fn set_pixel_if_newer(old_pixel: &Pointer<PixelEntry>, new_pixel: &PixelEntry) -> Result<(), Box<dyn error::Error>> {
        // Get current timestamp
        let entry = old_pixel.deref().map_err(|e| format!("Failed to deref pixel pointer: {}", e))?;
        if new_pixel.timestamp > entry.timestamp {
            // Update pixel data and timestamp
            old_pixel.set(&new_pixel).map_err(|e| format!("Failed to set new pixel data: {}", e))?;
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
    fn atomic_update_pixel(pixel: &PixelEntry, pointer: &Pointer<PixelEntry>, keyed_lock: &Arc<KeyedLock>) -> Result<(), Box<dyn error::Error>> {
        let key = pixel.pixel.key;

        // Aquire lock for this pixel key to ensure no concurrent updates as multiple raw pointers could exist to the same pixel entry.
        let lock_guard = keyed_lock.lock(key as usize);
        if lock_guard.is_err() {
            error!("Failed to acquire lock for pixel key: {}", key);
            panic!("Failed to acquire lock for pixel key: {}", key);
        }
    
        let lock_guard = lock_guard.unwrap();
        // aquire write access to the pixel data
        let _write_guard = lock_guard.write();
        Self::set_pixel_if_newer(pointer, pixel).unwrap();
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

        std::thread::spawn(move || {
            // Periodically call listeners after pixels have been persisted
            loop {
                std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));

                // Call all listeners
                let v_listeners: Vec<Box<dyn FnOnce() + Send>> = {
                    let mut listeners = listeners_clone.lock().unwrap();
                    listeners.drain(0..).collect()
                };

                // Commit the write-ahead log to ensure all entries are persisted.
                wal_clone.commit().unwrap();

                let n = v_listeners.len();

                // Call each listener -> data has been persisted
                for listener in v_listeners {
                    listener();
                }

                debug!("Called {} listeners after persisting write-ahead log.", n);
            }
        });

        let wal_clone = Arc::clone(&self.write_ahead_log);
        let btree_clone = Arc::clone(&self.btree_index);
        let data_store_clone = Arc::clone(&self.data_store);

        let (tx_work, rx_work) = crossbeam::channel::unbounded::<PixelEntry>();
        let (tx_res, rx_res) = std::sync::mpsc::channel::<()>();

        // Start a separate thread for periodic persistence, datastructures handle their own consistency.
        std::thread::spawn(move || {
            // Periodically persist data structures to disk
            loop {
                // Load the next batch of entries from the write-ahead log
                // TODO: Handle errors here instead of panicking.
                let entries = wal_clone.peek_many(PERSIST_BATCH_SIZE);
                if let Err(write_ahead_log::Error::PeakFailed) = entries {
                    error!("Failed to peek entries from write-ahead log.");
                    // Wait before trying again
                    std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));
                    continue; // try again in next iteration
                }
                let entries = entries.unwrap();

                let number_of_entries = entries.len();
                if entries.is_empty() {
                    // No entries to process, sleep for a while
                    std::thread::sleep(std::time::Duration::from_millis(PERSIST_LATENCY_MS));
                    continue;
                }

                // Process each entry in the batch
                for _entry in entries {
                    // Send entry to worker threads for processing
                    // TODO: Handle errors here instead of panicking.
                    tx_work.send(_entry).unwrap();
                }

                // Wait for all entries in the batch to be processed
                for _ in 0..number_of_entries {
                    // TODO: Handle errors here instead of panicking.
                    rx_res.recv().unwrap();
                }

                // Remove the peaked entries from the write-ahead log
                if let Err(e) = wal_clone.pop_many(number_of_entries) {
                    error!("Failed to pop entries from write-ahead log: {}", e);
                    continue; // try again in next iteration
                }

                // Persist data structures
                // If we fail here it's not a big deal as we can always recover from the WAL on startup - the data store has not been persisted and therefore we can reapply the entries on startup.
                if let Err(e) = data_store_clone.persist() {
                    error!("Failed to persist data store: {}", e);
                    continue; // try again in next iteration
                }

                // If we fail here it's not a big deal as we can always recover from the WAL on startup - the B-Tree index has not been persisted and therefore we can reapply the entries on startup.

                if let Err(e) = btree_clone.persist() {
                    error!("Failed to persist B-tree index: {}", e);
                    continue; // try again in next iteration
                }

                // Successfully persisted all data structures
                debug!("Successfully persisted data store, B-tree index, and write-ahead log.");
            }
        });

        
        let keyed_lock = Arc::new(keyed_lock::KeyedLock::new());

        // Start worker threads to process the write-ahead log and update the B-tree index and data store
        for _ in 0..num_threads {
            let btree_clone = Arc::clone(&self.btree_index);
            let data_store_clone = Arc::clone(&self.data_store);
            let keyed_lock_clone: Arc<keyed_lock::KeyedLock> = Arc::clone(&keyed_lock);
            let rx = rx_work.clone();
            let tx_res = tx_res.clone();

            std::thread::spawn(move || {
                // Instead of panicking on errors, we should try to log them and recover where possible. 
                // TODO: Only panic in unrecoverable situations.
                loop {
                    // TODO: Only panic in unrecoverable situations.
                    let pixel = rx.recv().map_err(|e| format!("Failed to receive pixel entry from channel: {}", e));
                    if let Err(e) = pixel {
                        info!("{}", e);
                        return; // Exit thread on unrecoverable error or shutdown
                    }
                    let pixel = pixel.unwrap();

                    // Update B-tree index and data store
                    let key = pixel.pixel.key;

                    // Using get here to check if the pixel already exists, this is faster than get_or_set as we avoid holding the write lock on the B-tree unnecessarily.
                    let pointer = btree_clone.get(key as u64);
                    if let Ok(pointer) = pointer {
                        // Update existing pixel
                        let pointer:Pointer<PixelEntry> = Pointer::from_address(pointer, data_store_clone.clone());
                        // TODO: add panic recovery here 
                        // Atomically update the pixel entry if the new pixel has a newer timestamp
                        Self::atomic_update_pixel(&pixel, &pointer, &keyed_lock_clone).unwrap();
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
                               
                                // TODO: add panic recovery here 
                                // Atomically update the pixel entry if the new pixel has a newer timestamp
                                Self::atomic_update_pixel(&pixel, &pointer, &keyed_lock_clone).unwrap();
                            } else {
                                // TODO: handle error properly
                                // Free the allocated memory if malloc failed
                                data_store_clone.free(new_pointer.address, length).unwrap();

                                error!("Failed to insert new pixel into B-tree index.");
                                panic!("Failed to insert new pixel into B-tree index.");
                            }                                    
                        } else{
                            error!("Failed to allocate memory for new pixel in data store.");
                            panic!("Failed to allocate memory for new pixel in data store.");
                        }
                    }
                    // Notify that the pixel has been processed
                    tx_res.send(()).unwrap();
                }
            });
        }
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
            // TODO: handle listener errors
            self.listeners.lock().unwrap().push(callback);
        }
    }

    fn get_pixel(&self, key: u32) -> Option<(Pixel, TimeStamp)> {
        // Search for the pixel in the Write-Ahead Log first - it may have more recent updates than the B-tree index and data store.
        let most_current_wal = self.write_ahead_log.iter()
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
        } else if let Err(e) = pointer_address {

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
}