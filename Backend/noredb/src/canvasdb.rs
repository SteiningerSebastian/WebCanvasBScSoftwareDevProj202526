use std::{path, sync::Arc};

use general::{persistent_random_access_memory::{PersistentRandomAccessMemory, Pointer}, pram_btree_index::{BTreeIndex, BTreeIndexPRAM}, write_ahead_log::{self, WriteAheadLog, WriteAheadLogTrait}};
use tracing::warn;

#[repr(C)]
pub struct TimeStamp {}

#[repr(C)]
pub struct Pixel{
    x: u16,
    y: u16,
    color: [u8; 3], // RGB
}

trait CanvasDBTrait {
    fn set_pixel(&mut self, pixel: Pixel);
    fn get_pixel(&self, x: u16, y: u16) -> Option<(Pixel, TimeStamp)>;
    fn clear_canvas(&mut self);
}

struct CanvasDBPRAM {
    // Fields representing the state of the CanvasDB
    /// Write-ahead log for recording pixel changes
    write_ahead_log: Arc<WriteAheadLog<Pixel>>,

    /// B-tree index for efficient pixel lookup
    btree_index: Arc<BTreeIndexPRAM>,

    /// Persistent random access memory for storing pixel data
    data_store: Arc<PersistentRandomAccessMemory>,

    
}

impl CanvasDBPRAM {
    pub fn new(
        width: u16,
        height: u16,
        path: &str,
        write_ahead_log_size: usize,
    ) -> Self {
        // +1024 for any necessary pointers, implementation overhead, etc.
        let wal_pram = PersistentRandomAccessMemory::new(write_ahead_log_size*std::mem::size_of::<Pixel>() + 1024,path);
        // *32 for each btree entry there is a u64 (8bytes) key a u64 (8bytes) value and some overhead (+1024bytes and *32 not *16)
        let btree_pram = PersistentRandomAccessMemory::new((width as usize * height as usize * 32) + 1024, path);
        let data_store = PersistentRandomAccessMemory::new((width as usize * height as usize * std::mem::size_of::<Pixel>()) + 1024, path);

        // Initialize the write-ahead log, B-tree index, and data store
        let write_ahead_log = Arc::new(WriteAheadLog::new(wal_pram, write_ahead_log_size));
        let btree_index = Arc::new(BTreeIndexPRAM::new(btree_pram));

        CanvasDBPRAM {
            write_ahead_log,
            btree_index,
            data_store,
        }
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
        for _ in 0..num_threads {
            let wal_clone = Arc::clone(&self.write_ahead_log);
            let btree_clone = Arc::clone(&self.btree_index);
            let data_store_clone = Arc::clone(&self.data_store);

            std::thread::spawn(move || {
                loop {
                    // Process write-ahead log entries
                    // Popping is safe here as if we don't persist the changes we can always reapply them from the log on startup
                    // We just make sure to first persist the pixel data and index before persisting the log entry
                    //
                    // TODO This is not safe yet as two threads can pop a change to the same pixel and overwrite each other
                    // when both read the index and decide to insert and not to update. 
                    // Then one insert will be lost. -> implement a get_or_set method for the btree index that atomically checks if a key exists and inserts if not.
                    if let Ok(pixel) = wal_clone.pop() {
                        // Update B-tree index and data store
                        let key = (pixel.y as u32) << 16 | (pixel.x as u32);
                        let pointer = btree_clone.get(key as u64);
                        if let Ok(pointer) = pointer {
                            // Update existing pixel
                            let pointer:Pointer<Pixel> = Pointer::from_address(pointer, data_store_clone.clone());
                            if let Err(_) = pointer.set(&pixel){
                                warn!("Failed to write new pixel data to data store.");
                                continue; // Skip to next iteration
                            }
                        } else {
                            // Insert new pixel
                            let pointer = data_store_clone.malloc::<Pixel>(std::mem::size_of::<Pixel>());
                            if let Ok(pointer) = pointer {
                                if let Err(_) = pointer.set(&pixel){
                                    warn!("Failed to write new pixel data to data store.");
                                    continue; // Skip to next iteration
                                }
                                btree_clone.set(key as u64, pointer.address as u64).unwrap();
                            } else{
                                warn!("Failed to allocate memory for new pixel in data store.");
                                continue; // Skip to next iteration
                            }
                        }
                    } else {
                        // Sleep briefly to avoid busy-waiting
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            });
        }
    }
}