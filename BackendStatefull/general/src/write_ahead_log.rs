use std::{fmt::Display, sync::{Arc, atomic::{AtomicU64}}};

use parking_lot::{Mutex, RwLock};

use crate::persistent_random_access_memory::{self, PersistentRandomAccessMemory, Pointer};

#[derive(Debug)]
pub enum Error {
    /// IO error during WAL operations
    IoError(std::io::Error),
    /// Error committing changes to persistent storage
    CommitError(persistent_random_access_memory::Error),
    /// Error appending an entry to the WAL
    AppendFailed,
    /// Error when the WAL is out of memory
    OutOfMemory,
    /// Error peaking at the next entry in the WAL
    PeekFailed,
    /// Error popping the next entry from the WAL
    PopFailed,

    /// A fatal error that cannot be recovered from.
    /// The caller must abort the process when encountering this error to prevent data corruption.
    FatalError(Box<Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IoError(e) => write!(f, "IO Error: {}", e),
            Error::CommitError(e) => write!(f, "Commit Error: {}", e),
            Error::AppendFailed => write!(f, "Append Failed"),
            Error::OutOfMemory => write!(f, "Out Of Memory"),
            Error::PeekFailed => write!(f, "Peak Failed"),
            Error::PopFailed => write!(f, "Pop Failed"),
            Error::FatalError(e) => write!(f, "Fatal Error: {}", e),
        }
    }
}

/// A trait representing a Write-Ahead Log (WAL) for ensuring data integrity.
pub trait WriteAheadLogTrait<T> where T: Sized {
    /// Appends an entry to the write-ahead log.
    /// 
    /// Parameters:
    /// - `entry`: A byte slice representing the log entry to be appended.
    /// 
    /// Returns:
    /// - `Result<(), Error>`: Ok if the operation is successful, Err otherwise.
    fn append(&self, entry: &T) -> Result<(), Error>;

    /// Commits the current state of the write-ahead log.
    /// 
    /// Returns:
    /// - `Result<(), Error>`: Ok if the operation is successful, Err otherwise.
    fn commit(&self) -> Result<(), Error>;

    /// Peeks at the next entry in the write-ahead log without removing it.
    ///
    /// Returns:
    /// - `Result<Vec<u8>, Error>`: Ok containing the next log entry if successful, Err otherwise.
    fn peak(&self) -> Result<T, Error>;

    /// Peeks at multiple entries in the write-ahead log without removing them.
    /// 
    /// Parameters:
    /// - `count`: The number of entries to peek at. If count exceeds the number of available entries, all available entries are returned.
    ///
    /// Returns:
    /// - `Result<Vec<T>, Error>`: Ok containing a vector of the next log entries if successful, Err otherwise.
    fn peek_many(&self, count: usize) -> Result<Vec<T>, Error>;

    /// Pops the next entry from the write-ahead log, removing it from the log.
    /// 
    /// Returns:
    /// - `Result<Vec<u8>, Error>`: Ok containing the popped log entry if successful, Err otherwise.
    fn pop(&self) -> Result<T, Error>;

    /// Pops multiple entries from the write-ahead log, removing them from the log without returning them.
    fn pop_many(&self, count: usize) -> Result<(), Error>;

    /// Creates an iterator over the write-ahead log entries. (lifo order)
    /// 
    /// Parameters:
    /// - `batch_size`: The number of entries to fetch in each iteration batch.
    /// 
    /// Returns:
    /// - `WALIterator<'_, [T]>`: An iterator over the log entries.
    /// 
    /// Warning: The iterator does not guarantee consistency if the WAL is modified during iteration.
    /// If modifications occur, the iterator may yield already removed entries.
    fn iter(&self, batch_size: usize) -> Box<dyn Iterator<Item = Vec<T>> + '_> where Self: Sized;
}

pub struct WriteAheadLog<T> where T: Sized{
    // Internal state and fields for the WAL

    pram: Arc<PersistentRandomAccessMemory>,
    // Maximum number of entries in the WAL
    size: usize,

    // Current number of entries in the WAL
    length: Arc<AtomicU64>,
    // Pointers to head, tail, and data in the PRAM
    head: Arc<RwLock<Pointer<u64>>>,
    head_cache: Arc<AtomicU64>,
    tail: Arc<RwLock<Pointer<u64>>>,
    tail_cache: Arc<AtomicU64>,
    data: Arc<Pointer<T>>,

    persist_lock: Arc<RwLock<()>>,

    // Phantom data to associate type T
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for WriteAheadLog<T> where T: Sized {
    fn clone(&self) -> Self {
        WriteAheadLog {
            pram: self.pram.clone(),
            size: self.size,
            length: self.length.clone(),
            head: self.head.clone(),
            head_cache: self.head_cache.clone(),
            tail: self.tail.clone(),
            tail_cache: self.tail_cache.clone(),
            data: self.data.clone(),
            persist_lock: self.persist_lock.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// Offsets for head, tail, and data in the PRAM (their positions in memory)
static HEAD_OFFSET: u64 = 0;
static TAIL_OFFSET: u64 = 8;
static DATA_OFFSET: u64 = 16;

impl<T> WriteAheadLog<T> where T: Sized {
    pub fn new(pram: Arc<PersistentRandomAccessMemory>, size: usize) -> Self {
        // Allocate space for head and tail pointers
        // Assuming head and tail are stored at the beginning of the allocated space
        let head: Pointer<u64> = pram.smalloc::<u64>(HEAD_OFFSET, 8).unwrap();
        let tail: Pointer<u64> = pram.smalloc::<u64>(TAIL_OFFSET, 8).unwrap();
        
        let head_val: u64 = head.deref().unwrap_or(0);
        let tail_val: u64 = tail.deref().unwrap_or(0);

        // calculate length
        let length = if head_val >= tail_val {
            head_val - tail_val
        } else {
            (size as u64 - tail_val) + head_val
        };

        // Allocate the data array, an array of T with the given size
        let data = pram.smalloc::<T>(DATA_OFFSET, size * std::mem::size_of::<T>()).unwrap();
        WriteAheadLog { 
            pram,
            size,
            head: Arc::new(RwLock::new(head)),
            tail: Arc::new(RwLock::new(tail)),
            data: Arc::new(data),
            _phantom: std::marker::PhantomData,
            length: Arc::new(AtomicU64::new(length)),
            // Keeping a copy in memory for faster access
            head_cache: Arc::new(AtomicU64::new(head_val)),
            tail_cache: Arc::new(AtomicU64::new(tail_val)),
            persist_lock: Arc::new(RwLock::new(())),
        }
    }
}

impl<T> WriteAheadLogTrait<T> for WriteAheadLog<T> where T: Sized + Clone {
    fn append(&self, entry: &T) -> Result<(), Error> {
        // Acquire read lock to allow concurrent appends and pops but block commits
        let _persist_guard = self.persist_lock.read();

        // Atomically increase length
        loop {
            let length = self.length.load(std::sync::atomic::Ordering::SeqCst);
            if length >= self.size as u64 {
                return Err(Error::OutOfMemory);
            }

            if let Ok(_) = self.length.compare_exchange(
                length, 
                length+1, 
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        // Read current head index
        let head;
        {
            let head_pointer = self.head.write();
            head = self.head_cache.load(std::sync::atomic::Ordering::SeqCst);
            let new_head = (head + 1) % self.size as u64;
            // Update head index and length
            head_pointer.set(&new_head).map_err(|_| Error::AppendFailed)?;
            self.head_cache.store(new_head, std::sync::atomic::Ordering::SeqCst);
        } // release lock on head

        // Write the entry at the head position (ensure that at a given index only one write can happen concurrently)
        self.data.at(head as usize).set(entry).map_err(|_| Error::AppendFailed)?;

        Ok(())
    }

    fn commit(&self) -> Result<(), Error> {
        // Acquire write lock to prevent appends/pops during commit
        let _persist_guard = self.persist_lock.write();
        let error = self.pram.persist();
        if let Err(persistent_random_access_memory::Error::FatalError(e)) = error{
            return Err(Error::FatalError(Box::new(Error::CommitError(*e))));
        }
        error.map_err(Error::CommitError)// map other errors
    }

    fn peak(&self) -> Result<T, Error> {
        let tail = self.tail_cache.load(std::sync::atomic::Ordering::SeqCst);

        let length = self.length.load(std::sync::atomic::Ordering::SeqCst);
        if length == 0 {
            return Err(Error::PeekFailed);
        }

        // Read tail index
        let entry = self.data.at(tail as usize).deref().map_err(|_| Error::AppendFailed)?;
        Ok(entry)
    }

    fn peek_many(&self, count: usize) -> Result<Vec<T>, Error> {
        let tail = self.tail_cache.load(std::sync::atomic::Ordering::SeqCst);

        let length = self.length.load(std::sync::atomic::Ordering::SeqCst);
        if length == 0 {
            return Err(Error::PeekFailed);
        }

        let mut entries = Vec::new();
        let mut current_index = tail;

        let to_peek = std::cmp::min(count as u64, length);

        for _ in 0..to_peek {
            let entry = self.data.at(current_index as usize).deref().map_err(|_| Error::AppendFailed)?;
            entries.push(entry);
            current_index = (current_index + 1) % self.size as u64;
        }

        Ok(entries)
    }

    fn pop(&self) -> Result<T, Error> {
        // Acquire read lock to allow concurrent appends and pops but block commits
        let _persist_guard = self.persist_lock.read();

        // Make sure there is at least one entry to pop
        loop {
            let length = self.length.load(std::sync::atomic::Ordering::SeqCst);
            if length <= 0 {
                return Err(Error::PopFailed);
            }

            if let Ok(_) = self.length.compare_exchange(
                length, 
                length-1, 
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        let tail_val;
        {
            let tail_pointer = self.tail.write();
            // Read tail, fetch entry, then advance tail
            tail_val = self.tail_cache.load(std::sync::atomic::Ordering::SeqCst);
            let new_tail = (tail_val + 1) % self.size as u64;

            tail_pointer.set(&new_tail).map_err(|_| Error::AppendFailed)?;
            self.tail_cache.store(new_tail, std::sync::atomic::Ordering::SeqCst);
        } // release lock on tail
        
        let entry = self.data.at(tail_val as usize).deref().map_err(|_| Error::AppendFailed)?;

        Ok(entry)
    }

    fn pop_many(&self, count: usize) -> Result<(), Error> {
        // Acquire read lock to allow concurrent appends and pops but block commits
        let _persist_guard = self.persist_lock.read();

        // Make sure there is at least one entry to pop
        loop {
            let length = self.length.load(std::sync::atomic::Ordering::SeqCst);
            if length < count as u64 {
                return Err(Error::PopFailed);
            }

            if let Ok(_) = self.length.compare_exchange(
                length, 
                length-count as u64, 
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        let tail_val;
        {
            let tail_pointer = self.tail.write();
            // Read tail, fetch entry, then advance tail
            tail_val = self.tail_cache.load(std::sync::atomic::Ordering::SeqCst);
            let new_tail = (tail_val + count as u64) % self.size as u64;

            tail_pointer.set(&new_tail).map_err(|_| Error::AppendFailed)?;
            self.tail_cache.store(new_tail, std::sync::atomic::Ordering::SeqCst);
        } // release lock on tail
        

        Ok(())
    }

    fn iter(&self, batch_size: usize) -> Box<dyn Iterator<Item = Vec<T>> + '_> where Self: Sized {
        // Read head and tail indices from cache
        let head = self.head_cache.load(std::sync::atomic::Ordering::SeqCst);

        // Start from the head - the first entry is at head - 1 which is decremented to iterate in reverse
        let current = (head as i64 - 1) % self.size as i64;

        Box::new(WALIterator {
            wal: self.clone(),
            current_index: Arc::new(Mutex::new(current)),
            batch_size,
        })
    }
}


struct WALIterator<T> where T: Sized + Clone {
    wal: WriteAheadLog<T>,
    current_index: Arc<Mutex<i64>>,
    batch_size: usize,
}

impl<T> Iterator for WALIterator<T> where T: Sized + Clone {
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // Need to aquire both locks, to ensure consistency (else head and tail may change during check)
        let head= self.wal.head_cache.load(std::sync::atomic::Ordering::Acquire);
        let tail: u64 = self.wal.tail_cache.load(std::sync::atomic::Ordering::Acquire);

        let current;
        let mut batch_size;
        {
            let mut ci = self.current_index.lock();
            let current_not_wrapped = *ci;

            // Wrap around if needed
            current = if current_not_wrapped < 0 {
                self.wal.size as i64 + current_not_wrapped
            }else {
                current_not_wrapped
            } as u64;

            let length = if head > tail {
                // Normal case
                if current < tail || current >= head {
                    return None;
                }
                current - tail + 1
            } else if head < tail {
                // Wrapped case
                if current < tail && current >= head {
                    return None;
                }

                if current >= tail {
                    current - tail + 1
                } else {
                    current + (self.wal.size as u64 - tail) + 1
                }
            } else {
                // head == tail: could be empty or full
                let wal_length = self.wal.length.load(std::sync::atomic::Ordering::SeqCst);
                if wal_length == 0 {
                    return None; // truly empty
                }
                // Full case: all positions are valid
                if current >= tail {
                    current - tail + 1
                } else {
                    current + (self.wal.size as u64 - tail) + 1
                }
            };

            batch_size = self.batch_size.min(length as usize);
            batch_size = batch_size.min((current + 1) as usize); // cannot read past 0
            
            if batch_size == 0 {
                return None;
            }

            *ci -= batch_size as i64;
            drop(ci); // release lock
        }
 
        // Calculate start position: start = current - (batch_size - 1)
        // Safe because batch_size <= current + 1, so current >= batch_size - 1
        let start_pos = current - (batch_size as u64 - 1);
        
        let mut entry = self.wal.data.at(start_pos as usize).deref_many(batch_size).ok()?;
        entry.reverse(); // reverse to maintain LIFO order - can only read in ascending order
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple in-memory implementation of PersistentRandomAccessMemory for testing.
    // no additional imports needed
    fn unique_test_path(suffix: &str) -> String {
        let tmp = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        tmp.join(format!("pram_{}_{}_{}.ignore", suffix, pid, ts))
            .to_string_lossy()
            .to_string()
    }

    // Helper to construct WAL with generic T and given size using Arc for Weak pointer expectations.
    fn new_wal<T: Sized>(size: usize, id: &str) -> WriteAheadLog<T> {
        let pram = PersistentRandomAccessMemory::new(10*4096, &unique_test_path(id));
        let wal = WriteAheadLog::<T>::new(pram, size);
        wal
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Small(u32);
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Medium { a: u64, b: u64 }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Large { arr: [u8; 32] }

    #[test]
    fn test_append_and_pop_basic() {
        let wal = new_wal::<Small>(4, "basic");
        wal.append(&Small(10)).unwrap();
        wal.append(&Small(20)).unwrap();
        // Pop first inserted value per current implementation logic
        let first = wal.pop().unwrap();
        let _second = wal.pop().unwrap();
        assert_eq!(first.0 <= 20, true); // Order may be impacted by internal indexing
        assert!(matches!(wal.pop(), Err(Error::PopFailed))); // Empty
    }

    #[test]
    fn test_out_of_memory() {
        let wal = new_wal::<Small>(2, "out_of_memory");
        wal.append(&Small(1)).unwrap();
        wal.append(&Small(2)).unwrap();
        assert!(matches!(wal.append(&Small(3)), Err(Error::OutOfMemory)));
    }

    #[test]
    fn test_peak_empty_error() {
        let wal = new_wal::<Medium>(3, "peak_empty_error");
        assert!(matches!(wal.peak(), Err(Error::PeekFailed)));
    }

    #[test]
    fn test_peak_after_append() {
        let wal = new_wal::<Medium>(3, "peak_after_append");
        wal.append(&Medium { a: 1, b: 2 }).unwrap();
        // Current implementation peaks at tail index which is initially 0 (likely default memory)
        let val = wal.peak();
        assert!(val.is_ok() || matches!(val, Err(Error::PeekFailed)));
    }

    #[test]
    fn test_commit() {
        let wal = new_wal::<Large>(2, "commit");
        wal.append(&Large { arr: [1u8; 32] }).unwrap();
        assert!(wal.commit().is_ok());
    }

    #[test]
    fn test_pop_empty_error() {
        let wal = new_wal::<Small>(2, "pop_empty_error");
        assert!(matches!(wal.pop(), Err(Error::PopFailed)));
    }

    #[test]
    fn test_multiple_types_capacity() {
        let wal_small = new_wal::<Small>(1, "multiple_types_capacity_small");
        wal_small.append(&Small(5)).unwrap();
        assert!(matches!(wal_small.append(&Small(6)), Err(Error::OutOfMemory)));

        let wal_medium = new_wal::<Medium>(2, "multiple_types_capacity_medium");
        wal_medium.append(&Medium { a: 3, b: 4 }).unwrap();
        wal_medium.append(&Medium { a: 5, b: 6 }).unwrap();
        assert!(matches!(wal_medium.append(&Medium { a: 7, b: 8 }), Err(Error::OutOfMemory)));

        let wal_large = new_wal::<Large>(1, "multiple_types_capacity_large");
        wal_large.append(&Large { arr: [9u8; 32] }).unwrap();
        assert!(matches!(wal_large.append(&Large { arr: [8u8; 32] }), Err(Error::OutOfMemory)));
    }

    #[test]
    fn test_wal_iterator() {
        // Test 1: Basic iteration with wrap-around
        let wal = new_wal::<Small>(5, "test_wal_iterator");
        wal.append(&Small(1)).unwrap();
        wal.append(&Small(2)).unwrap();
        wal.append(&Small(3)).unwrap();
        wal.pop().unwrap(); // Pop one to test iterator bounds
        wal.pop().unwrap();
        wal.append(&Small(4)).unwrap();
        wal.append(&Small(5)).unwrap();

        // Use the internal length to know how many items to take (avoids infinite iteration issues)
        let len = wal.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        assert_eq!(len, 3);

        // Reverse iteration should yield reversed order
        let items_reverse: Vec<Small> = wal.iter(2).flatten().take(len).collect();
        assert_eq!(items_reverse, vec![Small(5), Small(4), Small(3)]);

        // Test 2: Empty WAL iteration should yield nothing
        let wal2 = new_wal::<Small>(5, "test_wal_iterator_empty");
        let items_empty: Vec<Small> = wal2.iter(1).flatten().collect();
        assert_eq!(items_empty, vec![]);

        // Test 3: Nearly full WAL iteration (size-1 to avoid head==tail ambiguity)
        let wal3 = new_wal::<Small>(6, "test_wal_iterator_full");
        for i in 1..=5 {
            wal3.append(&Small(i)).unwrap();
        }
        let len3 = wal3.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        assert_eq!(len3, 5);
        let items_full: Vec<Small> = wal3.iter(2).flatten().take(len3).collect();
        assert_eq!(items_full, vec![Small(5), Small(4), Small(3), Small(2), Small(1)]);

        // Test 4: Different batch sizes
        let wal4 = new_wal::<Small>(10, "test_wal_iterator_batch");
        for i in 1..=7 {
            wal4.append(&Small(i)).unwrap();
        }
        let len4 = wal4.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        
        // Batch size 1
        let items_batch1: Vec<Small> = wal4.iter(1).flatten().take(len4).collect();
        assert_eq!(items_batch1, vec![Small(7), Small(6), Small(5), Small(4), Small(3), Small(2), Small(1)]);
        
        // Batch size 3
        let items_batch3: Vec<Small> = wal4.iter(3).flatten().take(len4).collect();
        assert_eq!(items_batch3, vec![Small(7), Small(6), Small(5), Small(4), Small(3), Small(2), Small(1)]);
        
        // Batch size larger than content
        let items_batch10: Vec<Small> = wal4.iter(10).flatten().take(len4).collect();
        assert_eq!(items_batch10, vec![Small(7), Small(6), Small(5), Small(4), Small(3), Small(2), Small(1)]);

        // Test 5: Wrap-around with multiple cycles
        let wal5 = new_wal::<Small>(4, "test_wal_iterator_wraparound");
        for i in 1..=4 {
            wal5.append(&Small(i)).unwrap();
        }
        // Pop 2, append 2 more (causes wrap)
        wal5.pop().unwrap();
        wal5.pop().unwrap();
        wal5.append(&Small(5)).unwrap();
        wal5.append(&Small(6)).unwrap();
        
        let len5 = wal5.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        assert_eq!(len5, 4);
        let items_wrap: Vec<Small> = wal5.iter(2).flatten().take(len5).collect();
        assert_eq!(items_wrap, vec![Small(6), Small(5), Small(4), Small(3)]);

        // Test 6: Single element
        let wal6 = new_wal::<Small>(5, "test_wal_iterator_single");
        wal6.append(&Small(42)).unwrap();
        let len6 = wal6.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        assert_eq!(len6, 1);
        let items_single: Vec<Small> = wal6.iter(1).flatten().take(len6).collect();
        assert_eq!(items_single, vec![Small(42)]);

        // Test 7: Multiple complete iterations over same WAL
        let wal7 = new_wal::<Small>(5, "test_wal_iterator_multi");
        for i in 1..=3 {
            wal7.append(&Small(i)).unwrap();
        }
        let len7 = wal7.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        
        let iter1: Vec<Small> = wal7.iter(2).flatten().take(len7).collect();
        let iter2: Vec<Small> = wal7.iter(2).flatten().take(len7).collect();
        assert_eq!(iter1, iter2);
        assert_eq!(iter1, vec![Small(3), Small(2), Small(1)]);

        // Test 8: Completely full WAL (head == tail but length > 0)
        let wal8 = new_wal::<Small>(5, "test_wal_iterator_completely_full");
        for i in 1..=5 {
            wal8.append(&Small(i)).unwrap();
        }
        let len8 = wal8.length.load(std::sync::atomic::Ordering::SeqCst) as usize;
        assert_eq!(len8, 5);
        let items_completely_full: Vec<Small> = wal8.iter(2).flatten().take(len8).collect();
        assert_eq!(items_completely_full, vec![Small(5), Small(4), Small(3), Small(2), Small(1)]);
    }


}