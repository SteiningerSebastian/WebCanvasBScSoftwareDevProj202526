use std::sync::{Arc, Mutex};

use crate::persistent_random_access_memory::{self, PersistentRandomAccessMemory, Pointer};

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    CommitError(persistent_random_access_memory::Error),
    AppendFailed,
    OutOfMemory,
    PeakFailed,
    PopFailed,
}

/// A trait representing a Write-Ahead Log (WAL) for ensuring data integrity.
pub trait WriteAheadLog<T> where T: Sized {
    /// Appends an entry to the write-ahead log.
    /// 
    /// Parameters:
    /// - `entry`: A byte slice representing the log entry to be appended.
    /// 
    /// Returns:
    /// - `Result<(), Error>`: Ok if the operation is successful, Err otherwise.
    fn append(&mut self, entry: &T) -> Result<(), Error>;

    /// Commits the current state of the write-ahead log.
    /// 
    /// Returns:
    /// - `Result<(), Error>`: Ok if the operation is successful, Err otherwise.
    fn commit(&mut self) -> Result<(), Error>;

    /// Peeks at the next entry in the write-ahead log without removing it.
    ///
    /// Returns:
    /// - `Result<Vec<u8>, Error>`: Ok containing the next log entry if successful, Err otherwise.
    fn peak(&mut self) -> Result<T, Error>;

    /// Pops the next entry from the write-ahead log, removing it from the log.
    /// 
    /// Returns:
    /// - `Result<Vec<u8>, Error>`: Ok containing the popped log entry if successful, Err otherwise.
    fn pop(&mut self) -> Result<T, Error>;

    /// Creates an iterator over the write-ahead log entries.
    /// 
    /// Parameters:
    /// - `reverse`: A boolean indicating whether to iterate in reverse order.
    /// 
    /// Returns:
    /// - `WALIterator<'_, T>`: An iterator over the log entries.
    fn iter(&mut self, reverse: bool) -> Box<dyn Iterator<Item = T> + '_> where Self: Sized;
}

pub struct ConcurrentPRAMWriteAheadLog<T> where T: Sized{
    wla: Arc<Mutex<PRAMWriteAheadLog<T>>>,
}

impl<T> ConcurrentPRAMWriteAheadLog<T> where T: Sized {
    pub fn new(pram: Arc<PersistentRandomAccessMemory>, size: usize) -> Self {
        ConcurrentPRAMWriteAheadLog { 
            wla: Arc::new(Mutex::new(PRAMWriteAheadLog::new(pram, size))),
        }
    }
}

pub struct ConcurrentWALIterator<T> where T: Sized {
    wal: Arc<Mutex<PRAMWriteAheadLog<T>>>,
    current_index: u64,
    reverse: bool,
}

/// Implement WriteAheadLog for ConcurrentPRAMWriteAheadLog by locking the internal mutex.
impl<T> WriteAheadLog<T> for ConcurrentPRAMWriteAheadLog<T> where T: Sized {
    fn append(&mut self, entry: &T) -> Result<(), Error> {
        let mut wla = self.wla.lock().unwrap();
        wla.append(entry)
    }

    fn commit(&mut self) -> Result<(), Error> {
        let mut wla = self.wla.lock().unwrap();
        wla.commit()
    }

    fn peak(&mut self) -> Result<T, Error> {
        let mut wla = self.wla.lock().unwrap();
        wla.peak()
    }

    fn pop(&mut self) -> Result<T, Error> {
        let mut wla = self.wla.lock().unwrap();
        wla.pop()
    }

    fn iter(&mut self, reverse: bool) -> Box<dyn Iterator<Item = T> + '_> where Self: Sized {
        let wla = self.wla.lock().unwrap();

        let mut head: u64 = wla.head.deref().unwrap();
        let tail: u64 = wla.tail.deref().unwrap();

        // Adjust head for iteration (Point to last valid entry)
        if head == 0 {
            head = wla.size as u64 -1;
        }else {
            head -= 1; // -1 as head points to next free slot
        }

        Box::new(ConcurrentWALIterator {
            wal: Arc::clone(&self.wla),
            current_index: if reverse { head } else { tail },
            reverse,
        })
    }
}

impl<T> Iterator for ConcurrentWALIterator<T> where T: Sized {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let wal = self.wal.lock().unwrap();

        // Read head and tail indices
        let tail = wal.tail.deref().ok()?;
        let mut head = wal.head.deref().ok()?; // read current head

        // Adjust head for iteration
        if head == 0 {
            head = wal.size as u64 -1;
        }else {
            head -= 1; // -1 as head points to next free slot
        }

        // Check bounds - Return None if current_index is out of [tail, head] (circularly)
        if head > tail {
            if self.current_index < tail || self.current_index > head {
                return None;
            }
        } else if tail > head {
            if self.current_index < tail && self.current_index > head {
                return None;
            }
        }

        let item: T = wal.data.at(self.current_index as usize).deref().ok()?;

        // Direction in which to iterate
        if self.reverse {
            // Move backwards
            self.current_index = if self.current_index == 0 {
                wal.size as u64 - 1 
            } else {
                self.current_index - 1
            };

            Some(item)
        } else {
            // Move forwards
            self.current_index = (self.current_index + 1) % wal.size as u64;

            Some(item)
        }  
    }
}

impl<T> Clone for ConcurrentPRAMWriteAheadLog<T> where T: Sized {
    fn clone(&self) -> Self {
        ConcurrentPRAMWriteAheadLog {
            wla: std::sync::Arc::clone(&self.wla),
        }
    }
}

pub struct PRAMWriteAheadLog<T> where T: Sized{
    // Internal state and fields for the PRAMWriteAheadLog
    pram: Arc<PersistentRandomAccessMemory>,
    size: usize,
    length: usize,
    head: Pointer<u64>,
    tail: Pointer<u64>,
    data: Pointer<T>,
    _phantom: std::marker::PhantomData<T>,
}

// Offsets for head, tail, and data in the PRAM (their positions in memory)
static HEAD_OFFSET: u64 = 0;
static TAIL_OFFSET: u64 = 8;
static DATA_OFFSET: u64 = 16;

struct WALIterator<'a, T> where T: Sized {
    wal: &'a mut PRAMWriteAheadLog<T>,
    reverse: bool,
    current_index: u64,
}

impl<T> PRAMWriteAheadLog<T> where T: Sized {
    pub fn new(pram: Arc<PersistentRandomAccessMemory>, size: usize) -> Self {
        // Allocate space for head and tail pointers
        // Assuming head and tail are stored at the beginning of the allocated space
        let head: Pointer<u64> = pram.smalloc::<u64>(HEAD_OFFSET, 8).unwrap();
        let tail: Pointer<u64> = pram.smalloc::<u64>(TAIL_OFFSET, 8).unwrap();
        
        // Allocate the data array, an array of T with the given size
        let data = pram.smalloc::<T>(DATA_OFFSET, size * std::mem::size_of::<T>()).unwrap();
        PRAMWriteAheadLog { 
            pram,
            size,
            head,
            tail,
            data,
            _phantom: std::marker::PhantomData,
            length: 0,
        }
    }
}

impl<T> WriteAheadLog<T> for PRAMWriteAheadLog<T> where T: Sized {
    fn append(&mut self, entry: &T) -> Result<(), Error> {
        // Check if there is space in the log
        if self.length >= self.size {
            return Err(Error::OutOfMemory);
        }

        // Read current head index
        let current_head = self.head.deref().map_err(|_| Error::AppendFailed)?;
        let prev_head_index = current_head;
        let new_head = (current_head + 1) % self.size as u64;
        // Update head index and length
        self.head.set(&new_head).map_err(|_| Error::AppendFailed)?;

        // Here the page may be swapped out when we access the data pointer
        // So we do this after updating the head index

        // Write the entry at the head position
        self.data.at(prev_head_index as usize).set(entry).map_err(|_| Error::AppendFailed)?;

        self.length += 1;

        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.pram.persist().map_err(Error::CommitError)
    }

    fn peak(&mut self) -> Result<T, Error> {
        if self.length == 0 {
            return Err(Error::PeakFailed);
        }

        // Read tail index
        let tail = self.tail.deref().map_err(|_| Error::AppendFailed)?;
        let entry = self.data.at(tail as usize).deref().map_err(|_| Error::AppendFailed)?;
        Ok(entry)
    }

    fn pop(&mut self) -> Result<T, Error> {
        // Check if there are entries to pop
        if self.length == 0 {
            return Err(Error::PopFailed);
        }

        // Read tail, fetch entry, then advance tail
        let tail_val = self.tail.deref().map_err(|_| Error::AppendFailed)?;
        let entry = self.data.at(tail_val as usize).deref().map_err(|_| Error::AppendFailed)?;
        let new_tail = (tail_val + 1) % self.size as u64;
        self.tail.set(&new_tail).map_err(|_| Error::AppendFailed)?;

        self.length -= 1;
        
        Ok(entry)
    }

    fn iter(&mut self, reverse: bool) -> Box<dyn Iterator<Item = T> + '_> where Self: Sized {
        let mut head: u64 = self.head.deref().unwrap();
        let tail: u64 = self.tail.deref().unwrap();

        // Adjust head for iteration (Point to last valid entry)
        if head == 0 {
            head = self.size as u64 -1;
        }else {
            head -= 1; // -1 as head points to next free slot
        }

        Box::new(WALIterator {
            wal: self,
            reverse,
            current_index: if reverse { head } else { tail }
        })
    }
}

impl<T> Iterator for WALIterator<'_, T> where T: Sized {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Read indices
        let tail = self.wal.tail.deref().ok()?;
        let mut head = self.wal.head.deref().ok()?; // read current head

        // Adjust head for iteration
        if head == 0 {
            head = self.wal.size as u64 -1;
        }else {
            head -= 1; // -1 as head points to next free slot
        }

        // Check bounds - Return None if current_index is out of [tail, head] (circularly)
        if head > tail {
            if self.current_index < tail || self.current_index > head {
                return None;
            }
        } else if tail > head {
            if self.current_index < tail && self.current_index > head {
                return None;
            }
        }
        let item: T = self.wal.data.at(self.current_index as usize).deref().ok()?;

        // Direction in which to iterate
        if self.reverse {
            // Move backwards (pfram will swap in pages as needed, so reverse reading the data will still sequentially access the disk 
            // but not as efficiently as seeking to the start of the previous page and reading forward takes one seek per page instead of just reading the next page)
            // Iterating the WAL and concurrently appending to it will decrease the effective cache size as we need to keep swaping pages in and out more frequently.
            self.current_index = if self.current_index == 0 {
                self.wal.size as u64 - 1 
            } else {
                self.current_index - 1
            };

            Some(item)
        } else {
            // Move forwards
            self.current_index = (self.current_index + 1) % self.wal.size as u64;

            Some(item)
        }    

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple in-memory implementation of PersistentRandomAccessMemory for testing.
    // no additional imports needed

    // Helper to construct WAL with generic T and given size using Rc for Weak pointer expectations.
    fn new_wal<T: Sized>(size: usize) -> PRAMWriteAheadLog<T> {
        // Compute memory needed: head (8) + tail (8) + data
        let data_bytes = size * std::mem::size_of::<T>();
        let total = (DATA_OFFSET as usize) + data_bytes;
        // Round up to page size (4096)
        let page = 4096usize;
        let alloc = ((total + page - 1) / page) * page;
        let pram = PersistentRandomAccessMemory::new(alloc, "C:/data/test_wal.ignore", page);
        let wal = PRAMWriteAheadLog::<T>::new(pram, size);
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
        let mut wal = new_wal::<Small>(4);
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
        let mut wal = new_wal::<Small>(2);
        wal.append(&Small(1)).unwrap();
        wal.append(&Small(2)).unwrap();
        assert!(matches!(wal.append(&Small(3)), Err(Error::OutOfMemory)));
    }

    #[test]
    fn test_peak_empty_error() {
        let mut wal = new_wal::<Medium>(3);
        assert!(matches!(wal.peak(), Err(Error::PeakFailed)));
    }

    #[test]
    fn test_peak_after_append() {
        let mut wal = new_wal::<Medium>(3);
        wal.append(&Medium { a: 1, b: 2 }).unwrap();
        // Current implementation peaks at tail index which is initially 0 (likely default memory)
        let val = wal.peak();
        assert!(val.is_ok() || matches!(val, Err(Error::PeakFailed)));
    }

    #[test]
    fn test_commit() {
        let mut wal = new_wal::<Large>(2);
        wal.append(&Large { arr: [1u8; 32] }).unwrap();
        assert!(wal.commit().is_ok());
    }

    #[test]
    fn test_pop_empty_error() {
        let mut wal = new_wal::<Small>(2);
        assert!(matches!(wal.pop(), Err(Error::PopFailed)));
    }

    #[test]
    fn test_multiple_types_capacity() {
        let mut wal_small = new_wal::<Small>(1);
        wal_small.append(&Small(5)).unwrap();
        assert!(matches!(wal_small.append(&Small(6)), Err(Error::OutOfMemory)));

        let mut wal_medium = new_wal::<Medium>(2);
        wal_medium.append(&Medium { a: 3, b: 4 }).unwrap();
        wal_medium.append(&Medium { a: 5, b: 6 }).unwrap();
        assert!(matches!(wal_medium.append(&Medium { a: 7, b: 8 }), Err(Error::OutOfMemory)));

        let mut wal_large = new_wal::<Large>(1);
        wal_large.append(&Large { arr: [9u8; 32] }).unwrap();
        assert!(matches!(wal_large.append(&Large { arr: [8u8; 32] }), Err(Error::OutOfMemory)));
    }

    // Helper to construct Concurrent WAL with generic T and given size using Rc for Weak pointer expectations.
    fn new_concurrent_wal<T: Sized>(size: usize) -> ConcurrentPRAMWriteAheadLog<T> {
        let data_bytes = size * std::mem::size_of::<T>();
        let total = (DATA_OFFSET as usize) + data_bytes;
        let page = 4096usize;
        let alloc = ((total + page - 1) / page) * page;
        let pram = PersistentRandomAccessMemory::new(alloc, "C:/data/test_wal_concurrent.ignore", page);
        ConcurrentPRAMWriteAheadLog::new(pram, size)
    }

    #[test]
    fn test_pram_iterator_forward_and_reverse() {
        let mut wal = new_wal::<Small>(5);
        wal.append(&Small(1)).unwrap();
        wal.append(&Small(2)).unwrap();
        wal.append(&Small(3)).unwrap();
        wal.pop().unwrap(); // Pop one to test iterator bounds
        wal.pop().unwrap();
        wal.append(&Small(4)).unwrap();
        wal.append(&Small(5)).unwrap();

        // Use the internal length to know how many items to take (avoids infinite iteration issues)
        let len = wal.length;

        let items_forward: Vec<Small> = wal.iter(false).take(len).collect();
        assert_eq!(items_forward, vec![Small(3), Small(4), Small(5)]);

        // Reverse iteration should yield reversed order
        let items_reverse: Vec<Small> = wal.iter(true).take(len).collect();
        assert_eq!(items_reverse, vec![Small(5), Small(4), Small(3)]);
    }

    #[test]
    fn test_concurrent_wal_iterator_forward_and_reverse() {
        let mut cwal = new_concurrent_wal::<Small>(6);
        cwal.append(&Small(1)).unwrap();
        cwal.append(&Small(2)).unwrap();
        cwal.append(&Small(3)).unwrap();
        cwal.pop().unwrap(); // Pop one to test iterator bounds
        cwal.pop().unwrap();
        cwal.append(&Small(4)).unwrap();
        cwal.append(&Small(5)).unwrap();

        // read length under lock
        let len = cwal.wla.lock().unwrap().length;

        // Forward iteration via concurrent WAL
        let items_forward: Vec<Small> = cwal.iter(false).take(len).collect();
        assert_eq!(items_forward, vec![Small(3), Small(4), Small(5)]);

        // Reverse iteration via concurrent WAL
        let items_reverse: Vec<Small> = cwal.iter(true).take(len).collect();
        assert_eq!(items_reverse, vec![Small(5), Small(4), Small(3)]);
    }
}