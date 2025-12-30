use std::{collections::BTreeSet, fmt::Display, fs::{File, OpenOptions}, io::{self, Read, Seek, Write}, sync::{Arc, Weak, atomic::{AtomicBool, AtomicPtr, Ordering}}};
use memmap2::{MmapMut};
use parking_lot::{Mutex, RwLock};
use tracing::error;

#[derive(Debug)]
pub enum Error {
    /// Error reading from persistent memory.
    ReadError,
    /// Error writing to persistent memory.
    WriteError,
    /// Error flushing persistent memory to disk.
    FlushError,
    /// Out of memory error. Unable to allocate requested memory.
    OutOfMemoryError,
    /// Data is fragmented across pages.
    PageFragmentationError,
    /// Error while swapping files.
    SwapFileError
    {
        inner:io::Error,
        to: String,
        from: String,
    },
    /// Error reading from file.
    FileReadError(io::Error),
    /// Error opening file.
    FileOpenError{
        inner: io::Error,
        path: String,
    },
    /// Error writing to file.
    FileWriteError(io::Error),
    /// Unexpected memory allocation error.
    MemoryAllocationError,
    /// Error during synchronization of access to persistent memory.
    SynchronizationError,

    /// A fatal error that cannot be recovered from.
    /// The caller must abort the process when encountering this error to prevent data corruption.
    /// After a FatalError, all guarantees about the state of the persistent memory are void.
    FatalError(Box<Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ReadError => write!(f, "Unable to read from persistent memory."),
            Error::WriteError => write!(f, "Unable to write to persistent memory."),
            Error::FlushError => write!(f, "Unable to flush persistent memory to disk."),
            Error::FileReadError(e) => write!(f, "Unable to read file: {}", e),
            Error::OutOfMemoryError => write!(f, "Out of memory Error"),
            Error::PageFragmentationError => write!(f, "Data is fragmented across pages"),
            Error::SwapFileError { inner, from, to } => write!(f, "Error while swapping file from '{}' to '{}': {}", from, to, inner),
            Error::FileOpenError{ inner, path }  => write!(f, "Unable to open file '{}': {}", path, inner),
            Error::FileWriteError(e) => write!(f, "Unable to write to file: {}", e),
            Error::MemoryAllocationError => write!(f, "Memory Allocation Error"),
            Error::SynchronizationError => write!(f, "Synchronization Error"),
            Error::FatalError(e) => write!(f, "Fatal Error: {}", e),
        }
    }
}

/// A Pointer represents a location in persistent random access memory.
/// 
/// Warning: Be careful when using this struct as it contains a raw pointer to the memory manager.
/// Improper use can lead to undefined behavior, memory leaks, or data corruption.
/// Also make sure to synchronize access to the underlying PersistentRandomAccessMemory if used across threads.
pub struct Pointer<T> where T: Sized {
    pub address: u64,
    memory: Weak<PersistentRandomAccessMemory>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Clone for Pointer<T> where T: Sized {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            memory: self.memory.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

/// A Pointer represents a location in persistent random access memory.
/// 
/// Warning: Be careful when using this struct as it contains a raw pointer to the memory manager.
/// Improper use can lead to undefined behavior, memory leaks, or data corruption.
impl<T> Pointer<T> where T: Sized {
    /// Creates a new Pointer instance.
    ///    
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - memory: A Mutex-wrapped reference to a PersistentRandomAccessMemory instance.
    /// 
    /// Returns:
    /// - A new Pointer instance.
    pub fn new(pointer: u64, memory: Weak<PersistentRandomAccessMemory>) -> Self {
        Self { address: pointer, memory, _marker: std::marker::PhantomData}
    }

    /// Creates a Pointer from a given address and memory manager.
    /// 
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - memory: A reference-counted pointer to a PersistentRandomAccessMemory instance.
    /// 
    /// Returns:
    /// - A new Pointer instance.
    /// 
    /// Warning: The memory manager must outlive the Pointer instance to avoid dangling references.
    /// Also this will not allocate new memory (mark space as used) to allocate new use salloc on the memory.
    pub fn from_address(pointer: u64, memory: Arc<PersistentRandomAccessMemory>) -> Self {
        Self { address: pointer, memory: Arc::downgrade(&memory), _marker: std::marker::PhantomData }
    }

    /// Writes the given value of type T to the location pointed to by this Pointer.
    /// 
    /// Parameters:
    /// - value: The value of type T to be written to persistent memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn set(&self, value: &T) -> Result<(), Error> where T: Sized {
        let pram = self.memory.upgrade().ok_or(Error::MemoryAllocationError)?;
        let bytes = unsafe {
            std::slice::from_raw_parts((value as *const T) as *const u8, std::mem::size_of::<T>())
        };
        pram.write(self.address, bytes)
    }

    /// Returns a new Pointer offset by the given index of type T.
    /// 
    /// Parameters:
    /// - index: The index to offset by.
    /// - T: The type of the elements being indexed.
    /// 
    /// Returns:
    /// - A new Pointer offset by the given index.
    pub fn at(&self, index: usize) -> Pointer<T> where T: Sized { 
        Pointer {
            address: self.address + (index as u64 * std::mem::size_of::<T>() as u64),
            memory: self.memory.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Dereferences the pointer to get the value of type T.
    /// 
    /// Returns:
    /// - Result containing the value of type T on success or Error on failure.
    /// 
    /// Warning: The caller must ensure that the memory at the pointer's address is valid and properly aligned for type T.
    /// Accessing invalid memory may lead to undefined behavior.
    /// The returned value is a copy of the data at the pointer's address.
    pub fn deref(&self) -> Result<T, Error> where T: Sized {
        let pram = self.memory.upgrade().ok_or(Error::MemoryAllocationError)?;
        pram.read::<T>(self.address)
    }

    /// Dereferences the pointer to get multiple values of type T.
    /// 
    /// Returns:
    /// - Result containing a vector of values of type T on success or Error on failure.
    pub fn deref_many(&self, count: usize) -> Result<Vec<T>, Error> where T: Sized {
        let pram = self.memory.upgrade().ok_or(Error::MemoryAllocationError)?;
        let mut buf = vec![0u8; count * std::mem::size_of::<T>()];
        pram.read_exact(self.address, &mut buf)?;

        // Convert the byte buffer into a vector of T
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let start = i * std::mem::size_of::<T>();
            let end = start + std::mem::size_of::<T>();
            let t_bytes = &buf[start..end];
            let t: T = unsafe {
                std::ptr::read(t_bytes.as_ptr() as *const T)
            };
            result.push(t);
        }

        Ok(result)
    }
}

/// The page size we assume the OS to use.
const PAGE_SIZE: usize = 4096;

/// A persistent random access memory implementation that uses two files as backing storage.
/// 
/// The first file is the persistent file that holds the actual "commited" data.
/// The second file is the swap file that holds the "uncommited" changes.
/// This is necessary to ensure data atomicity in case of crashes or power failures.
/// Either all changes are persisted or none are.
pub struct PersistentRandomAccessMemory {
    // The path does not change.
    path: String,
    size: usize,

    malloc_called: Arc<AtomicBool>,

    // The order of fields is important to prevent dead-locks. All locks locking multiple fields must lock them
    // in the same order - the order they are listed below!
    free_slots: Arc<Mutex<Vec<(u64, usize)>>>, 

    // Read Write lock allows concurrent read access to same file but prevents concurrent writes to the file.
    swap_file: Arc<RwLock<File>>,
    // Memory map of the swap file. 
    // No thread-safty is needed as MmapMut allows for concurrent read and writes, as long as the bytes are different.
    // The caller is responsible to not create two pointers to the same data and write to it concurrently.
    swap_mmap: AtomicPtr<MmapMut>,
    // Locking the swap mmap for writes during persist.
    _swap_mmap_lock: Arc<RwLock<()>>,

    // Store / Remember which "logical" pages are dirty in the swap file and need to be persisted.
    // This is used to optimize the persist() function to only write dirty pages to disk.
    // The time difference between writing a whole page and just a few bytes on a page is negligible. 
    // So we reduce the complexity by just storing the pages themselfs.
    dirty_swaped_pages: Arc<RwLock<BTreeSet<usize>>>,

    // The file where the commited data is stored.
    persistent_file: Arc<Mutex<File>>,
 
    // A weak reference to oneself.
    me: Weak<PersistentRandomAccessMemory>,
}

impl PersistentRandomAccessMemory {
    /// Generates the swap file path based on the given base path.
    fn get_swap_file_path(path: &str) -> String {
        format!("{}.swap.fpram", path)
    }

    /// Generates the persistent file path based on the given base path.
    fn get_persistent_file_path(path: &str) -> String {
        format!("{}.fpram", path)
    }

    /// Creates a new FilePersistentRandomAccessMemory instance.
    /// 
    /// Parameters:
    /// - size: The total size of the persistent memory in bytes. Must be a multiple of PAGE_SIZE (4096 bytes).
    /// - path: The file path for the persistent memory storage.
    /// 
    /// 
    ///   Returns:
    /// - A atomic-reference-counted pointer to the newly created FilePersistentRandomAccessMemory instance.
    pub fn new(size: usize, path: &str) -> Arc<Self> {
        // Ensure size is a multiple of PAGE_SIZE.
        let size = size + (PAGE_SIZE - (size % PAGE_SIZE)) % PAGE_SIZE;
        assert!(size % PAGE_SIZE == 0, "Size must be a multiple of PAGE_SIZE (4096 bytes).");

        // Be aware that this is a very naive free slot management
        // and the free list will not be persisted across restarts.
        let mut free_slots = Vec::new();
        free_slots.push((0, size));
        
        let swap_file_path = Self::get_swap_file_path(path);
        let persistent_file_path = Self::get_persistent_file_path(path);

        // If the .tmp file exists, something went wrong during the last persist operation.
        // The .tmp file is only created during persist and renamed back to .swap.fpram after persisting.
        let tmp_file_path = format!("{}.tmp", persistent_file_path);
        if std::path::Path::new(&tmp_file_path).exists() {
            std::fs::rename(&tmp_file_path, &swap_file_path)
                .expect("Failed to rename temporary swap file back to swap file.");
        }

        // Initialize the swap file.
        let swap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&swap_file_path)
            .expect("Failed to create page file");

        // Ensure the swap file is the correct size (size + length of freelist) or larger (freelist)
        if swap_file.metadata().expect("Failed to get swap file metadata").len() < (size as u64 + 8) {
            // Set the file size to the total size (+8 for the length of the free list)
            swap_file.set_len(size as u64 + 8)
                .expect("Failed to size page file");   
        }

        // Initialize the persistent file.
        let mut persistent_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&persistent_file_path)
            .expect("Failed to create persistent file");

        // Ensure the persistent file is the correct size (size + length of freelist) or larger (freelist)
        if persistent_file.metadata().expect("Failed to get persistent file metadata").len() < (size as u64 + 8) {
            persistent_file.set_len(size as u64 + 8)
                .expect("Failed to size persistent file");
        }

        // As at this point we can't gurantee that the swap file is in a valid state, we copy the persistent file over it.
        io::copy(&mut persistent_file, &mut &swap_file)
            .expect("Failed to initialize swap file from persistent file");

        // Create the memory map for the swap file.
        let mmap_swap = unsafe {
            MmapMut::map_mut(&swap_file).expect("Failed to memory map the swap file")
        };

        let arc_mmap_swap = Arc::new(mmap_swap);
            
        let me = Self {
            swap_file: Arc::new(RwLock::new(swap_file)),
            dirty_swaped_pages: Arc::new(RwLock::new(BTreeSet::new())),
            path: path.to_string(),
            persistent_file: Arc::new(Mutex::new(persistent_file)),
            free_slots: Arc::new(Mutex::new(free_slots)),
            malloc_called: Arc::new(AtomicBool::new(false)),
            me: Weak::new(),
            size,
            swap_mmap: AtomicPtr::new(Arc::into_raw(arc_mmap_swap) as *mut MmapMut),
            _swap_mmap_lock: Arc::new(RwLock::new(())),
        };

        // Load the free list from disk.
        me.load_free_list().unwrap();
        
        // Ensure there is at least one free slot covering the entire memory if none was loaded.
        if me.free_slots.lock().is_empty() {
            me.free_slots.lock().push((0, size));
        }
        
        let rc_me = Arc::new(me);

        let weak_me = Arc::downgrade(&rc_me);

        let ptr = Arc::into_raw(rc_me);

        unsafe {
            let mut_ref = (ptr as *mut Self).as_mut().unwrap();
            mut_ref.me = weak_me;
            Arc::from_raw(ptr)
        }
    }

    /// Statically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer to that location.
    /// 
    /// Parameters:
    /// - length: The length of space to allocate.
    /// - pointer: The memory address as a u64.
    /// 
    /// Warning: Static allocations must be done before any dynamic allocations (malloc).
    /// They do not manage free space and will ignore any overlaps with other static allocated memory.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    pub fn smalloc<T>(&self, pointer: u64, length: usize) -> Result<Pointer<T>, Error> where T: Sized {
        // This panic is not guaranteed to be thread-safe but should generally work.
        if self.malloc_called.load(Ordering::SeqCst) {
            panic!("Static memory allocation cannot happen after malloc has been used. This ensures that dynamically allocated memory is not overwritten.");
        }

        // Reserve the specified memory region by removing it from the free slots.
        let result = Self::reserve_exact(&mut *self.free_slots.lock(), pointer, length);

        let pointer = Pointer::new(pointer, self.me.clone() as Weak<PersistentRandomAccessMemory>);

        match result {
            Err(Error::MemoryAllocationError) => {
                Ok(pointer)
            },
            Err(e) => {
                Err(e)
            },
            Ok(_) => {
                // Successfully reserved the memory region. Initialize with 0's
                let zero_bytes = vec![0u8; length];
                self.write(pointer.address, &zero_bytes)?;
                Ok(pointer)
            },
        }
    }

    /// Dynamically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer<T> to that location.
    /// 
    /// This function allocates 'len' bytes in persistent memory and returns a Pointer to that location.
    /// The runtime complexity is O(n) where n is the number of free slots, as it may need to search through the free slots.
    /// 
    /// Warning: Don't leak the returned Pointer as it will cause memory leaks!
    /// Also be conscious that the free list is held in memory and enough memory must be available ot work with this list.
    /// If the memory is fragmented, the list will grow and may consume significant memory.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    pub fn malloc<T>(&self, length: usize) -> Result<Pointer<T>, Error> where T: Sized {
        self.malloc_called.store(true, Ordering::SeqCst);

        // Find a free slot that can accommodate the requested length
        if let Ok(allocated_pointer) = self.reserve(length) {
            let pointer = Pointer::new(allocated_pointer, self.me.clone() as Weak<PersistentRandomAccessMemory>);
            
            // Initialize the allocated memory with 0's
            let zero_bytes = vec![0u8; length];
            self.write(pointer.address, &zero_bytes)?;
            Ok(pointer)
        } else {
            Err(Error::OutOfMemoryError)
        }
    }

    /// Frees the space allocated for the value in persistent memory
    /// pointed to by the given Pointer.
    /// 
    /// Warning: Don't use the Pointer after calling free on it as it will lead to undefined behavior.
    /// And don't double free the same Pointer as it will lead to undefined behavior.
    /// Additionally, freeing memory that was not allocated will lead to undefined behavior.
    /// Also, be aware that freeing memory may increase the size of the freelist and may overflow memory.
    /// 
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - length: The length of space to free.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn free(&self, pointer: u64, length: usize) -> Result<(), Error> {
        // First we need to aquire a read-lock so we know that data is not currently being persisted
        // This prevents modifying the freelist during a persist operation.
        let _lock = self._swap_mmap_lock.read();

        // Free the specified memory region and merge it with adjacent free slots.
        let mut free_slots = self.free_slots.lock();
        Self::free_and_merge(&mut *free_slots, pointer, length);
        Ok(())
    }

    /// Atomically renames a file from one path to another with retries.
    /// 
    /// Parameters:
    /// - from: The current file path.
    /// - to: The new file path.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn atomic_rename(from: &str, to: &str) -> Result<(), Error> {
        let rename_result = std::fs::rename(from, to);
        if let Ok(()) =  rename_result {
            return Ok(());
        } 

        // TODO: Somehow the rename fails even though the files exist and are not locked.
        // That is strange behavior, think of a solution for this...
        if let Err(e) = rename_result {
            error!("Failed to rename file from '{}' to '{}': {}", from, to, e);
        }

        return Err(Error::SwapFileError { 
                    inner: io::Error::new(io::ErrorKind::Other, "Failed to rename persistent file to temporary file after multiple attempts."), 
                    from: from.to_string(), 
                    to: to.to_string(),
                });
    }

    /// Persist all data in memory.
    /// 
    /// Warning: This function must be called to ensure all data is written to persistent storage.
    /// Failing to call this function will result in data loss. This function flushes all unsynced data to disk.
    /// As such, it may be a costly operation depending on the amount of unsynced data and the underlying storage performance.
    /// It will block until all data is persisted.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn persist(&self) -> Result<(), Error> {
        let mut new_swap_file;
        let mut new_persistent_file;
        let mut dirty_pages;

        // Lock the swap mmap for reading and replacing during the persist operation.
        let _lock = self._swap_mmap_lock.write();

        // Flush the memory-mapped swap file to disk.
        // Dereference is safe as we hold the lock preventing other threads from modifying the atomic pointer.
        unsafe { 
            (*self.swap_mmap.load(Ordering::Acquire)).flush().map_err(|_| Error::FlushError)?; 
        }
        // Ensure all writes are completed before proceeding.
        std::sync::atomic::fence(Ordering::Release);

        // Lock the swap file for writing during the persist operation.
        let mut swap_file = self.swap_file.write();

        // Flush the free list to the swap file.
        self.flush_free_list(&mut *swap_file)?;

        // Sync the swap file to ensure all data is written to disk.
        swap_file.sync_all().map_err(|_| Error::FlushError)?;

        // Ensure all writes are completed before proceeding.
        std::sync::atomic::fence(Ordering::Release);

        let swap_file_path_str = Self::get_swap_file_path(&self.path);
        let persistent_file_path_str = Self::get_persistent_file_path(&self.path);

        // Swap the files atomically -> the swap file has been fully written at this point
        // so we can just rename it to the persistent file.
        
        // Swap procedure:
        // WARN: This may leave temporary files on disk if the process crashes during the swap.

        // First move the current persistent file to a temporary location
        // These renames must be executed, else we are unable to recover and need to try to recover on startup.
        
        // Unfortunately, we can't recover from failed renames, so we retry a few times before giving up and making sure to exit.
        
        if let Err(e) = Self::atomic_rename(&persistent_file_path_str, &format!("{}.tmp", persistent_file_path_str)) {
            error!("Failed to rename persistent file to temporary file during persist: {}", e);
            return Err(Error::FatalError(Box::new(e)));
        }

        // Then move the swap file to the persistent file location
        if let Err(e) = Self::atomic_rename(&swap_file_path_str, &persistent_file_path_str) {
            error!("Failed to rename swap file to persistent file during persist: {}", e);
            return Err(Error::FatalError(Box::new(e)));
        }
    
        // Finally move the temporary file to the swap file location
        if let Err(e) = Self::atomic_rename(&format!("{}.tmp", persistent_file_path_str), &swap_file_path_str) {
            error!("Failed to rename temporary file to swap file during persist: {}", e);
            return Err(Error::FatalError(Box::new(e)));
        }

        // Reopen the swap file and persistent file handles after the swap
        new_swap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&swap_file_path_str)
            .map_err(|e| Error::FileOpenError { inner: e, path: swap_file_path_str.clone() })?;

        new_persistent_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&persistent_file_path_str)
            .map_err(|e| Error::FileOpenError { inner: e, path: persistent_file_path_str.clone() })?;

        // The swap file is at this point stale, as it has been swapped with the persistent file.
        // We need to write the recorded dirty pages back to the new swap file.
        dirty_pages = self.dirty_swaped_pages.write();
        let mut current_cursor: u64 = u64::MAX;

        // Read the page data from the persistent file and write it to the new swap file
        let mut page_data = [0u8; PAGE_SIZE];

        for dirty_page_index in dirty_pages.iter() {
            let offset = (*dirty_page_index as u64) * (PAGE_SIZE as u64);

            // Seek to the page offset in both files to keep cursors aligned
            if offset != current_cursor {
                new_swap_file
                    .seek(std::io::SeekFrom::Start(offset))
                    .map_err(Error::FileReadError)?;
                new_persistent_file
                    .seek(std::io::SeekFrom::Start(offset))
                    .map_err(Error::FileReadError)?;
                current_cursor = offset;
            }

            let n_read = new_persistent_file
                .read(&mut page_data)
                .map_err(Error::FileReadError)?;
            new_swap_file
                .write_all(&page_data[..n_read])
                .map_err(Error::FileWriteError)?;
            current_cursor += PAGE_SIZE as u64;
        }

        // Also write the free list to the new swap file
        self.flush_free_list(&mut new_swap_file)?;

        // To ensure the files are persisted to disk, we need to flush and sync them.
        new_swap_file.flush().map_err(|_| Error::FlushError)?;
        new_persistent_file.flush().map_err(|_| Error::FlushError)?;

        // Ensure all writes are completed before proceeding.
        std::sync::atomic::fence(Ordering::Release);

        new_swap_file.sync_all().map_err(|_| Error::FlushError)?;
        new_persistent_file.sync_all().map_err(|_| Error::FlushError)?;

        // Update the file handles
        let mut persistent_file = self.persistent_file.lock();
        *persistent_file = new_persistent_file;
        *swap_file = new_swap_file;

        // Now that both are in sync, we can clear the dirty pages set
        dirty_pages.clear(); 

        // Memory map the new swap file
        let new_mmap_swap = unsafe {
            MmapMut::map_mut(&*swap_file).map_err(|_| Error::ReadError)?
        };

        let new_arc_mmap_swap = Arc::new(new_mmap_swap);
        let new_ptr = Arc::into_raw(new_arc_mmap_swap) as *mut MmapMut;

        // The old pointer is actually a null pointer as we swapped it out earlier.
        let _ = self.swap_mmap.swap(new_ptr, Ordering::Release);

        // fence to ensure mmap is fully replaced before releasing the lock
        std::sync::atomic::fence(Ordering::Release);

        Ok(()) // the lock is released here
    }

    /// Reads data from the specified pointer into the provided buffer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of data to read.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn read<T>(&self, pointer: u64) -> Result<T, Error> {
        let len = std::mem::size_of::<T>();
        let bytes = unsafe {
            (&mut (*self.swap_mmap.load(Ordering::SeqCst)))
                .get(pointer as usize..(pointer as usize + len))
                .ok_or(Error::ReadError)?
        };

        let value = unsafe {
            std::ptr::read_unaligned(bytes.as_ptr() as *const T)
        };

        Ok(value)
    }

    /// Reads data from the specified pointer into the provided buffer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - buf: The buffer to read data into.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn read_exact(&self, pointer: u64, buf: &mut [u8]) -> Result<(), Error> {
        let len = buf.len();

        if len == 0 {
            return Ok(());
        }

        let bytes = unsafe {
            (&mut (*self.swap_mmap.load(Ordering::SeqCst)))
                .get(pointer as usize..(pointer as usize + len))
                .ok_or(Error::ReadError)?
        };

        buf.copy_from_slice(bytes);

        Ok(())
    }
    
    /// Writes data from the provided buffer to the specified pointer.
    /// 
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - buf: The buffer containing data to write.
    ///
    /// Returns:
    /// - Result indicating success or failure.
    pub fn write(&self, pointer: u64, buf: &[u8]) -> Result<(), Error> {
        let len = buf.len();

        if len == 0 {
            return Ok(());
        }

        {
            // First we need to aquire a read-lock so we know that data is not currently being persisted
            let _lock = self._swap_mmap_lock.read();

            unsafe {
                (&mut (*self.swap_mmap.load(Ordering::SeqCst)))
                    .get_mut(pointer as usize..(pointer as usize + len))
                    .ok_or(Error::WriteError)?
                    .copy_from_slice(buf);
            }
        } // release lock

        // Mark all pages touched by this write as dirty
        let start_page = pointer as usize / PAGE_SIZE;
        let end_offset = pointer + (len as u64).saturating_sub(1);
        let end_page = end_offset as usize / PAGE_SIZE;
        {
            let mut dirty = self
                .dirty_swaped_pages
                .write();
            for page in start_page..=end_page {
                dirty.insert(page);
            }
        } // release lock

        Ok(())
    }


    /// Reserves memory of the specified length from the free slots.
    /// 
    /// Parameters:
    /// - len: The length of memory to reserve.
    /// 
    /// Returns:
    /// - Result containing the starting pointer of the reserved memory on success or Error on failure.
    fn reserve(&self, len:usize) -> Result<u64, Error> {
        // First we need to aquire a read-lock so we know that data is not currently being persisted.
        // Changing the freelist during the persist-operation will lead to undefined behaviour.
        let _lock = self._swap_mmap_lock.read();

        // Find a free slot that can accommodate the requested length
        // First, try to find a free slot where the allocation can fit entirely within a single page.
        // This avoids allocations that span pages when possible. (reduced page faults)
        let mut allocated_pointer: Option<u64> = None;

        let mut free_slots = self.free_slots.lock();

        if len <= PAGE_SIZE {
            let page_size = PAGE_SIZE as u64;

            // We need to borrow the free_slots twice, so we do it in a nested scope to avoid borrow conflicts.
            for (free_pointer, free_len) in free_slots.iter() {
                if *free_len < len {
                    continue;
                }
                let free_start = *free_pointer;
                let free_end = free_start + (*free_len as u64);

                // If starting at free_start fits within the page, use it.
                let r = free_start % page_size;
                if r + (len as u64) <= page_size {
                    allocated_pointer = Some(free_start);
                    break;
                }

                // Otherwise try to move to the next page boundary (s.t. s % page_size == 0)
                // which will allow len bytes to fit in that page, if the free slot is large enough.
                let shift = page_size - r;
                let candidate = free_start + shift;
                if candidate + (len as u64) <= free_end {
                    allocated_pointer = Some(candidate);
                    break;
                }
            }
        }else {
            // Fallback: first-fit allocation (original behavior) if no single-page slot was found
            for (free_pointer, free_len) in free_slots.iter() {
                if *free_len >= len {
                    allocated_pointer = Some(*free_pointer);
                    break;
                }
            }
        }

        if let Some(allocated_pointer) = allocated_pointer {
            Self::reserve_exact(&mut *free_slots, allocated_pointer, len)?;
            return Ok(allocated_pointer);
        }

        Err(Error::OutOfMemoryError)
    }

    /// Reserves the specified memory region by removing it from the free slots.
    ///
    /// Parameters:
    /// - free_slots: A mutable reference to the vector of free slots.
    /// - pointer: The memory address as a u64.
    /// - len: The length of the memory region.
    /// 
    /// Returns:
    /// - None.
    /// Panics if the region is not fully free.
    fn reserve_exact(free_slots: &mut Vec<(u64, usize)>, pointer: u64, len: usize) -> Result<(), Error> {
        // Allow allocation that spans multiple free slots or partially overlaps free slots,
        // but ensure the full requested region is covered by free space.
        let mut indices_to_remove: Vec<usize> = Vec::new();
        let mut slots_to_add: Vec<(u64, usize)> = Vec::new();

        let alloc_start = pointer;
        let alloc_end = pointer + (len as u64);
        let mut covered_bytes: usize = 0;

        // Pre-allocate to avoid repeated reallocation
        indices_to_remove.reserve(free_slots.len());
        slots_to_add.reserve(4); // Usually at most 2 fragments

        for (index, (free_pointer, free_len)) in free_slots.iter().enumerate() {
            let free_start = *free_pointer;
            let free_end = *free_pointer + (*free_len as u64);

            // compute overlap between [alloc_start, alloc_end) and [free_start, free_end)
            let overlap_start = std::cmp::max(free_start, alloc_start);
            let overlap_end = std::cmp::min(free_end, alloc_end);

            if overlap_start < overlap_end {
                // This free slot contributes to covering the allocation.
                covered_bytes += (overlap_end - overlap_start) as usize;
                indices_to_remove.push(index);

                // If there's free space before the overlap, keep that part.
                if free_start < overlap_start {
                    slots_to_add.push((free_start, (overlap_start - free_start) as usize));
                }

                // If there's free space after the overlap, keep that part.
                if overlap_end < free_end {
                    slots_to_add.push((overlap_end, (free_end - overlap_end) as usize));
                }
            }
        }

        // Ensure the requested region is fully covered by free space.
        if covered_bytes != len {
            return Err(Error::MemoryAllocationError);
        }

        // Remove affected slots in reverse order so indices remain valid.
        indices_to_remove.sort_unstable();
        for idx in indices_to_remove.iter().rev() {
            free_slots.remove(*idx);
        }

        // Add back any split fragments.
        free_slots.extend(slots_to_add);
        return Ok(());
    }

    /// Frees the specified memory region and merges it with adjacent free slots.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of the memory region.
    /// 
    /// Returns:
    /// - None.
    fn free_and_merge(free_slots: &mut Vec<(u64, usize)>, pointer: u64, len: usize) {
        let mut new_start = pointer;
        let mut new_end = pointer + (len as u64);

        let mut indices_to_remove: Vec<usize> = Vec::new();
        
        indices_to_remove.reserve(free_slots.len());

        // Find and merge with adjacent or overlapping free slots.
        for (idx, (free_pointer, free_len)) in free_slots.iter().enumerate() {
            let free_start = *free_pointer;
            let free_end = free_start + (*free_len as u64);

            if !(free_end < new_start || free_start > new_end) {
                if free_start < new_start {
                    new_start = free_start;
                }
                if free_end > new_end {
                    new_end = free_end;
                }
                indices_to_remove.push(idx);
            }
        }

        indices_to_remove.sort_unstable();
        for idx in indices_to_remove.iter().rev() {
            free_slots.remove(*idx);
        }

        free_slots.push((new_start, (new_end - new_start) as usize));
        free_slots.sort_unstable_by_key(|(p, _)| *p);
    }

    /// Loads the free list from persistent storage.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn load_free_list(&self) -> Result<(), Error> {
        let mut file = self.swap_file.write();
        
        file.seek(std::io::SeekFrom::Start(self.size as u64)).map_err(Error::FileReadError)?;

        let mut buffer: Vec<u8> = Vec::new();

        // Read the length-prefixed serialized free slots
        let mut length_prefix = [0u8; 8];
        file.read_exact(&mut length_prefix).map_err(Error::FileReadError)?;

        let length = u64::from_le_bytes(length_prefix) as usize;

        // no need to load anything
        if length == 0 {
            return Ok(());
        }

        buffer.resize(length, 0);

        // Read the serialized free slots into the buffer
        file.read_exact(&mut buffer).map_err(Error::FileReadError)?;

        let (decoded_free_slots, _): (Vec<(u64, usize)>, _) = bincode::decode_from_slice(
            &buffer[0..length],
            bincode::config::standard()
        ).map_err(|_| Error::ReadError)?;

        let mut free_slots = self.free_slots.lock();
        *free_slots = decoded_free_slots;

        Ok(())
    }

    /// Flushes the free list to persistent storage.    
    /// 
    /// Returns:
    ///  - Result indicating success or failure.
    fn flush_free_list(&self, file: &mut std::fs::File) -> Result<(), Error> {
        let free_slots = self.free_slots.lock();
        
        file.seek(std::io::SeekFrom::Start(self.size as u64)).map_err(Error::FileReadError)?;

        let mut bytes = Vec::new();
        bytes.resize((8+std::mem::size_of::<usize>()) * free_slots.len(), 0);

        let length = bincode::encode_into_slice(free_slots.as_slice(), &mut bytes, bincode::config::standard())
            .map_err(|_| Error::WriteError)?;

        // Write the length-prefixed serialized free slots
        let length_prefix: [u8; 8] = (length as u64).to_le_bytes();
        file.write_all(&length_prefix).map_err(Error::FileWriteError)?;

        // Write the page from the cached page buffer to the file
        file.write_all(&bytes[0..length]).map_err(Error::FileWriteError)?;
        
        Ok(())
    }
}