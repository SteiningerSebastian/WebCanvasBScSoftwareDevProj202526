use std::{cell::RefCell, fmt::Display, fs::OpenOptions, io::{BufReader, BufWriter, Read, Write}, rc::{Rc, Weak}};

use crate::page_cache::{self, LruKCache};

#[derive(Debug)]
pub enum Error {
    ReadError,
    WriteError,
    FlushError,
    FileReadError,
    OutOfMemoryError,
    NotOnOnePageError,
    CacheStoreError(page_cache::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ReadError => write!(f, "Read Error"),
            Error::WriteError => write!(f, "Write Error"),
            Error::FlushError => write!(f, "Flush Error"),
            Error::FileReadError => write!(f, "File Read Error"),
            Error::OutOfMemoryError => write!(f, "Out of Memory Error"),
            Error::NotOnOnePageError => write!(f, "Data not contained on single page Error"),
            Error::CacheStoreError(e) => write!(f, "Cache Store Error: {}", e),
        }
    }
}

pub struct Pointer {
    pub pointer: u64,
    memory: Weak<dyn PersistentRandomAccessMemory>,
}

impl Clone for Pointer {
    fn clone(&self) -> Self {
        Self {
            pointer: self.pointer,
            memory: self.memory.clone(),
        }
    }
}

/// A Pointer represents a location in persistent random access memory.
/// 
/// Warning: Be careful when using this struct as it contains a raw pointer to the memory manager.
/// Improper use can lead to undefined behavior, memory leaks, or data corruption.
impl Pointer {
    /// Creates a new Pointer instance.
    ///    
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - memory: A Mutex-wrapped reference to a PersistentRandomAccessMemory instance.
    /// 
    /// Returns:
    /// - A new Pointer instance.
    pub fn new(pointer: u64, memory: Weak<dyn PersistentRandomAccessMemory>) -> Self {
        Self { pointer, memory }
    }

    /// Dereferences the pointer to get a reference to the value of type T.
    /// This only works if T is stored within the same page, it will return None otherwise.
    ///
    /// Warning: A reference to the internal page_cache is returned. If the page is swapped out, the reference will become invalid.
    /// Only use this function for short lived operations with high performance requirements, where you are sure the page will not be swapped out of memory.
    /// 
    /// Returns:
    /// - A reference to the value of type T.
    /// 
    /// # Safety
    /// This function is unsafe because it allows access to the underlying data without any lifetime guarantees.
    /// The caller must ensure that the page containing the data is not swapped out of memory
    /// while the reference is in use.
    pub fn unsafe_deref<T>(&mut self) -> Result<Option<&T>, Error> {
        // Reject misaligned access to prevent UB on &T
        if (self.pointer as usize) % std::mem::align_of::<T>() != 0 {
            return Ok(None);
        }
        unsafe {
            let res = (*self.memory.as_ptr()).unsafe_read(self.pointer, std::mem::size_of::<T>()).map(|data| {
                if data.len() == std::mem::size_of::<T>() {
                    let ptr = data.as_ptr() as *const T;
                    Some(&*ptr)
                } else {
                    None
                }
            });

            if let Err(e) = &res {
                if matches!(e, &Error::NotOnOnePageError) {
                    return Ok(None);
                }
            }
            res
        }
    }

    /// Mutably dereferences the pointer to get a mutable reference to the value of type T.
    /// This only works if T is stored within the same page, it will return None otherwise.
    /// 
    /// Warning: A mutable reference to the internal page_cache is returned. If the page is swapped out, the reference will become invalid.
    /// Only use this function for short lived operations with high performance requirements, where you are sure
    /// the page will not be swapped out of memory.
    /// 
    /// Returns:
    /// - A mutable reference to the value of type T.
    /// 
    /// # Safety
    /// This function is unsafe because it allows mutable access to the underlying data, which can lead to
    /// data races if not used carefully. The caller must ensure that no other references to the same data exist
    /// while this mutable reference is in use. Additionally, the caller must ensure that the page containing the data is not swapped out of memory
    /// while the mutable reference is in use.
    pub fn unsafe_deref_mut<T>(&mut self) -> Result<Option<&mut T>, Error> where T: Sized {
        // Reject misaligned access to prevent UB on &mut T
        if (self.pointer as usize) % std::mem::align_of::<T>() != 0 {
            return Ok(None);
        }
        unsafe {
            let res = (*self.memory.as_ptr()).unsafe_read_mut(self.pointer, std::mem::size_of::<T>()).map(|data| {
                if data.len() == std::mem::size_of::<T>() {
                    let ptr = data.as_ptr() as *mut T;
                    Some(&mut *ptr)
                } else {
                    None
                }
            });

            // Mirror unsafe_deref: treat cross-page as None
            if let Err(e) = &res {
                if matches!(e, &Error::NotOnOnePageError) {
                    return Ok(None);
                }
            }
            res
        }
    }

    /// Dereferences the pointer to get a value of type T. 
    /// This involves reading from the persistent memory and copying the data.
    /// 
    /// Warning: Changes to the returned value will not be reflected in persistent memory.
    /// You must explicitly write back to persistent memory if you want to persist changes.
    /// 
    /// Returns:
    /// - Result containing the value of type T on success or Error on failure.
    pub fn deref<T>(&self) -> Result<Box<T>, Error> where T: Sized {
        use std::mem::MaybeUninit;
        unsafe {
            let mut value = MaybeUninit::<T>::uninit();
            let buf = std::slice::from_raw_parts_mut(
                value.as_mut_ptr() as *mut u8,
                std::mem::size_of::<T>(),
            );
            (*self.memory.as_ptr()).read(self.pointer, buf)?;
            Ok(Box::new(value.assume_init()))
        }
    }

    /// Dereferences the pointer to get a byte vector of exact length.
    /// 
    /// Warning: Changes to the returned value will not be reflected in persistent memory.
    /// You must explicitly write back to persistent memory if you want to persist changes.
    /// 
    /// Returns:
    /// - Result containing the byte vector on success or Error on failure.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        unsafe {
            (*self.memory.as_ptr()).read(self.pointer, buf)?;
        }
        Ok(())
    }

    /// Writes the given value of type T to the location pointed to by this Pointer.
    ///     
    /// Parameters:
    /// - value: The value to be written to persistent memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn write<T>(&mut self, value: &T) -> Result<(), Error> where T: Sized {
        unsafe {
            let slice = std::slice::from_raw_parts(
                (value as *const T) as *const u8,
                std::mem::size_of::<T>(),
            );
        
            (*self.memory.as_ptr()).write(self.pointer, slice)?;
        };
        Ok(())
    }

    /// Writes the given byte slice to the location pointed to by this Pointer.
    /// 
    /// Parameters:
    /// - buf: The byte slice to be written to persistent memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn write_exact(&mut self, buf: &[u8]) -> Result<(), Error> {
        unsafe {
            (*self.memory.as_ptr()).write(self.pointer, buf)?;
        };
        Ok(())
    }

    /// Returns a new Pointer offset by the given index of type T.
    /// 
    /// Parameters:
    /// - index: The index to offset by.
    /// - T: The type of the elements being indexed.
    /// 
    /// Returns:
    /// - A new Pointer offset by the given index.
    pub fn at<T>(&self, index: usize) -> Pointer where T: Sized {
        let offset = (index * std::mem::size_of::<T>()) as u64;
        Pointer::new(self.pointer + offset, self.memory.clone())
    }

    /// Dereferences the pointer at the given index to get a value of type T.
    /// 
    /// Warning: Changes to the returned value will not be reflected in persistent memory.
    /// You must explicitly write back to persistent memory if you want to persist changes.
    /// 
    /// Parameters:
    /// - index: The index to dereference.
    /// 
    /// Returns:
    /// - Result containing the value of type T on success or Error on failure.
    pub fn deref_at<T>(&self, index: usize) -> Result<Box<T>, Error> where T: Sized {
        use std::mem::MaybeUninit;
        unsafe {
            let mut value = MaybeUninit::<T>::uninit();
            let buf = std::slice::from_raw_parts_mut(
                value.as_mut_ptr() as *mut u8,
                std::mem::size_of::<T>(),
            );
            let offset = (index * std::mem::size_of::<T>()) as u64;
            (*self.memory.as_ptr()).read(self.pointer + offset, buf)?;
            Ok(Box::new(value.assume_init()))
        }
    }   
}

pub trait PersistentRandomAccessMemory {
    /// Dynamically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer<T> to that location.
    /// 
    /// Parameters:
    /// - value: The value to be stored in persistent memory.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    fn malloc(&self, len: usize) -> Result<Pointer, Error>;

    /// Statically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer to that location.
    /// 
    /// Parameters:
    /// - len: The length of space to allocate.
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    fn salloc(&self, pointer: u64, len: usize) -> Result<Pointer, Error>;

    /// Frees the space allocated for the value in persistent memory
    /// pointed to by the given Pointer.
    /// 
    /// Parameters:
    /// - pointer: The Pointer pointing to the value to be freed.
    /// - len: The length of space to free.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn free(&self, pointer: Pointer, len: usize) -> Result<(), Error>;

    /// Persist all data in memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn persist(&self) -> Result<(), Error>;

    /// Reads data from the specified pointer into the provided buffer.
    /// 
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of data to read.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn read(&self, pointer: u64, buf: &mut [u8]) -> Result<(), Error>;

    /// Unsafely reads data from the specified pointer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of data to read.
    /// 
    /// Returns:
    /// - Result containing a byte slice on success or Error on failure.
    /// 
    /// # Safety
    /// This function is unsafe because it allows access to the underlying data without any lifetime guarantees.
    /// The caller must ensure that the page containing the data is not swapped out of memory.
    fn unsafe_read(&self, pointer: u64, len: usize) -> Result<&[u8], Error>;

    /// Unsafely reads data from the specified pointer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of data to read.
    /// 
    /// Returns:
    /// - Result containing a byte slice on success or Error on failure.
    /// 
    /// # Safety
    /// This function is unsafe because it allows access to the underlying data without any lifetime guarantees.
    /// The caller must ensure that the page containing the data is not swapped out of memory.
    fn unsafe_read_mut(&self, pointer: u64, len: usize) -> Result<&mut [u8], Error>;
    
    /// Writes data from the provided buffer to the specified pointer.
    ///  
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - buf: The buffer containing data to write.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn write(&self, pointer: u64, buf: &[u8]) -> Result<(), Error>;  
}

pub struct FilePersistentRandomAccessMemory {
    cached_page: RefCell<Vec<u8>>,
    cached_page_dirty: RefCell<bool>,
    path: String,
    current_page_index: RefCell<Option<usize>>,
    unsynced_pages: RefCell<Vec<bool>>,

    free_slots: RefCell<Vec<(u64, usize)>>,
    malloc_called: RefCell<bool>,
    me: Weak<FilePersistentRandomAccessMemory>,

    page_size: usize,
    page_shift: usize,
    cache: RefCell<LruKCache>,
}

impl PersistentRandomAccessMemory for FilePersistentRandomAccessMemory {
    /// Statically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer to that location.
    /// 
    /// Parameters:
    /// - len: The length of space to allocate.
    /// - pointer: The memory address as a u64.
    /// 
    /// Warning: Static allocations must be done before any dynamic allocations (malloc).
    /// They do not manage free space and will ignore any overlaps with other static allocated memory.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    fn salloc(&self, pointer: u64, len: usize) -> Result<Pointer, Error> {
        {
            let malloc_called = self.malloc_called.borrow();
            if *malloc_called {
                panic!("Static memory allocation cannot happen after malloc has been used. This is to prevent overriding dynamically allocated memory.");
            }
        } // release borrow of malloc_called

        // Reserve the specified memory region by removing it from the free slots.
        self.reserve_exact(pointer, len);

        return Ok(Pointer::new(pointer, self.me.clone() as Weak<dyn PersistentRandomAccessMemory>));
    }

    /// Dynamically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer<T> to that location.
    /// 
    /// This function allocates 'len' bytes in persistent memory and returns a Pointer to that location.
    /// The Runtime Complexity is O(n) where n is the number of free slots, as it may need to search through the free slots.
    /// 
    /// TODO: Future optimizations could include maintaining a more efficient data structure for free slots to reduce allocation time.
    /// 
    /// Warning: Don't leak the returned Pointer as it will cause memory leaks!
    /// Also the free list is not persisted across restarts. On restart all memory is considered unused. 
    /// This is not an issue for static allocations as they are managed seperately.
    ///           
    /// Parameters:
    /// - len: The length of space to allocate.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    fn malloc(&self, len: usize) -> Result<Pointer, Error> {
        self.malloc_called.replace(true);

        // Find a free slot that can accommodate the requested length
        if let Ok(allocated_pointer) = self.reserve(len) {
            return Ok(Pointer::new(allocated_pointer, self.me.clone() as Weak<dyn PersistentRandomAccessMemory>));
        }

        Err(Error::OutOfMemoryError)
    }

    /// Frees the space allocated for the value in persistent memory
    /// pointed to by the given Pointer.
    /// 
    /// Warning: Don't use the Pointer after calling free on it as it will lead to undefined behavior.
    /// And don't double free the same Pointer as it will lead to undefined behavior.
    /// Additionally, freeing memory that was not allocated will lead to undefined behavior.
    /// Also be conscious that the free list is not persisted across restarts. On restart all memory is considered unused.
    /// 
    /// Parameters:
    /// - pointer: The Pointer pointing to the value to be freed.
    /// - len: The length of space to free.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn free(&self, pointer: Pointer, len: usize) -> Result<(), Error> {
        // Free the specified memory region and merge it with adjacent free slots.
        self.free_and_merge(pointer.pointer, len);
        Ok(())
    }

    /// Persist all data in memory.
    /// 
    /// Warning: This function must be called to ensure all data is written to persistent storage.
    /// Failing to call this function may result in data loss. This function flushes all unsynced pages to disk.
    /// As such, it may be a costly operation depending on the amount of unsynced data and the underlying storage performance.
    /// It will block until all data is persisted.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn persist(&self) -> Result<(), Error> {
        // write the current cached page if it is unsynced
        if let Some(current_page_index) = *self.current_page_index.borrow() {
            if *self.cached_page_dirty.borrow() {
                self.flush_page(current_page_index, &self.cached_page.borrow())?;
                self.cached_page_dirty.replace(false);
            }
        }

        // Flush all other dirty pages in the cache
        let mut cache = self.cache.borrow_mut();

        for slot in cache.iter_mut() {
            if let Some(slot) = slot {
                if slot.dirty {
                    if let Some(data) = slot.data.as_ref() {
                        self.flush_page(slot.page_index, data)?;
                        slot.dirty = false;
                    }
                }
            }
        }

        // Iterate over unsynced pages and flush them
        let mut unsynced = self.unsynced_pages.borrow_mut();
        for (idx, needs_sync) in unsynced.iter_mut().enumerate() {
            if !*needs_sync { continue; }
            let path = Self::get_page_path(&self.path, idx);
            // Do not truncate; just sync existing contents
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|_| Error::WriteError)?;
            file.sync_all().map_err(|_| Error::FlushError)?;
            *needs_sync = false;
        }

        Ok(())
    }

    fn read(&self, pointer: u64, buf: &mut [u8]) -> Result<(), Error> {
        let len = buf.len();

        if len == 0 {
            return Ok(());
        }

        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire requested data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= self.page_size {
            buf.copy_from_slice(&self.cached_page.borrow()[page_offset..page_offset + len]);
            Ok(())
        } else {
            // Data spans multiple pages, copy data into a temporary buffer
            let mut remaining = len;
            let mut dst_offset = 0;
            let mut current_pointer = pointer;
            while remaining > 0 {
                let page_index = self.get_page_index(current_pointer);
                self.ensure_page_loaded(page_index)?;

                let page_offset = self.get_page_offset(current_pointer);
                let to_copy = std::cmp::min(remaining, self.page_size - page_offset);

                // Copy data from the current page to the buffer
                let page = self.cached_page.borrow();
                buf[dst_offset..dst_offset + to_copy]
                    .copy_from_slice(&page[page_offset..page_offset + to_copy]);

                current_pointer += to_copy as u64;
                dst_offset += to_copy;
                remaining -= to_copy;
            }
            Ok(())
        }
    }
    
    fn unsafe_read(&self, pointer: u64, len: usize) -> Result<&[u8], Error> {
        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire requested data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= self.page_size {
            unsafe {
                let raw_ptr = self.cached_page.borrow().as_ptr();
                let cached_page = std::slice::from_raw_parts(raw_ptr, self.page_size);
                Ok(&cached_page[page_offset..page_offset + len])
            }
        } else {
            // Data spans multiple pages, cannot provide a continuous slice
            Err(Error::NotOnOnePageError)
        }
    }
    
    fn write(&self, pointer: u64, buf: &[u8]) -> Result<(), Error> {
        let len = buf.len();

        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= self.page_size {
            self.cached_page.borrow_mut()[page_offset..page_offset + len].copy_from_slice(buf);
            self.cached_page_dirty.replace(true);
            Ok(())
        } else {
            // Data spans multiple pages, copy data from a temporary buffer
            let mut src_offset = 0;
            let mut remaining = len;
            let mut current_pointer = pointer;
            while remaining > 0 {
                let page_index = self.get_page_index(current_pointer);
                self.ensure_page_loaded(page_index)?;

                let page_offset = self.get_page_offset(current_pointer);
                let to_copy = std::cmp::min(remaining, self.page_size - page_offset);

                // Copy data from the buffer to the current page
                // Single borrow_mut per iteration
                {
                    let mut page = self.cached_page.borrow_mut();
                    page[page_offset..page_offset + to_copy]
                        .copy_from_slice(&buf[src_offset..src_offset + to_copy]);
                }

                // Mark the page as dirty
                self.cached_page_dirty.replace(true);

                current_pointer += to_copy as u64;
                src_offset += to_copy;
                remaining -= to_copy;
            }
            Ok(())
        }
    }
    
    fn unsafe_read_mut(&self, pointer: u64, len: usize) -> Result<&mut [u8], Error> {
        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire requested data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= self.page_size {
            unsafe {
                let raw_ptr = self.cached_page.borrow().as_ptr();
                let cached_page = std::slice::from_raw_parts_mut(raw_ptr as *mut u8, self.page_size);
                
                // creating an unsafe mutable slice, mark the page as dirty
                self.cached_page_dirty.replace(true);
        
                Ok(&mut cached_page[page_offset..page_offset + len])
            }
        } else {
            // Data spans multiple pages, cannot provide a continuous slice
            Err(Error::NotOnOnePageError)
        }
    }
}

impl FilePersistentRandomAccessMemory {
    pub fn new(size: usize, path: &str, page_size: usize, lru_capacity: usize, lru_history_length: usize, lru_pardon: usize) -> Rc<Self> {
        // Ensure size is a multiple of PAGE_SIZE
        if (size % page_size) != 0 {
            panic!("Size must be a multiple of PAGE_SIZE (4096 bytes)");
        }

        // Be aware that this is a very naive free slot management
        // and the free list will not be persisted across restarts.
        let mut free_slots = Vec::new();
        free_slots.push((0, size));

        let page_count = size / page_size;

        // Initialize the persistent random access memory
        for page_index in 0..page_count {
            let page_path = Self::get_page_path(path, page_index);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&page_path)
                .expect("Failed to create page file");
            file.set_len(page_size as u64)
                .expect("Failed to size page file");
        }

        let me = Self {
            cached_page: RefCell::new(vec![0; page_size]),
            cached_page_dirty: RefCell::new(false),
            path: path.to_string(),
            current_page_index: RefCell::new(None),
            unsynced_pages: RefCell::new(vec![false; size / page_size]),
            free_slots: RefCell::new(free_slots),
            malloc_called: RefCell::new(false),
            me: Weak::new(),
            page_size,
            page_shift: page_size.trailing_zeros() as usize,
            cache: RefCell::new(LruKCache::new(lru_history_length, lru_capacity, lru_pardon)),
        };

        let rc_me = Rc::new(me);

        let weak_me = Rc::downgrade(&rc_me);

        let ptr = Rc::into_raw(rc_me);

        unsafe {
            let mut_ref = (ptr as *mut Self).as_mut().unwrap();
            mut_ref.me = weak_me;
            Rc::from_raw(ptr)
        }
    }

    /// Returns the current loaded page index, or None if no page is loaded.
    pub fn get_current_page_index(&self) -> Option<usize> {
        self.current_page_index.borrow().clone()
    }

    /// Reserves memory of the specified length from the free slots.
    /// 
    /// Parameters:
    /// - len: The length of memory to reserve.
    /// 
    /// Returns:
    /// - Result containing the starting pointer of the reserved memory on success or Error on failure.
    fn reserve(&self, len:usize) -> Result<u64, Error> {
        // Find a free slot that can accommodate the requested length
        // First, try to find a free slot where the allocation can fit entirely within a single page.
        // This avoids allocations that span pages when possible.
        let mut allocated_pointer: Option<u64> = None;

        if len <= self.page_size {
            let page_size = self.page_size as u64;

            // We need to borrow the free_slots twice, so we do it in a nested scope to avoid borrow conflicts.
            let free_slots = self.free_slots.borrow();
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
            let free_slots = self.free_slots.borrow();

            // Fallback: first-fit allocation (original behavior) if no single-page slot was found
            for (free_pointer, free_len) in free_slots.iter() {
                if *free_len >= len {
                    allocated_pointer = Some(*free_pointer);
                    break;
                }
            }
        }

        if let Some(allocated_pointer) = allocated_pointer {
            self.reserve_exact(allocated_pointer, len);
            return Ok(allocated_pointer);
        }

        Err(Error::OutOfMemoryError)
    }

    /// Reserves the specified memory region by removing it from the free slots.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of the memory region.
    /// 
    /// Returns:
    /// - None.
    /// Panics if the region is not fully free.
    fn reserve_exact(&self, pointer: u64, len: usize) {
        // Allow allocation that spans multiple free slots or partially overlaps free slots,
        // but ensure the full requested region is covered by free space.
        let mut indices_to_remove: Vec<usize> = Vec::new();
        let mut slots_to_add: Vec<(u64, usize)> = Vec::new();

        let alloc_start = pointer;
        let alloc_end = pointer + (len as u64);
        let mut covered_bytes: usize = 0;

        let mut free_slots = self.free_slots.borrow_mut();

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
            panic!("Attempted to reserve a slot that is not fully free");
        }

        // Remove affected slots in reverse order so indices remain valid.
        indices_to_remove.sort_unstable();
        for idx in indices_to_remove.iter().rev() {
            free_slots.remove(*idx);
        }

        // Add back any split fragments.
        free_slots.extend(slots_to_add);
    }

    /// Frees the specified memory region and merges it with adjacent free slots.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of the memory region.
    /// 
    /// Returns:
    /// - None.
    fn free_and_merge(&self, pointer: u64, len: usize) {
        let mut new_start = pointer;
        let mut new_end = pointer + (len as u64);

        let mut indices_to_remove: Vec<usize> = Vec::new();
        
        {
            let free_slots = self.free_slots.borrow();
            indices_to_remove.reserve(free_slots.len());

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
        }

        indices_to_remove.sort_unstable();
        let mut free_slots = self.free_slots.borrow_mut();
        for idx in indices_to_remove.iter().rev() {
            free_slots.remove(*idx);
        }

        free_slots.push((new_start, (new_end - new_start) as usize));
        free_slots.sort_unstable_by_key(|(p, _)| *p);
    }

    /// Calculates the page index for a given pointer.
    ///     
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - The page index as usize.
    fn get_page_index(&self, pointer: u64) -> usize {
        (pointer as usize) >> self.page_shift
    }

    /// Calculates the offset within a page for a given pointer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - The offset within the page as usize.
    fn get_page_offset(&self, pointer: u64) -> usize {
        (pointer as usize) & (self.page_size - 1)
    }

    /// Constructs the file path for a given page index.
    ///     
    /// Parameters:
    /// - page_index: The index of the page.
    /// 
    /// Returns:
    /// - The file path as a String.
    fn get_page_path(path: &str, page_index: usize) -> String {
        format!("{}.page{}", path, page_index)
    }

    /// Loads the specified page from persistent storage into memory.
    ///
    /// Parameters:
    /// - page_index: The index of the page to load.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn load_page(&self, page_index: usize) -> Result<(), Error> {
        let path = Self::get_page_path(&self.path, page_index);
        
        let file = std::fs::File::open(&path).map_err(|_| Error::FileReadError)?;
        let metadata = file.metadata().map_err(|_| Error::FileReadError)?;
        
        if metadata.len() == 0 {
            // Page was created but never written - fill with zeros
            self.cached_page.borrow_mut().fill(0);
        } else {
            let mut reader = BufReader::with_capacity(self.page_size, file);
            reader.read_exact(self.cached_page.borrow_mut().as_mut())
                .map_err(|_| Error::FileReadError)?;
        }
        
        *self.current_page_index.borrow_mut() = Some(page_index);
        self.cached_page_dirty.replace(false);
        Ok(())
    }

    /// Flushes the specified page from memory to persistent storage.
    ///     
    /// Parameters:
    /// - page_index: The index of the page to flush.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn flush_page(&self, page_index: usize, page: &Vec<u8>) -> Result<(), Error> {
        let path = Self::get_page_path(&self.path, page_index);
        
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|_| Error::WriteError)?;
        
        let mut writer = BufWriter::with_capacity(self.page_size, file);
        writer.write_all(page).map_err(|_| Error::WriteError)?;
        writer.flush().map_err(|_| Error::FlushError)?;

        self.unsynced_pages.borrow_mut()[page_index] = true;

        Ok(())
    }

    /// Ensures that the specified page is loaded into memory.
    /// 
    /// Parameters:
    /// - page_index: The index of the page to ensure is loaded.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn ensure_page_loaded(&self, page_index: usize) -> Result<(), Error> {
        if *self.current_page_index.borrow() == Some(page_index) {
            return Ok(());
        }

        let mut cache = self.cache.borrow_mut();
        // Case 1: Page is in cache
        if cache.contains_page(page_index) {
            // Can not return null as we made sure the page exists above
            let cache_data = cache.fetch_page(page_index).unwrap();
            let old_page = self.cached_page.replace(cache_data);
            
            if let Some(old_index) = *self.current_page_index.borrow() {
                let was_dirty = *self.cached_page_dirty.borrow();
                // Store the current cached page back into the cache
                let evicted = cache.store_page(old_index, old_page, was_dirty)
                                                .map_err(Error::CacheStoreError)?;

                // Now we need to actually flush the evicted page to disk if it was dirty as we can't keep it in memory anymore.
                if let Some(slot) = evicted {
                    if slot.dirty {
                        // If we fetch the page that we are evicting from the cache, this will be null
                        // as the current page is held by the memory manager and not the cache.
                        if let Some(data) = slot.data {
                            self.flush_page(slot.page_index, &data)?;
                        }
                    }
                }
            }

            *self.current_page_index.borrow_mut() = Some(page_index);
            self.cached_page_dirty.replace(false);
            return Ok(());
        }

        // Case 2: Page is not in cache, need to evict a page or create a new buffer.

        // Remember the current page index and dirty state before swapping
        let old_index = *self.current_page_index.borrow();
        let was_dirty = *self.cached_page_dirty.borrow();
        // Store the old page back into the cache if it exists
        if let Some(old_index) = old_index {
            let candidate = cache.eviction_candidate(old_index);

            // Now we need to actually flush the evicted page to disk if it was dirty as we can't keep it in memory anymore.
            let reuse_buffer = if let Some(slot) = candidate {
                if slot.dirty {
                    // If we fetch the page that we are evicting from the cache, this will be null
                    // as the current page is held by the memory manager and not the cache.
                    if let Some(data) = slot.data.as_ref() {
                        self.flush_page(slot.page_index, data)?;
                    }
                }
                slot.data.take()
            } else {
                None
            };
       
            let old_page_buffer = self.cached_page.replace(
                reuse_buffer.unwrap_or_else(|| vec![0; self.page_size])
            );

            // Store the current cached page back into the cache
            let _ = cache.store_page(old_index, old_page_buffer, was_dirty)
                    .map_err(Error::CacheStoreError)?;
        }
        
        // Now load the requested page from disk to memory
        return self.load_page(page_index); 
    }
}