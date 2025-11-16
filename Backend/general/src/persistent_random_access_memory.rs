use std::{collections::HashSet, fmt::Display, io::{Read, Write}};
use std::alloc::{alloc, dealloc, Layout};

#[derive(Debug)]
pub enum Error {
    ReadError,
    WriteError,
    FlushError,
    FileReadError,
    OutOfMemoryError,
    NotOnOnePageError,
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
        }
    }
}

pub struct Pointer {
    pub pointer: u64,
    memory: *mut dyn PersistentRandomAccessMemory,
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
    pub fn new(pointer: u64, memory: *mut dyn PersistentRandomAccessMemory) -> Self {
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
        unsafe {
            let res = (*self.memory).unsafe_read(self.pointer, std::mem::size_of::<T>()).map(|data| {
                if data.len() == std::mem::size_of::<T>() {
                    let ptr = data.as_ptr() as *const T;

                    // SAFETY: The caller must ensure that the page containing the data is not swapped out of memory
                    Some(&*ptr) 
                } else {
                    None
                }
            });

            // If the error is NotOnOnePageError, return Ok(None) instead as it is not a error but expected behavior
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
        unsafe {
            (*self.memory).unsafe_read(self.pointer, std::mem::size_of::<T>()).map(|data| {
                if data.len() == std::mem::size_of::<T>() {
                    let ptr = data.as_ptr() as *mut T;

                    // SAFETY: The caller must ensure that no other references to the same data exist
                    // while this mutable reference is in use. Additionally, the caller must ensure that the page containing the data is not swapped out of memory
                    Some(&mut *ptr)
                } else {
                    None
                }
            })
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
    pub fn deref<T>(&mut self) -> Result<Box<T>, Error> where T: Sized {
        unsafe {
            // Allocate uninitialized memory for T
            let layout = Layout::new::<T>();
            let ptr = alloc(layout) as *mut T;
            
            if ptr.is_null() {
                return Err(Error::OutOfMemoryError);
            }

            // Read directly into the allocated memory
            let slice = std::slice::from_raw_parts_mut(ptr as *mut u8, std::mem::size_of::<T>());
            match (*self.memory).read(self.pointer, slice) {
                Ok(_) => Ok(Box::from_raw(ptr)),
                Err(e) => {
                    // Clean up on error
                    dealloc(ptr as *mut u8, layout);
                    Err(e)
                }
            }
        }
    }

    /// Writes the given value of type T to the location pointed to by this Pointer.
    ///     
    /// Parameters:
    /// - value: The value to be written to persistent memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    pub fn write<T>(&mut self, value: &T) -> Result<(), Error> where T: Sized {
        let slice = unsafe {
            std::slice::from_raw_parts(
                (value as *const T) as *const u8,
                std::mem::size_of::<T>(),
            )
        };
        unsafe {
            (*self.memory).write(self.pointer, slice)
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
    fn malloc(&mut self, len: usize) -> Result<Pointer, Error>;

    /// Statically allocates the necessary space for a value in persistent memory
    /// and returns a Pointer to that location.
    /// 
    /// Parameters:
    /// - len: The length of space to allocate.
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - Result containing Pointer on success or Error on failure.
    fn salloc(&mut self, pointer: u64, len: usize) -> Result<Pointer, Error>;

    /// Frees the space allocated for the value in persistent memory
    /// pointed to by the given Pointer.
    /// 
    /// Parameters:
    /// - pointer: The Pointer pointing to the value to be freed.
    /// - len: The length of space to free.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn free(&mut self, pointer: Pointer, len: usize) -> Result<(), Error>;

    /// Persist all data in memory.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn persist(&mut self) -> Result<(), Error>;

    /// Reads data from the specified pointer into the provided buffer.
    /// 
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of data to read.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn read(&mut self, pointer: u64, buf: &mut [u8]) -> Result<(), Error>;

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
    fn unsafe_read(&mut self, pointer: u64, len: usize) -> Result<&[u8], Error>;
    
    /// Writes data from the provided buffer to the specified pointer.
    ///  
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - buf: The buffer containing data to write.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn write(&mut self, pointer: u64, buf: &[u8]) -> Result<(), Error>;  
}

pub struct FilePersistentRandomAccessMemory {
    cached_page: Vec<u8>,
    path: String,
    current_page_index: Option<usize>,
    unsynced_pages: HashSet<usize>,
    free_slots: Vec<(u64, usize)>,
    malloc_called: bool,
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
    fn salloc(&mut self, pointer: u64, len: usize) -> Result<Pointer, Error> {
        if self.malloc_called {
            panic!("Static memory allocation cannot happen after malloc has been used. This is to prevent overriding dynamically allocated memory.");
        }

        // Reserve the specified memory region by removing it from the free slots.
        self.reserve_exact(pointer, len);

        return Ok(Pointer::new(pointer, self as *mut dyn PersistentRandomAccessMemory));
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
    fn malloc(&mut self, len: usize) -> Result<Pointer, Error> {
        self.malloc_called = true;

        // Find a free slot that can accommodate the requested length
        if let Ok(allocated_pointer) = self.reserve(len) {
            return Ok(Pointer::new(allocated_pointer, self as *mut dyn PersistentRandomAccessMemory));
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
    fn free(&mut self, pointer: Pointer, len: usize) -> Result<(), Error> {
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
    fn persist(&mut self) -> Result<(), Error> {
        // write the current cached page if it is unsynced
        self.flush_page()?;


        // Iterate over unsynced pages and flush them
        for page_index in self.unsynced_pages.drain() {
            let path = Self::get_page_path(&self.path, page_index);
            let mut file = std::fs::File::create(&path).map_err(|_| Error::WriteError)?;

            // Write the data to the file
            file.flush().map_err(|_| Error::FlushError)?;
            file.sync_all().map_err(|_| Error::FlushError)?;
        }

        Ok(())
    }

    fn read(&mut self, pointer: u64, buf: &mut [u8]) -> Result<(), Error> {
        let len = buf.len();

        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire requested data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= Self::PAGE_SIZE {
            buf.copy_from_slice(&self.cached_page[page_offset..page_offset + len]);
            Ok(())
        } else {
            // Data spans multiple pages, copy data into a temporary buffer
            let mut remaining = len;
            let mut current_pointer = pointer;
            while remaining > 0 {
                let page_index = self.get_page_index(current_pointer);
                self.ensure_page_loaded(page_index)?;

                let page_offset = self.get_page_offset(current_pointer);
                let to_copy = std::cmp::min(remaining, Self::PAGE_SIZE - page_offset);

                // Copy data from the current page to the buffer
                buf[len - remaining..len - remaining + to_copy]
                    .copy_from_slice(&self.cached_page[page_offset..page_offset + to_copy]);

                current_pointer += to_copy as u64;
                remaining -= to_copy;
            }
            Ok(())
        }
    }
    
    fn unsafe_read(&mut self, pointer: u64, len: usize) -> Result<&[u8], Error> {
        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire requested data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= Self::PAGE_SIZE {
            Ok(&self.cached_page[page_offset..page_offset + len])
        } else {
            // Data spans multiple pages, cannot provide a continuous slice
            Err(Error::NotOnOnePageError)
        }
    }
    
    fn write(&mut self, pointer: u64, buf: &[u8]) -> Result<(), Error> {
        let len = buf.len();

        // if the page containing the pointer is not loaded, load it
        let page_index = self.get_page_index(pointer);
        self.ensure_page_loaded(page_index)?;

        // Check if the entire data fits within the current page
        let page_offset = self.get_page_offset(pointer);
        if page_offset + len <= Self::PAGE_SIZE {
            self.cached_page[page_offset..page_offset + len].copy_from_slice(buf);
            Ok(())
        } else {
            // Data spans multiple pages, copy data from a temporary buffer
            let mut remaining = len;
            let mut current_pointer = pointer;
            while remaining > 0 {
                let page_index = self.get_page_index(current_pointer);
                self.ensure_page_loaded(page_index)?;

                let page_offset = self.get_page_offset(current_pointer);
                let to_copy = std::cmp::min(remaining, Self::PAGE_SIZE - page_offset);

                // Copy data from the buffer to the current page
                self.cached_page[page_offset..page_offset + to_copy]
                    .copy_from_slice(&buf[len - remaining..len - remaining + to_copy]);

                current_pointer += to_copy as u64;
                remaining -= to_copy;
            }
            Ok(())
        }
    }
}

impl FilePersistentRandomAccessMemory {
    const PAGE_SIZE: usize = 4096;

    pub fn new(size: usize, path: &str) -> Self {
        // Ensure size is a multiple of PAGE_SIZE
        if (size % Self::PAGE_SIZE) != 0 {
            panic!("Size must be a multiple of PAGE_SIZE (4096 bytes)");
        }

        // Be aware that this is a very naive free slot management
        // and the free list will not be persisted across restarts.
        let mut free_slots = Vec::new();
        free_slots.push((0, size));

        // Initialize the persistent random access memory
        for _page_index in 0..(size / Self::PAGE_SIZE) {
            let page_path = Self::get_page_path(path, _page_index);
            let mut file = std::fs::File::create(&page_path).expect("Failed to create page file");
            let empty_page = vec![0u8; Self::PAGE_SIZE];
            file.write_all(&empty_page).expect("Failed to initialize page file");
        }

        Self {
            cached_page: vec![0; Self::PAGE_SIZE],
            path: path.to_string(),
            current_page_index: None,
            unsynced_pages: HashSet::new(),
            free_slots: free_slots,
            malloc_called: false,
        }
    }

    /// Returns the current loaded page index, or None if no page is loaded.
    pub fn get_current_page_index(&self) -> Option<usize> {
        self.current_page_index
    }

    /// Reserves memory of the specified length from the free slots.
    /// 
    /// Parameters:
    /// - len: The length of memory to reserve.
    /// 
    /// Returns:
    /// - Result containing the starting pointer of the reserved memory on success or Error on failure.
    fn reserve(&mut self, len:usize) -> Result<u64, Error> {
        // Find a free slot that can accommodate the requested length
        // First, try to find a free slot where the allocation can fit entirely within a single page.
        // This avoids allocations that span pages when possible.
        if len <= Self::PAGE_SIZE {
            let page_size = Self::PAGE_SIZE as u64;
            for (free_pointer, free_len) in &self.free_slots {
                if *free_len < len {
                    continue;
                }
                let free_start = *free_pointer;
                let free_end = free_start + (*free_len as u64);

                // If starting at free_start fits within the page, use it.
                let r = free_start % page_size;
                if r + (len as u64) <= page_size {
                    let allocated_pointer = free_start;
                    self.reserve_exact(allocated_pointer, len);
                    return Ok(allocated_pointer);
                }

                // Otherwise try to move to the next page boundary (s.t. s % page_size == 0)
                // which will allow len bytes to fit in that page, if the free slot is large enough.
                let shift = page_size - r;
                let candidate = free_start + shift;
                if candidate + (len as u64) <= free_end {
                    let allocated_pointer = candidate;
                    self.reserve_exact(allocated_pointer, len);
                    return Ok(allocated_pointer);
                }
            }
        }

        // Fallback: first-fit allocation (original behavior) if no single-page slot was found
        for (free_pointer, free_len) in &self.free_slots {
            if *free_len >= len {
            let allocated_pointer = *free_pointer;
            // Reserve the specified memory region by removing it from the free slots.
            self.reserve_exact(allocated_pointer, len);
            return Ok(allocated_pointer);
            }
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
    fn reserve_exact(&mut self, pointer: u64, len: usize) {
        // Allow allocation that spans multiple free slots or partially overlaps free slots,
        // but ensure the full requested region is covered by free space.
        let mut indices_to_remove: Vec<usize> = Vec::new();
        let mut slots_to_add: Vec<(u64, usize)> = Vec::new();

        let alloc_start = pointer;
        let alloc_end = pointer + (len as u64);
        let mut covered_bytes: usize = 0;

        for (index, (free_pointer, free_len)) in self.free_slots.iter().enumerate() {
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
            self.free_slots.remove(*idx);
        }

        // Add back any split fragments.
        self.free_slots.extend(slots_to_add);
    }

    /// Frees the specified memory region and merges it with adjacent free slots.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// - len: The length of the memory region.
    /// 
    /// Returns:
    /// - None.
    fn free_and_merge(&mut self, pointer: u64, len: usize) {
        let mut new_start = pointer;
        let mut new_end = pointer + (len as u64);

        // Collect indices of free slots that overlap or are adjacent to the region.
        let mut indices_to_remove: Vec<usize> = Vec::new();
        for (idx, (free_pointer, free_len)) in self.free_slots.iter().enumerate() {
            let free_start = *free_pointer;
            let free_end = free_start + (*free_len as u64);

            // If the free slot is not completely before or after the region, merge it.
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

        // Remove merged slots in reverse order so indices remain valid.
        indices_to_remove.sort_unstable();
        for idx in indices_to_remove.iter().rev() {
            self.free_slots.remove(*idx);
        }

        // Insert the merged slot.
        self.free_slots.push((new_start, (new_end - new_start) as usize));

        // Keep free_slots ordered by pointer for easier future operations.
        self.free_slots.sort_by_key(|(p, _)| *p);
    }

    /// Calculates the page index for a given pointer.
    ///     
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - The page index as usize.
    fn get_page_index(&self, pointer: u64) -> usize {
        (pointer as usize) / Self::PAGE_SIZE
    }

    /// Calculates the offset within a page for a given pointer.
    ///
    /// Parameters:
    /// - pointer: The memory address as a u64.
    /// 
    /// Returns:
    /// - The offset within the page as usize.
    fn get_page_offset(&self, pointer: u64) -> usize {
        (pointer as usize) % Self::PAGE_SIZE
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
    fn load_page(&mut self, page_index: usize) -> Result<(), Error> {
        let path = Self::get_page_path(&self.path, page_index);
        let mut file = std::fs::File::open(&path).map_err(|_| Error::FileReadError)?;
        file.read(&mut self.cached_page).map_err(|_| Error::FileReadError)?;
        self.current_page_index = Some(page_index);
        Ok(())
    }

    /// Flushes the specified page from memory to persistent storage.
    ///     
    /// Parameters:
    /// - page_index: The index of the page to flush.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn flush_page(&mut self) -> Result<(), Error> {
        if self.current_page_index.is_none() {
            return Ok(());
        }

        let path = format!("{}.page{}", self.path, self.current_page_index.unwrap());
        let mut file = std::fs::File::create(&path).map_err(|_| Error::WriteError)?;
        file.write_all(&self.cached_page).map_err(|_| Error::WriteError)?;

        // Mark the page as unsynced for later persistence
        self.unsynced_pages.insert(self.current_page_index.unwrap());

        Ok(())
    }

    /// Ensures that the specified page is loaded into memory.
    /// 
    /// Parameters:
    /// - page_index: The index of the page to ensure is loaded.
    /// 
    /// Returns:
    /// - Result indicating success or failure.
    fn ensure_page_loaded(&mut self, page_index: usize) -> Result<(), Error> {
        if self.current_page_index != Some(page_index) {
            // Swap to the requested page
            self.flush_page()?;
            return self.load_page(page_index);
        }
        Ok(())
    }
}