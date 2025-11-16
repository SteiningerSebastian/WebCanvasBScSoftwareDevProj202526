#[cfg(test)]
mod tests {
    use super::super::persistent_random_access_memory::{
        FilePersistentRandomAccessMemory, PersistentRandomAccessMemory, Error
    };
    use std::{fs, rc::Rc};

    const TEST_SIZE: usize = 16384; // 4 pages
    const TEST_PATH: &str = "C:/Users/sebas/Repos/WebCanvas/Backend/general/testdata/test_persistent_memory";

    fn cleanup_test_files() {
        let _ = fs::remove_file(format!("{}.page0", TEST_PATH));
        let _ = fs::remove_file(format!("{}.page1", TEST_PATH));
        let _ = fs::remove_file(format!("{}.page2", TEST_PATH));
        let _ = fs::remove_file(format!("{}.page3", TEST_PATH));
    }

    fn create_test_memory() -> Rc<FilePersistentRandomAccessMemory> {
        cleanup_test_files();
        FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH)
    }

    #[test]
    fn test_new_creates_memory() {
        let memory = create_test_memory();
        assert!(memory.get_current_page_index().is_none());
        cleanup_test_files();
    }

    #[test]
    #[should_panic(expected = "Size must be a multiple of PAGE_SIZE")]
    fn test_new_panics_on_invalid_size() {
        FilePersistentRandomAccessMemory::new(100, TEST_PATH);
    }

    #[test]
    fn test_malloc_basic() {
        let memory = create_test_memory();
        let result = memory.malloc(64);
        assert!(result.is_ok());
        cleanup_test_files();
    }

    #[test]
    fn test_malloc_out_of_memory() {
        let memory = create_test_memory();
        let result = memory.malloc(TEST_SIZE + 1);
        assert!(matches!(result, Err(Error::OutOfMemoryError)));
        cleanup_test_files();
    }

    #[test]
    fn test_salloc_basic() {
        let memory = create_test_memory();
        let result = memory.salloc(0, 128);
        assert!(result.is_ok());
        cleanup_test_files();
    }

    #[test]
    #[should_panic(expected = "Static memory allocation cannot happen after malloc")]
    fn test_salloc_after_malloc_panics() {
        let memory = create_test_memory();
        let _ = memory.malloc(64);
        let _ = memory.salloc(0, 128);
        cleanup_test_files();
    }

    #[test]
    fn test_write_and_read_u64() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc(8).unwrap();
        
        let value: u64 = 0x1234567890ABCDEF;
        ptr.write(&value).unwrap();
        
        let read_value = ptr.deref::<u64>().unwrap();
        assert_eq!(*read_value, value);
        
        cleanup_test_files();
    }

    #[test]
    fn test_write_and_read_struct() {
        #[derive(Debug, PartialEq)]
        #[repr(C)]
        struct TestStruct {
            a: u32,
            b: u64,
            c: u16,
        }
        
        let memory = create_test_memory();
        let mut ptr = memory.malloc(std::mem::size_of::<TestStruct>()).unwrap();
        
        let value = TestStruct { a: 42, b: 0xDEADBEEF, c: 255 };
        ptr.write(&value).unwrap();
        
        let read_value = ptr.deref::<TestStruct>().unwrap();
        assert_eq!(*read_value, value);
        
        cleanup_test_files();
    }

    #[test]
    fn test_write_and_read_array() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc(40).unwrap();
        
        let value: [u64; 5] = [1, 2, 3, 4, 5];
        ptr.write(&value).unwrap();
        
        let read_value = ptr.deref::<[u64; 5]>().unwrap();
        assert_eq!(*read_value, value);
        
        cleanup_test_files();
    }

    #[test]
    fn test_multiple_allocations() {
        let memory = create_test_memory();
        
        let mut ptr1 = memory.malloc(64).unwrap();
        let mut ptr2 = memory.malloc(128).unwrap();
        let mut ptr3 = memory.malloc(256).unwrap();
        
        let val1: u64 = 111;
        let val2: u64 = 222;
        let val3: u64 = 333;
        
        ptr1.write(&val1).unwrap();
        ptr2.write(&val2).unwrap();
        ptr3.write(&val3).unwrap();
        
        assert_eq!(*ptr1.deref::<u64>().unwrap(), val1);
        assert_eq!(*ptr2.deref::<u64>().unwrap(), val2);
        assert_eq!(*ptr3.deref::<u64>().unwrap(), val3);
        
        cleanup_test_files();
    }

    #[test]
    fn test_free_and_realloc() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc(64).unwrap();
        let ptr1_addr = ptr1.pointer;
        
        memory.free(ptr1, 64).unwrap();
        
        let ptr2 = memory.malloc(64).unwrap();
        assert_eq!(ptr2.pointer, ptr1_addr); // Should reuse the freed space
        
        cleanup_test_files();
    }

    #[test]
    fn test_free_merges_adjacent_slots() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc(64).unwrap();
        let ptr2 = memory.malloc(64).unwrap();
        let ptr3 = memory.malloc(64).unwrap();
        
        memory.free(ptr1, 64).unwrap();
        memory.free(ptr3, 64).unwrap();
        memory.free(ptr2, 64).unwrap();
        
        // After freeing all three adjacent blocks, we should be able to allocate 192 bytes
        let large_ptr = memory.malloc(192).unwrap();
        assert!(large_ptr.pointer == 0); // Should start at the beginning
        
        cleanup_test_files();
    }

    #[test]
    fn test_cross_page_write_and_read() {
        let memory = create_test_memory();
        
        // Allocate near page boundary
        let page_size = 4096;
        let ptr = memory.salloc((page_size - 4) as u64, 8).unwrap();
        
        let mut ptr_mut = ptr;
        let value: u64 = 0xFEDCBA9876543210;
        ptr_mut.write(&value).unwrap();
        
        let read_value = ptr_mut.deref::<u64>().unwrap();
        assert_eq!(*read_value, value);
        
        cleanup_test_files();
    }

    #[test]
    fn test_persist() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc(8).unwrap();
        
        let value: u64 = 0x123456789ABCDEF0;
        ptr.write(&value).unwrap();
        
        let result = memory.persist();
        assert!(result.is_ok());
        
        cleanup_test_files();
    }

    #[test]
    fn test_unsafe_deref_within_page() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc(8).unwrap();
        
        let value: u64 = 0xCAFEBABE;
        ptr.write(&value).unwrap();
        
        let read_value = ptr.unsafe_deref::<u64>().unwrap();
        assert_eq!(*read_value.unwrap(), value);
        
        cleanup_test_files();
    }

    #[test]
    fn test_unsafe_deref_cross_page_returns_none() {
        let memory = create_test_memory();
        
        // Allocate near page boundary so u64 spans pages
        let page_size = 4096;
        let mut ptr = memory.salloc((page_size - 4) as u64, 8).unwrap();
        
        let value: u64 = 0xDEADBEEF;
        ptr.write(&value).unwrap();
        
        let result = ptr.unsafe_deref::<u64>().unwrap();
        assert!(result.is_none()); // Should return None because data spans pages
        
        cleanup_test_files();
    }

    #[test]
    fn test_unsafe_deref_mut_within_page() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc(8).unwrap();
        
        let value: u64 = 0x1111;
        ptr.write(&value).unwrap();
        
        let mut_ref = ptr.unsafe_deref_mut::<u64>().unwrap().unwrap();
        *mut_ref = 0x2222;
        
        let read_value = ptr.deref::<u64>().unwrap();
        assert_eq!(*read_value, 0x2222);
        
        cleanup_test_files();
    }

    #[test]
    fn test_malloc_prefers_single_page_allocation() {
        let memory = create_test_memory();
        
        // Allocate a large block that leaves a small gap before the page boundary
        let page_size = 4096;
        let ptr1 = memory.malloc(page_size - 100).unwrap();
        assert_eq!(ptr1.pointer, 0);
        
        // Next allocation should skip the 100-byte gap and start at the next page
        // to ensure the allocation fits within a single page
        let ptr2 = memory.malloc(128).unwrap();
        assert_eq!(ptr2.pointer, page_size as u64);
        
        cleanup_test_files();
    }

    #[test]
    fn test_reserve_exact_multiple_slots() {
        let memory = create_test_memory();
        
        // Create fragmented free space
        let ptr1 = memory.salloc(0,100).unwrap();
        let ptr2 = memory.salloc(100, 100).unwrap();
        let ptr3 = memory.salloc(200, 100).unwrap();
        
        memory.free(ptr1, 100).unwrap();
        memory.free(ptr3, 100).unwrap();
        memory.free(ptr2, 100).unwrap();
        
        // Static allocation spanning freed regions
        let result = memory.salloc(0, 300);
        assert!(result.is_ok());
        
        cleanup_test_files();
    }

    #[test]
    #[should_panic(expected = "Attempted to reserve a slot that is not fully free")]
    fn test_reserve_exact_panics_on_overlap() {
        let memory = create_test_memory();
        
        let _ptr1 = memory.salloc(0,100).unwrap();
        
        // Try to allocate overlapping memory
        let _ = memory.salloc(50, 100);
        
        cleanup_test_files();
    }

    #[test]
    fn test_large_data_across_multiple_pages() {
        let memory = create_test_memory();
        
        let data_size = 8192; // 2 pages
        let ptr = memory.malloc(data_size).unwrap();
        
        // Create test data
        let mut test_data = vec![0u8; data_size];
        for i in 0..data_size {
            test_data[i] = (i % 256) as u8;
        }
        
        // Write data
        let slice = unsafe {
            std::slice::from_raw_parts(test_data.as_ptr(), data_size)
        };
        memory.write(ptr.pointer, slice).unwrap();
        
        // Read back
        let mut read_buffer = vec![0u8; data_size];
        memory.read(ptr.pointer, &mut read_buffer).unwrap();
        
        assert_eq!(test_data, read_buffer);
        
        cleanup_test_files();
    }

    #[test]
    fn test_page_swapping() {
        let memory = create_test_memory();
        
        // Write to page 0
        let mut ptr1 = memory.salloc(0, 8).unwrap();
        let val1: u64 = 0xAAAA;
        ptr1.write(&val1).unwrap();
        
        // Write to page 2 (forces page swap)
        let mut ptr2 = memory.salloc(8192, 8).unwrap();
        let val2: u64 = 0xBBBB;
        ptr2.write(&val2).unwrap();
        
        // Read from page 0 again (forces another page swap)
        let read1 = ptr1.deref::<u64>().unwrap();
        assert_eq!(*read1, val1);
        
        cleanup_test_files();
    }

    #[test]
    fn test_persist_flushes_all_pages() {
        let memory = create_test_memory();
        
        // Write to multiple pages
        let mut ptr1 = memory.salloc(0, 8).unwrap();
        let mut ptr2 = memory.salloc(4096, 8).unwrap();
        let mut ptr3 = memory.salloc(8192, 8).unwrap();
        
        ptr1.write(&1u64).unwrap();
        ptr2.write(&2u64).unwrap();
        ptr3.write(&3u64).unwrap();
        
        // Persist should flush all unsynced pages
        memory.persist().unwrap();
        
        // Verify pages were written to disk
        assert!(std::path::Path::new(&format!("{}.page0", TEST_PATH)).exists());
        assert!(std::path::Path::new(&format!("{}.page1", TEST_PATH)).exists());
        assert!(std::path::Path::new(&format!("{}.page2", TEST_PATH)).exists());
        
        cleanup_test_files();
    }

    #[test]
    fn test_malloc_exhausts_memory() {
        let memory = create_test_memory();
        
        let mut allocations = Vec::new();
        let alloc_size = 1024;
        
        // Keep allocating until we run out
        loop {
            match memory.malloc(alloc_size) {
                Ok(ptr) => allocations.push(ptr),
                Err(Error::OutOfMemoryError) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        
        // Should have allocated most of the available space
        assert!(allocations.len() >= (TEST_SIZE / alloc_size) - 1);
        
        cleanup_test_files();
    }

    #[test]
    fn test_free_slots_remain_sorted() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc(100).unwrap();
        let ptr2 = memory.malloc(100).unwrap();
        let ptr3 = memory.malloc(100).unwrap();
        
        // Free in non-sequential order
        memory.free(ptr3, 100).unwrap();
        memory.free(ptr1, 100).unwrap();
        memory.free(ptr2, 100).unwrap();
        
        // Verify free_slots are sorted by checking allocation order
        let new_ptr1 = memory.malloc(50).unwrap();
        let new_ptr2 = memory.malloc(50).unwrap();
        assert!(new_ptr1.pointer < new_ptr2.pointer);
        
        cleanup_test_files();
    }

    #[test]
    fn test_index_into_salloc_array() {
        #[repr(C)]
        struct Elem(u64);

        let memory = create_test_memory();

        // allocate an array of 10 u64 elements via static allocation
        let elem_count = 10;
        let total_size = (elem_count * size_of::<Elem>()) as usize;
        let array = memory.malloc( total_size).unwrap();

        // write distinct values into each element using .index(i)
        for i in 0..elem_count {
            let mut elem_ptr = array.at::<Elem>(i);
            let val = Elem((i as u64) * 10 + 1);
            elem_ptr.write(&val).unwrap();
        }

        // read them back and verify values
        for i in 0..elem_count {
            let mut elem_ptr = array.at::<Elem>(i);
            let read = elem_ptr.deref::<Elem>().unwrap();
            assert_eq!(read.0, (i as u64) * 10 + 1);
        }

        cleanup_test_files();
    }
}