#[cfg(test)]
mod tests {
    use super::super::persistent_random_access_memory::{
        FilePersistentRandomAccessMemory, PersistentRandomAccessMemory, Error
    };
    use std::{fs, rc::Rc};

    const TEST_SIZE: usize = 16384; // 4 pages
    const PAGE_SIZE: usize = 4096;
    const TEST_PATH: &str = "C:/data/test_persistent_memory.ignore";

    fn cleanup_test_files() {
        let _ = fs::remove_file(format!("{}.fpram", TEST_PATH));
        let _ = fs::remove_file(format!("{}.swap.fpram", TEST_PATH));
    }

    fn create_test_memory() -> Rc<FilePersistentRandomAccessMemory> {
        cleanup_test_files();
        // Create a new persistent RAM instance for testing
        // use low capacity and history length for faster tests and easier cache eviction tests
        FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH, PAGE_SIZE, 2, 2, 1)
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
        FilePersistentRandomAccessMemory::new(100, TEST_PATH, PAGE_SIZE, 16, 2, 1);
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
        let result = memory.smalloc(0, 128);
        assert!(result.is_ok());
        cleanup_test_files();
    }

    #[test]
    #[should_panic(expected = "Static memory allocation cannot happen after malloc")]
    fn test_salloc_after_malloc_panics() {
        let memory = create_test_memory();
        let _ = memory.malloc(64);
        let _ = memory.smalloc(0, 128);
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
        let ptr1_addr = ptr1.address;
        
        memory.free(ptr1, 64).unwrap();
        
        let ptr2 = memory.malloc(64).unwrap();
        assert_eq!(ptr2.address, ptr1_addr); // Should reuse the freed space
        
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
        assert!(large_ptr.address == 0); // Should start at the beginning
        
        cleanup_test_files();
    }

    #[test]
    fn test_cross_page_write_and_read() {
        let memory = create_test_memory();
        
        // Allocate near page boundary
        let page_size = PAGE_SIZE;
        let ptr = memory.smalloc((page_size - 4) as u64, 8).unwrap();
        
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
        let page_size = PAGE_SIZE;
        let mut ptr = memory.smalloc((page_size - 4) as u64, 8).unwrap();
        
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
        let page_size = PAGE_SIZE;
        let ptr1 = memory.malloc(page_size - 100).unwrap();
        assert_eq!(ptr1.address, 0);
        
        // Next allocation should skip the 100-byte gap and start at the next page
        // to ensure the allocation fits within a single page
        let ptr2 = memory.malloc(128).unwrap();
        assert_eq!(ptr2.address, page_size as u64);
        
        cleanup_test_files();
    }

    #[test]
    fn test_reserve_exact_multiple_slots() {
        let memory = create_test_memory();
        
        // Create fragmented free space
        let ptr1 = memory.smalloc(0,100).unwrap();
        let ptr2 = memory.smalloc(100, 100).unwrap();
        let ptr3 = memory.smalloc(200, 100).unwrap();
        
        memory.free(ptr1, 100).unwrap();
        memory.free(ptr3, 100).unwrap();
        memory.free(ptr2, 100).unwrap();
        
        // Static allocation spanning freed regions
        let result = memory.smalloc(0, 300);
        assert!(result.is_ok());
        
        cleanup_test_files();
    }

    #[test]
    #[should_panic(expected = "Attempted to reserve a slot that is not fully free")]
    fn test_reserve_exact_panics_on_overlap() {
        let memory = create_test_memory();
        
        let _ptr1 = memory.smalloc(0,100).unwrap();
        
        // Try to allocate overlapping memory
        let _ = memory.smalloc(50, 100);
        
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
        memory.write(ptr.address, slice).unwrap();
        
        // Read back
        let mut read_buffer = vec![0u8; data_size];
        memory.read(ptr.address, &mut read_buffer).unwrap();
        
        assert_eq!(test_data, read_buffer);
        
        cleanup_test_files();
    }

    #[test]
    fn test_page_swapping() {
        let memory = create_test_memory();
        
        // Write to page 0
        let mut ptr1 = memory.smalloc(0, 8).unwrap();
        let val1: u64 = 0xAAAA;
        ptr1.write(&val1).unwrap();
        
        // Write to page 2 (forces page swap)
        let mut ptr2 = memory.smalloc(8192, 8).unwrap();
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
        let mut ptr1 = memory.smalloc(0, 8).unwrap();
        let mut ptr2 = memory.smalloc(4096, 8).unwrap();
        let mut ptr3 = memory.smalloc(8192, 8).unwrap();
        
        ptr1.write(&1u64).unwrap();
        ptr2.write(&2u64).unwrap();
        ptr3.write(&3u64).unwrap();
        
        // Persist should flush all unsynced pages
        memory.persist().unwrap();
        
        // Verify pages were written to disk
        assert!(std::path::Path::new(&format!("{}.fpram", TEST_PATH)).exists());
        assert!(std::path::Path::new(&format!("{}.swap.fpram", TEST_PATH)).exists());
        
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
        assert!(new_ptr1.address < new_ptr2.address);
        
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
            let read = array.deref_at::<Elem>(i).unwrap();
            assert_eq!(read.0, (i as u64) * 10 + 1);
        }

        cleanup_test_files();
    }

    #[test]
    fn test_stress_many_page_swaps_integrity() {
        let memory = create_test_memory();

        let page_size = PAGE_SIZE as u64;
        let mut p0 = memory.smalloc(0, 8).unwrap();
        let mut p1 = memory.smalloc(page_size, 8).unwrap();
        let mut p2 = memory.smalloc(page_size * 2, 8).unwrap();
        let mut p3 = memory.smalloc(page_size * 3, 8).unwrap();

        let mut vals = [
            0x1111_1111_1111_1111u64,
            0x2222_2222_2222_2222u64,
            0x3333_3333_3333_3333u64,
            0x4444_4444_4444_4444u64,
        ];

        p0.write(&vals[0]).unwrap();
        p1.write(&vals[1]).unwrap();
        p2.write(&vals[2]).unwrap();
        p3.write(&vals[3]).unwrap();

        for i in 0..2000usize {
            let idx = (i * 7) % 4;
            let got = match idx {
                0 => *p0.deref::<u64>().unwrap(),
                1 => *p1.deref::<u64>().unwrap(),
                2 => *p2.deref::<u64>().unwrap(),
                _ => *p3.deref::<u64>().unwrap(),
            };
            assert_eq!(got, vals[idx]);

            // Occasionally rewrite a value to ensure dirty-page handling is exercised.
            if i % 257 == 0 {
                let new_val = vals[idx].wrapping_add(i as u64);
                vals[idx] = new_val; // Update our tracking array
                match idx {
                    0 => p0.write(&new_val).unwrap(),
                    1 => p1.write(&new_val).unwrap(),
                    2 => p2.write(&new_val).unwrap(),
                    _ => p3.write(&new_val).unwrap(),
                }
            }
        }

        cleanup_test_files();
    }

    #[test]
    fn test_concurrent_reads_different_pages() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        // Write to all pages
        for page in 0..4 {
            let mut ptr = memory.smalloc(page * page_size, 8).unwrap();
            let value = (page + 1) as u64 * 0x1111_1111_1111_1111u64;
            ptr.write(&value).unwrap();
            memory.free(ptr, 8).unwrap(); // Free after writing so someone else can allocate.
        }
        
        // Read all pages in sequence multiple times
        for _ in 0..10 {
            for page in 0..4 {
                let ptr = memory.smalloc(page * page_size, 8).unwrap();
                let expected = (page + 1) as u64 * 0x1111_1111_1111_1111u64;
                assert_eq!(*ptr.deref::<u64>().unwrap(), expected);
                memory.free(ptr, 8).unwrap(); // Free after reading so someone else can allocate.
            }
        }
        
        cleanup_test_files();
    }

    #[test]
    fn test_write_read_exact_boundary_cases() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        // Test write at exact page boundary
        let mut ptr = memory.smalloc(page_size, 16).unwrap();
        let data = [0x12u8, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        ptr.write_exact(&data).unwrap();
        
        let mut read_buf = [0u8; 16];
        ptr.read_exact(&mut read_buf).unwrap();
        assert_eq!(data, read_buf);
        
        cleanup_test_files();
    }

    #[test]
    fn test_fragmentation_and_defragmentation() {
        let memory = create_test_memory();
        
        // Create fragmentation
        let ptrs: Vec<_> = (0..10).map(|_| memory.malloc(100).unwrap()).collect();
        
        // Free every other allocation
        for i in (0..10).step_by(2) {
            memory.free(ptrs[i].clone(), 100).unwrap();
        }
        
        // Try to allocate larger block - should find merged space
        let large = memory.malloc(200);
        assert!(large.is_ok());
        
        cleanup_test_files();
    }

    #[test]
    fn test_zero_length_operations() {
        let memory = create_test_memory();
        
        // Zero-length malloc should still return a valid pointer
        let ptr = memory.malloc(0);
        assert!(ptr.is_ok() || matches!(ptr, Err(Error::OutOfMemoryError)));
        
        cleanup_test_files();
    }

    #[test]
    fn test_page_eviction_preserves_dirty_data() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        // Write to more pages than cache can hold (cache holds 2 pages)
        let mut ptrs = Vec::new();
        for page in 0..4 {
            let mut ptr = memory.smalloc(page * page_size, 8).unwrap();
            let value = 0xAAAA_0000_0000_0000u64 | page;
            ptr.write(&value).unwrap();
            ptrs.push((ptr, value));
        }
        
        // Read back all values - forces eviction and reload
        for (ptr, expected_val) in &ptrs {
            let read_val = ptr.deref::<u64>().unwrap();
            assert_eq!(*read_val, *expected_val);
        }
        
        cleanup_test_files();
    }

    #[test]
    fn test_alternating_page_access_pattern() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        let mut p0 = memory.smalloc(0, 8).unwrap();
        let mut p1 = memory.smalloc(page_size * 3, 8).unwrap();
        
        p0.write(&0xAAAAu64).unwrap();
        p1.write(&0xBBBBu64).unwrap();
        
        // Alternate between pages many times
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(*p0.deref::<u64>().unwrap(), 0xAAAAu64);
            } else {
                assert_eq!(*p1.deref::<u64>().unwrap(), 0xBBBBu64);
            }
        }
        
        cleanup_test_files();
    }

    #[test]
    fn test_malloc_fills_gaps_efficiently() {
        let memory = create_test_memory();
        
        // Allocate three blocks
        let _ = memory.malloc(100).unwrap();
        let p2 = memory.malloc(100).unwrap();
        let _ = memory.malloc(100).unwrap();
        
        // Free the middle one
        memory.free(p2.clone(), 100).unwrap();
        
        // Allocate smaller block - should fill the gap
        let p4 = memory.malloc(50).unwrap();
        assert_eq!(p4.address, p2.address);
        
        cleanup_test_files();
    }

    #[test]
    fn test_persist_and_reload_entire_memory() {
        // Fresh start
        cleanup_test_files();
        let memory = FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH, PAGE_SIZE, 16, 2, 1);

        // Fill entire memory with a deterministic pattern
        let total = TEST_SIZE;
        let mut buf = vec![0u8; total];
        for i in 0..total {
            buf[i] = (((i as u32).wrapping_mul(73).wrapping_add(19)) & 0xFF) as u8;
        }

        memory.write(0, &buf).unwrap();
        memory.persist().unwrap();

        // Drop and reload
        drop(memory);
        let memory2 = FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH, PAGE_SIZE, 16, 2, 1);

        let mut read_back = vec![0u8; total];
        memory2.read(0, &mut read_back).unwrap();
        assert_eq!(buf, read_back);

        cleanup_test_files();
    }

    #[test]
    fn test_spanning_write_across_multiple_pages_persist_and_reload() {
        let page_size =  PAGE_SIZE as u64;
        let start = page_size - 50;                  // start near end of page 0
        let len = (page_size as usize) * 2 + 200;    // span three pages in total range

        let memory = create_test_memory();

        // Create data pattern that spans multiple pages
        let mut pattern = vec![0u8; len];
        for i in 0..len {
            pattern[i] = (((i as u32) * 37 + 11) & 0xFF) as u8;
        }

        memory.write(start, &pattern).unwrap();
        memory.persist().unwrap();
        drop(memory);

        // Reload and verify
        let memory2 = FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH, PAGE_SIZE, 16, 2, 1);
        let mut read_back = vec![0u8; len];
        memory2.read(start, &mut read_back).unwrap();

        assert_eq!(pattern, read_back);

        cleanup_test_files();
    }

    #[test]
    fn test_index_across_pages_large_array() {
        #[repr(C)]
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct Elem(u64);

        let memory = create_test_memory();

        // Allocate enough elements to cross at least two pages
        let elem_count = 1000usize; // 1000 * 8 = 8000 bytes (> 1 page)
        let total_size = elem_count * std::mem::size_of::<Elem>();
        let array = memory.malloc(total_size).unwrap();

        // Write values with indexing
        for i in 0..elem_count {
            let mut p = array.at::<Elem>(i);
            let v = Elem(((i as u64) << 32) ^ 0xDEAD_BEEF_u64 ^ (i as u64));
            p.write(&v).unwrap();
        }

        // Read back in reverse order to force cache churn across pages
        for i in (0..elem_count).rev() {
            let v = array.deref_at::<Elem>(i).unwrap();
            let expected = Elem(((i as u64) << 32) ^ 0xDEAD_BEEF_u64 ^ (i as u64));
            assert_eq!(v, Box::new(expected));
        }

        cleanup_test_files();
    }

    #[test]
    fn test_persist_twice_with_interleaved_swaps() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;

        // Place four u64s on distinct pages
        let mut p0 = memory.smalloc(0, 8).unwrap();
        let mut p1 = memory.smalloc(page_size-1, 8).unwrap();
        let mut p2 = memory.smalloc(page_size * 2 -2, 8).unwrap();
        let mut p3 = memory.smalloc(page_size * 3 -3, 8).unwrap();

        let vals_a = [
            0xAAAA_AAAA_AAAA_AAAAu64,
            0xBBBB_BBBB_BBBB_BBBBu64,
            0xCCCC_CCCC_CCCC_CCCCu64,
            0xDDDD_DDDD_DDDD_DDDDu64,
        ];
        p0.write(&vals_a[0]).unwrap();
        p1.write(&vals_a[1]).unwrap();
        p2.write(&vals_a[2]).unwrap();
        p3.write(&vals_a[3]).unwrap();

        memory.persist().unwrap();

        // Thrash pages by repeatedly accessing them in a non-trivial pattern
        for i in 0..600usize {
            match (i * 5) % 4 {
                0 => assert_eq!(*p0.deref::<u64>().unwrap(), vals_a[0]),
                1 => assert_eq!(*p1.deref::<u64>().unwrap(), vals_a[1]),
                2 => assert_eq!(*p2.deref::<u64>().unwrap(), vals_a[2]),
                _ => assert_eq!(*p3.deref::<u64>().unwrap(), vals_a[3]),
            }
        }

        // Overwrite with new values and persist again
        let vals_b = [
            0x1111_2222_3333_4444u64,
            0x5555_6666_7777_8888u64,
            0x9999_AAAA_BBBB_CCCCu64,
            0xDDDD_EEEE_FFFF_0000u64,
        ];
        p0.write(&vals_b[0]).unwrap();
        p1.write(&vals_b[1]).unwrap();
        p2.write(&vals_b[2]).unwrap();
        p3.write(&vals_b[3]).unwrap();

        memory.persist().unwrap();
        drop(memory);

        // Reopen and verify second set of values survived
        let memory2 = FilePersistentRandomAccessMemory::new(TEST_SIZE, TEST_PATH, PAGE_SIZE, 16, 2, 1);
        let p0r = memory2.smalloc(0, 8).unwrap();
        let p1r = memory2.smalloc(page_size -1, 8).unwrap();
        let p2r = memory2.smalloc(page_size * 2 -2, 8).unwrap();
        let p3r = memory2.smalloc(page_size * 3 -3, 8).unwrap();

        assert_eq!(*p0r.deref::<u64>().unwrap(), vals_b[0]);
        assert_eq!(*p1r.deref::<u64>().unwrap(), vals_b[1]);
        assert_eq!(*p2r.deref::<u64>().unwrap(), vals_b[2]);
        assert_eq!(*p3r.deref::<u64>().unwrap(), vals_b[3]);

        cleanup_test_files();
    }

    #[test]
    fn test_cache_thrashing_randomized_like_access_integrity() {
        let memory = create_test_memory();

        // Reserve the whole space so we can freely read/write by absolute offsets
        let _guard = memory.smalloc(0, TEST_SIZE).unwrap();

        // Use a set of u64 positions spread across the address space
        let count = (TEST_SIZE / 8).min(1024); // cap work
        let stride = 257usize; // coprime-ish stride to spread across pages
        let mut written = Vec::with_capacity(count);

        // Write phase
        for i in 0..count {
            let idx = (i * stride) % count;
            let byte_off = (idx * 8) as u64;
            let val = (byte_off ^ 0xDEAD_BEEF_CAFE_BABE_u64).wrapping_add(i as u64);
            memory.write(byte_off, &val.to_le_bytes()).unwrap();
            written.push((byte_off, val));
        }

        // Thrash by reading back in a different order
        let stride2 = 73usize;
        for i in 0..count {
            let idx = (i * stride2) % count;
            let (off, val) = written[idx];
            let mut buf = [0u8; 8];
            memory.read(off, &mut buf).unwrap();
            let got = u64::from_le_bytes(buf);
            assert_eq!(got, val);
        }

        cleanup_test_files();
    }
}