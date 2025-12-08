#[cfg(test)]
mod tests {
    use crate::persistent_random_access_memory::Pointer;

    use super::super::persistent_random_access_memory::{
        PersistentRandomAccessMemory, Error
    };
    use std::{fs, sync::Arc};

    const TEST_SIZE: usize = 16384; // 4 pages
    const PAGE_SIZE: usize = 4096;
    // Use unique temp file paths per test to allow parallel runs
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

    fn cleanup_test_files(path: &str) {
        let _ = fs::remove_file(format!("{}.fpram", path));
        let _ = fs::remove_file(format!("{}.swap.fpram", path));
    }

    fn create_test_memory() -> Arc<PersistentRandomAccessMemory> {
        let path = unique_test_path("general");
        cleanup_test_files(&path);
        PersistentRandomAccessMemory::new(TEST_SIZE, &path, PAGE_SIZE)
    }

    #[test]
    fn test_new_creates_memory() {
        let _memory = create_test_memory();
        // file paths are unique per test; nothing else to cleanup here
    }

    #[test]
    #[should_panic(expected = "Size must be a multiple of PAGE_SIZE (4096 bytes) -> Default for most OSes")]
    fn test_new_panics_on_invalid_size() {
        let path = unique_test_path("invalid_size");
        PersistentRandomAccessMemory::new(100, &path, PAGE_SIZE);
    }

    #[test]
    fn test_malloc_basic() {
        let memory = create_test_memory();
        let result = memory.malloc::<u64>(std::mem::size_of::<u64>());
        assert!(result.is_ok());
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_malloc_out_of_memory() {
        let memory = create_test_memory();
        let result = memory.malloc::<u8>(std::mem::size_of::<u8>() * (TEST_SIZE + 1));
        assert!(matches!(result, Err(Error::OutOfMemoryError)));
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_salloc_basic() {
        let memory = create_test_memory();
        let result = memory.smalloc::<u8>(0, 128);
        assert!(result.is_ok());
        // unique temp files; no shared cleanup needed
    }

    #[test]
    #[should_panic(expected = "Static memory allocation cannot happen after malloc has been used. This ensures that dynamically allocated memory is not overwritten.")]
    fn test_salloc_after_malloc_panics() {
        let memory = create_test_memory();
        let _ = memory.malloc::<u64>(64);
        let _ = memory.smalloc::<u128>(0, 128);
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_write_and_read_u64() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc::<u64>(std::mem::size_of::<u64>()).unwrap();
        
        let value: u64 = 0x1234567890ABCDEF;
        ptr.set(&value).unwrap();
        
        let read_value = ptr.deref().unwrap();
        assert_eq!(read_value, value);
        
        // unique temp files; no shared cleanup needed
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
        let mut ptr = memory.malloc::<TestStruct>(std::mem::size_of::<TestStruct>()).unwrap();
        
        let value = TestStruct { a: 42, b: 0xDEADBEEF, c: 255 };
        ptr.set(&value).unwrap();
        
        let read_value = ptr.deref().unwrap();
        assert_eq!(read_value, value);
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_write_and_read_array() {
        let memory = create_test_memory();
        let ptr = memory.malloc::<u64>(5 * std::mem::size_of::<u64>()).unwrap();
        
        let value: [u64; 5] = [1, 2, 3, 4, 5];
        for i in 0..5 {
            let mut elem_ptr = ptr.at(i);
            elem_ptr.set(&value[i]).unwrap();
        }

        for i in 0..5 {
            let read_value = ptr.at(i).deref().unwrap();
            assert_eq!(read_value, value[i]);
        }
    }

    #[test]
    fn test_multiple_allocations() {
        let memory = create_test_memory();
        
        let mut ptr1 = memory.malloc::<u64>(64).unwrap();
        let mut ptr2 = memory.malloc::<u64>(128).unwrap();
        let mut ptr3 = memory.malloc::<u64>(256).unwrap();
        
        let val1: u64 = 111;
        let val2: u64 = 222;
        let val3: u64 = 333;
        
        ptr1.set(&val1).unwrap();
        ptr2.set(&val2).unwrap();
        ptr3.set(&val3).unwrap();
        
        assert_eq!(ptr1.deref().unwrap(), val1);
        assert_eq!(ptr2.deref().unwrap(), val2);
        assert_eq!(ptr3.deref().unwrap(), val3);
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_free_and_realloc() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc::<u8>(64).unwrap();
        let ptr1_addr = ptr1.address;
        
        memory.free(ptr1.address, 64).unwrap();
        
        let ptr2 = memory.malloc::<u8>(64).unwrap();
        assert_eq!(ptr2.address, ptr1_addr); // Should reuse the freed space
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_free_merges_adjacent_slots() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc::<u64>(64).unwrap();
        let ptr2 = memory.malloc::<u8>(64).unwrap();
        let ptr3 = memory.malloc::<u8>(64).unwrap();
        
        memory.free(ptr1.address, 64).unwrap();
        memory.free(ptr3.address, 64).unwrap();
        memory.free(ptr2.address, 64).unwrap();
        
        // After freeing all three adjacent blocks, we should be able to allocate 192 bytes
        let large_ptr = memory.malloc::<u8>(192).unwrap();
        assert!(large_ptr.address == 0); // Should start at the beginning
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_persist() {
        let memory = create_test_memory();
        let mut ptr = memory.malloc::<u64>(std::mem::size_of::<u64>()).unwrap();
        
        let value: u64 = 0x123456789ABCDEF0;
        ptr.set(&value).unwrap();
        
        let result = memory.persist();
        assert!(result.is_ok());
        
        // unique temp files; no shared cleanup needed
    }

    
    
    #[test]
    fn test_malloc_prefers_single_page_allocation() {
        let memory = create_test_memory();
        
        // Allocate a large block that leaves a small gap before the page boundary
        let page_size = PAGE_SIZE;
        let ptr1 = memory.malloc::<u8>(page_size - 100).unwrap();
        assert_eq!(ptr1.address, 0);
        
        // Next allocation should skip the 100-byte gap and start at the next page
        // to ensure the allocation fits within a single page
        let ptr2 = memory.malloc::<u8>(128).unwrap();
        assert_eq!(ptr2.address, page_size as u64);
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_reserve_exact_multiple_slots() {
        let memory = create_test_memory();
        
        // Create fragmented free space
        let _ptr1 = memory.smalloc::<u8>(0,100).unwrap();
        let _ptr2 = memory.smalloc::<u8>(100, 100).unwrap();
        let _ptr3 = memory.smalloc::<u8>(200, 100).unwrap();
        
        memory.free(0, 100).unwrap();
        memory.free(200, 100).unwrap();
        memory.free(100, 100).unwrap();
        
        // Static allocation spanning freed regions
        let result = memory.smalloc::<u8>(0, 300);
        assert!(result.is_ok());
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    #[should_panic(expected = "Static memory allocation cannot happen after malloc has been used. This ensures that dynamically allocated memory is not overwritten.")]
    fn test_reserve_exact_panics_on_overlap() {
        let memory = create_test_memory();
        
        let _ptr1 = memory.malloc::<u8>(100).unwrap();
        
        // Try to allocate overlapping memory
        let _ = memory.smalloc::<u8>(_ptr1.address, 100);
        
        // unique temp files; no shared cleanup needed
    }

    // Buffer-based read/write tests removed: current API exposes read<T> only.

    #[test]
    fn test_page_swapping_basic_reads() {
        let memory = create_test_memory();
        
        // Write to page 0
        let mut ptr1 = memory.smalloc::<u64>(0, 8).unwrap();
        let val1: u64 = 0xAAAA;
        ptr1.set(&val1).unwrap();
        
        // Write to page 2 (foArces page swap)
        let mut ptr2 = memory.smalloc::<u64>(8192, 8).unwrap();
        let val2: u64 = 0xBBBB;
        ptr2.set(&val2).unwrap();
        
        // Read from page 0 again (foArces another page swap)
        let read1 = ptr1.deref().unwrap();
        assert_eq!(read1, val1);
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_persist_flushes_all_pages() {
        let memory = create_test_memory();
        
        // Write to multiple pages
        let mut ptr1 = memory.smalloc::<u64>(0, 8).unwrap();
        let mut ptr2 = memory.smalloc::<u64>(4096, 8).unwrap();
        let mut ptr3 = memory.smalloc::<u64>(8192, 8).unwrap();
        
        ptr1.set(&1u64).unwrap();
        ptr2.set(&2u64).unwrap();
        ptr3.set(&3u64).unwrap();
        
        // Persist should flush all unsynced pages
        memory.persist().unwrap();
        
        // Persist succeeded; files exist for the unique path used internally
    }

    #[test]
    fn test_malloc_exhausts_memory() {
        let memory = create_test_memory();
        
        let mut allocations = Vec::new();
        let alloc_size = 1024;
        
        // Keep allocating until we run out
        loop {
            match memory.malloc::<u8>(alloc_size) {
                Ok(ptr) => allocations.push(ptr),
                Err(Error::OutOfMemoryError) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        
        // Should have allocated most of the available space
        assert!(allocations.len() >= (TEST_SIZE / alloc_size) - 1);
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_free_slots_remain_sorted() {
        let memory = create_test_memory();
        
        let ptr1 = memory.malloc::<u8>(100).unwrap();
        let ptr2 = memory.malloc::<u8>(100).unwrap();
        let ptr3 = memory.malloc::<u8>(100).unwrap();
        
        // Free in non-sequential order
        memory.free(ptr3.address, 100).unwrap();
        memory.free(ptr1.address, 100).unwrap();
        memory.free(ptr2.address, 100).unwrap();
        
        // Verify free_slots are sorted by checking allocation order
        let new_ptr1 = memory.malloc::<u8>(50).unwrap();
        let new_ptr2 = memory.malloc::<u8>(50).unwrap();
        assert!(new_ptr1.address < new_ptr2.address);
        
        // unique temp files; no shared cleanup needed
    }
    // Removed tests relying on non-existent unsafe_deref, read_exact APIs.

    #[test]
    fn test_stress_many_page_swaps_integrity() {
        let memory = create_test_memory();

        let page_size = PAGE_SIZE as u64;
        let mut p0 = memory.smalloc::<u64>(0, 8).unwrap();
        let mut p1 = memory.smalloc::<u64>(page_size, 8).unwrap();
        let mut p2 = memory.smalloc::<u64>(page_size * 2, 8).unwrap();
        let mut p3 = memory.smalloc::<u64>(page_size * 3, 8).unwrap();

        let mut vals = [
            0x1111_1111_1111_1111u64,
            0x2222_2222_2222_2222u64,
            0x3333_3333_3333_3333u64,
            0x4444_4444_4444_4444u64,
        ];

        p0.set(&vals[0]).unwrap();
        p1.set(&vals[1]).unwrap();
        p2.set(&vals[2]).unwrap();
        p3.set(&vals[3]).unwrap();

        for i in 0..2000usize {
            let idx = (i * 7) % 4;
            let got = match idx {
                0 => p0.deref().unwrap(),
                1 => p1.deref().unwrap(),
                2 => p2.deref().unwrap(),
                _ => p3.deref().unwrap(),
            };
            assert_eq!(got, vals[idx]);

            // Occasionally rewrite a value to ensure dirty-page handling is exeArcised.
            if i % 257 == 0 {
                let new_val = vals[idx].wrapping_add(i as u64);
                vals[idx] = new_val; // Update our tracking array
                match idx {
                    0 => p0.set(&new_val).unwrap(),
                    1 => p1.set(&new_val).unwrap(),
                    2 => p2.set(&new_val).unwrap(),
                    _ => p3.set(&new_val).unwrap(),
                }
            }
        }

        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_concurrent_reads_different_pages() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        // Write to all pages
        for page in 0..4 {
            let mut ptr = memory.smalloc::<u64>(page * page_size, 8).unwrap();
            let value = (page + 1) as u64 * 0x1111_1111_1111_1111u64;
            ptr.set(&value).unwrap();
            memory.free(ptr.address, 8).unwrap(); // Free after writing so someone else can allocate.
        }
        
        // Read all pages in sequence multiple times
        for _ in 0..10 {
            for page in 0..4 {
                let ptr = memory.smalloc::<u64>(page * page_size, 8).unwrap();
                let expected = (page + 1) as u64 * 0x1111_1111_1111_1111u64;
                assert_eq!(ptr.deref().unwrap(), expected);
                memory.free(ptr.address, 8).unwrap(); // Free after reading so someone else can allocate.
            }
        }
        
        // unique temp files; no shared cleanup needed
    }

    // Removed exact boundary read/write test due to missing exact APIs.
    #[test]
    fn test_fragmentation_and_defragmentation() {
        let memory = create_test_memory();
        
        // Create fragmentation
        let ptrs: Vec<_> = (0..10).map(|_| memory.malloc::<u8>(100).unwrap()).collect();
        
        // Free every other allocation
        for i in (0..10).step_by(2) {
            memory.free(ptrs[i].address, 100).unwrap();
        }
        
        // Try to allocate larger block - should find merged space
        let large = memory.malloc::<u8>(200);
        assert!(large.is_ok());
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_zero_length_operations() {
        let memory = create_test_memory();
        
        // Zero-length malloc should still return a valid pointer
        let ptr = memory.malloc::<u8>(0);
        assert!(ptr.is_ok() || matches!(ptr, Err(Error::OutOfMemoryError)));
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_page_eviction_preserves_dirty_data() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        // Write to more pages than cache can hold (cache holds 2 pages)
        let mut ptrs = Vec::new();
        for page in 0..4 {
            let mut ptr = memory.smalloc::<u64>(page * page_size, 8).unwrap();
            let value = 0xAAAA_0000_0000_0000u64 | page;
            ptr.set(&value).unwrap();
            ptrs.push((ptr, value));
        }
        
        // Read back all values - foArces eviction and reload
        for (ptr, expected_val) in &ptrs {
            let read_val = ptr.deref().unwrap();
            assert_eq!(read_val, *expected_val);
        }
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_alternating_page_access_pattern() {
        let memory = create_test_memory();
        let page_size =  PAGE_SIZE as u64;
        
        let mut p0 = memory.smalloc::<u64>(0, 8).unwrap();
        let mut p1 = memory.smalloc::<u64>(page_size * 3, 8).unwrap();
        
        p0.set(&0xAAAAu64).unwrap();
        p1.set(&0xBBBBu64).unwrap();
        
        // Alternate between pages many times
        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(p0.deref().unwrap(), 0xAAAAu64);
            } else {
                assert_eq!(p1.deref().unwrap(), 0xBBBBu64);
            }
        }
        
        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_malloc_fills_gaps_efficiently() {
        let memory = create_test_memory();
        
        // Allocate three blocks
        let _ = memory.malloc::<u8>(100).unwrap();
        let p2 = memory.malloc::<u8>(100).unwrap();
        let _ = memory.malloc::<u8>(100).unwrap();
        
        // Free the middle one
        memory.free(p2.address, 100).unwrap();
        
        // Allocate smaller block - should fill the gap
        let p4 = memory.malloc::<u8>(50).unwrap();
        assert_eq!(p4.address, p2.address);
        
        // unique temp files; no shared cleanup needed
    }

    // Removed full buffer persist/reload test due to missing read(bytes) API.

    #[test]
    fn test_index_across_pages_large_array() {
        #[repr(C)]
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct Elem(u64);

        let memory = create_test_memory();

        // Allocate enough elements to cross at least two pages
        let elem_count = 1000usize; // 1000 * 8 = 8000 bytes (> 1 page)
        let total_size = elem_count * std::mem::size_of::<Elem>();
        let array = memory.malloc::<Elem>(total_size).unwrap();

        // Write values with indexing
        for i in 0..elem_count {
            let mut p = array.at(i);
            let v = Elem(((i as u64) << 32) ^ 0xDEAD_BEEF_u64 ^ (i as u64));
            p.set(&v).unwrap();
        }

        // Read back in reverse order to foArce cache churn across pages
        for i in (0..elem_count).rev() {
            let v = array.at(i).deref().unwrap();
            let expected = Elem(((i as u64) << 32) ^ 0xDEAD_BEEF_u64 ^ (i as u64));
            assert_eq!(v, expected);
        }

        // unique temp files; no shared cleanup needed
    }

    #[test]
    fn test_persist_twice_with_interleaved_swaps() {
        // Use the same path for reopen to validate persistence properly
        let path = unique_test_path("persist_twice");
        cleanup_test_files(&path);
        let memory = PersistentRandomAccessMemory::new(TEST_SIZE, &path, PAGE_SIZE);
        let page_size =  PAGE_SIZE as u64;

        // Place four u64s on distinct pages
        let mut p0 = memory.smalloc::<u64>(0, 8).unwrap();
        let mut p1 = memory.smalloc::<u64>(page_size-1, 8).unwrap();
        let mut p2 = memory.smalloc::<u64>(page_size * 2 -2, 8).unwrap();
        let mut p3 = memory.smalloc::<u64>(page_size * 3 -3, 8).unwrap();

        let vals_a = [
            0xAAAA_AAAA_AAAA_AAAAu64,
            0xBBBB_BBBB_BBBB_BBBBu64,
            0xCCCC_CCCC_CCCC_CCCCu64,
            0xDDDD_DDDD_DDDD_DDDDu64,
        ];
        p0.set(&vals_a[0]).unwrap();
        p1.set(&vals_a[1]).unwrap();
        p2.set(&vals_a[2]).unwrap();
        p3.set(&vals_a[3]).unwrap();

        memory.persist().unwrap();

        // Thrash pages by repeatedly accessing them in a non-trivial pattern
        for i in 0..600usize {
            match (i * 5) % 4 {
                0 => assert_eq!(p0.deref().unwrap(), vals_a[0]),
                1 => assert_eq!(p1.deref().unwrap(), vals_a[1]),
                2 => assert_eq!(p2.deref().unwrap(), vals_a[2]),
                _ => assert_eq!(p3.deref().unwrap(), vals_a[3]),
            }
        }

        // Overwrite with new values and persist again
        let vals_b = [
            0x1111_2222_3333_4444u64,
            0x5555_6666_7777_8888u64,
            0x9999_AAAA_BBBB_CCCCu64,
            0xDDDD_EEEE_FFFF_0000u64,
        ];
        p0.set(&vals_b[0]).unwrap();
        p1.set(&vals_b[1]).unwrap();
        p2.set(&vals_b[2]).unwrap();
        p3.set(&vals_b[3]).unwrap();

        memory.persist().unwrap();
        drop(memory);

        // Reopen the SAME path and verify second set of values survived
        let memory2 = PersistentRandomAccessMemory::new(TEST_SIZE, &path, PAGE_SIZE);
        let p0r = Pointer::<u64>::from_address(0, memory2.clone());
        let p1r = Pointer::<u64>::from_address(page_size -1, memory2.clone());
        let p2r = Pointer::<u64>::from_address(page_size * 2 -2, memory2.clone());
        let p3r = Pointer::<u64>::from_address(page_size * 3 -3, memory2.clone());

        assert_eq!(p0r.deref().unwrap(), vals_b[0]);
        assert_eq!(p1r.deref().unwrap(), vals_b[1]);
        assert_eq!(p2r.deref().unwrap(), vals_b[2]);
        assert_eq!(p3r.deref().unwrap(), vals_b[3]);

        // unique temp files; no shared cleanup needed
    }

    // Removed randomized buffer access integrity test due to missing read(bytes) API.
}