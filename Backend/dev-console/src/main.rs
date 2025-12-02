use general::persistent_random_access_memory::{FilePersistentRandomAccessMemory, PersistentRandomAccessMemory};
use std::mem::size_of;

fn main() {
    // Create a temporary directory to hold backing page files
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("demo.ignore._fpram");
    let path_str = path.to_string_lossy().to_string();

    // Create a 4-page (16KiB) persistent RAM instance
    let fpram = FilePersistentRandomAccessMemory::new(4096 * 4, &path_str, 4096, 16, 2, 1);
    println!("Created fpram at {}", path_str);

    // --- Static allocation (salloc) example ---
    // Reserve an absolute offset (100) and write a value there
    let mut sp = fpram.smalloc(100, size_of::<u64>()).expect("salloc u64");
    sp.write(&0x1234u64).expect("write salloc");
    let sread: Box<u64> = sp.deref().expect("deref salloc");
    println!("salloc@100 -> read {}", *sread);

    // --- Dynamic allocation (malloc) example ---
    let mut p = fpram.malloc(size_of::<u64>()).expect("malloc u64");
    p.write(&0xDEADBEEFu64).expect("write u64");
    let read_val: Box<u64> = p.deref().expect("deref u64");
    println!("malloc -> read 0x{:X}", *read_val);

    let text: String = "Hello, Persistent RAM!".to_string();
    let text_bytes = text.as_bytes();
    println!("malloc string -> write '{:X?}'", text_bytes);
    let mut text_ptr = fpram.malloc(text_bytes.len()).expect("malloc string");
    text_ptr.write_exact(&text_bytes).expect("write string");
    
    let mut read_buf = vec![0u8; text_bytes.len()];
    text_ptr.read_exact(&mut read_buf).expect("deref string");
    println!("malloc string -> read '{:X?}'", read_buf);
    println!("malloc string -> read '{}'", String::from_utf8_lossy(&read_buf));

    // --- Array access with .at<T>(index) ---
    let array = fpram.malloc(10 * size_of::<u64>()).expect("malloc array");
    for i in 0..10 {
        array.at::<u64>(i).write(&(i as u64 * 10)).expect("write array element");
    }
    for i in 0..10 {
        let v: Box<u64> = array.at::<u64>(i).deref().expect("deref array element");
        println!("array[{}] = {}", i, *v);
    }

    // Persist changes to disk
    fpram.persist().expect("persist");
    println!("Persisted pages to disk. Page files are stored in the temp dir.");

    // Free an allocation
    fpram.free(array, 10 * size_of::<u64>()).expect("free array");
    println!("Freed the array allocation.");

    // Keep the tempdir alive until program exit so the backing files can be inspected
    println!("Demo complete. Tempdir path: {}", dir.path().display());

    // --- Cache performance test ---
    const PAGE_SIZE: usize = 1024; // 1KiB
    const LRU_CAPACITY: usize = 16; // number of pages in LRU cache
    const LRU_HISTORY_LENGTH: usize = 2; // K value for LRU-K
    const LRU_PARDON: usize = 4; // pardon value for LRU-K

    const MEMORY_SIZE: usize = PAGE_SIZE * 500; // 2MB
    const ELEMENT_SIZE: usize = std::mem::size_of::<u64>();
    const HOT_LEN: usize = 256;
    const READ_HEAVY_LEN: usize = 2048;
    const WRITE_HEAVY_LEN: usize = 2048;
    const COLD_LEN: usize = 256;
    const ITERS: usize = 1_000; // per-sample work

    // Test cache stats
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("f.ignore.pram");
    let path_str = path.to_string_lossy().to_string();
    let fpram = FilePersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str, PAGE_SIZE, LRU_CAPACITY, LRU_HISTORY_LENGTH, LRU_PARDON);
    
    let hot = fpram.malloc(HOT_LEN * ELEMENT_SIZE).expect("hot alloc");
    let rh: general::persistent_random_access_memory::Pointer = fpram.malloc(READ_HEAVY_LEN * ELEMENT_SIZE).expect("rh alloc");
    let wh = fpram.malloc(WRITE_HEAVY_LEN * ELEMENT_SIZE).expect("wh alloc");
    let cold = fpram.malloc(COLD_LEN * ELEMENT_SIZE).expect("cold alloc");

    // init small portions to touch pages
    for i in 0..HOT_LEN { hot.at::<u64>(i).write(&0).expect("init hot"); }
    for i in 0..READ_HEAVY_LEN { rh.at::<u64>(i).write(&0).expect("init rh"); }
    for i in 0..WRITE_HEAVY_LEN { wh.at::<u64>(i).write(&0).expect("init wh"); }
    for i in 0..COLD_LEN { cold.at::<u64>(i).write(&0).expect("init cold"); }
    
    let _keep_dir = dir;
    for i in 0..ITERS {
        // HOT: frequent read-write (+1)
        let idx = i % HOT_LEN;
        let mut p = hot.at::<u64>(idx);
        let v = *p.deref::<u64>().expect("hot rd");
        p.write(&(v + 1)).expect("hot wr");

        // READ-HEAVY: 3 reads, occasional write every 16
        let r1 = i % READ_HEAVY_LEN;
        let r2 = (i.wrapping_mul(7)) % READ_HEAVY_LEN;
        let r3 = (i.wrapping_mul(13)) % READ_HEAVY_LEN;
        let _ = rh.at::<u64>(r1).deref::<u64>().expect("rh rd1");
        let _ = rh.at::<u64>(r2).deref::<u64>().expect("rh rd2");
        let _ = rh.at::<u64>(r3).deref::<u64>().expect("rh rd3");
        if (i & 0xF) == 0 {
            let mut p = rh.at::<u64>(r1);
            let v = *p.deref::<u64>().expect("rh wr rd");
            p.write(&(v + 1)).expect("rh wr");
        }

        // WRITE-HEAVY: two writes per iter, rare reads
        let w1 = i % WRITE_HEAVY_LEN;
        let w2 = (i.wrapping_mul(3)) % WRITE_HEAVY_LEN;
        let mut p1 = wh.at::<u64>(w1);
        let v1 = *p1.deref::<u64>().expect("wh rd1");
        p1.write(&(v1 + 1)).expect("wh wr1");
        let mut p2 = wh.at::<u64>(w2);
        let v2 = *p2.deref::<u64>().expect("wh rd2");
        p2.write(&(v2 + 1)).expect("wh wr2");
        if (i & 0x1F) == 0 {
            let _ = p1.deref::<u64>().expect("wh occasional rd");
        }

        // COLD: rare write (every 128), else read
        let c = i % COLD_LEN;
        if (i & 0x7F) == 0 {
            let mut p = cold.at::<u64>(c);
            let v = *p.deref::<u64>().expect("cold rd");
            p.write(&(v + 1)).expect("cold wr");
        } else {
            let _ = cold.at::<u64>(c).deref::<u64>().expect("cold read");
        }
    }

    println!("Cache Misses: {:.2}%", fpram.get_cache_miss_rate() * 100.0);
    
}