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
    let mut sp = fpram.salloc(100, size_of::<u64>()).expect("salloc u64");
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
}