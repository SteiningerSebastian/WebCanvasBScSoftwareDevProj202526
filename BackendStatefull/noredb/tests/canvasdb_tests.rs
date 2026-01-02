use noredb::canvasdb::{CanvasDB, CanvasDBTrait, Pixel, PixelEntry, TimeStamp};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;

static TEST: AtomicU64 = AtomicU64::new(0);

/// Helper function to create a test CanvasDB instance
fn create_test_canvas_db(name: &str) -> CanvasDB {
    // Use a unique path for each test to avoid conflicts
    let test_id = TEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = format!("D:/tmp/canvasdb/{}_test_canvas_db_{}", test_id, name);

    // If any file with a matching name name* exists, delete it
    // list all files matching the pattern
    for enty in std::fs::read_dir("D:/tmp/canvasdb/").unwrap() {
        let entry = enty.unwrap();
        let file_name = entry.file_name().into_string().unwrap();
        if file_name.starts_with(&path) {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    // Let filesystem settle
    thread::sleep(Duration::from_millis(100));

    CanvasDB::new(
        1920, // width
        1080, // height
        &path,
        10000, // write_ahead_log_size
    )
}

/// Helper function to create a TimeStamp from a u128 value
fn create_timestamp(value: u128) -> TimeStamp {
    TimeStamp {
        bytes: value.to_be_bytes(),
    }
}

/// Helper function to create a Pixel
fn create_pixel(key: u32, r: u8, g: u8, b: u8) -> Pixel {
    Pixel {
        key,
        color: [r, g, b],
    }
}

/// Helper function to create a PixelEntry
fn create_pixel_entry(key: u32, r: u8, g: u8, b: u8, timestamp: u128) -> PixelEntry {
    PixelEntry {
        pixel: create_pixel(key, r, g, b),
        timestamp: create_timestamp(timestamp),
    }
}

#[test]
fn test_canvas_db_creation() {
    let _canvas_db = create_test_canvas_db("creation");
    // If we get here, the CanvasDB was created successfully
    assert!(true);
}

#[test]
fn  test_set_and_get_single_pixel() {
    let canvas_db = create_test_canvas_db("set_and_get_single_pixel");
    canvas_db.start_worker_threads(2); 
    
    let pixel_entry = create_pixel_entry(100, 255, 0, 0, 1000);
    
    // Set the pixel
    canvas_db.set_pixel(pixel_entry, None);
    
    // Get the pixel
    let result = canvas_db.get_pixel(100);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.key, 100);
    assert_eq!(pixel.color, [255, 0, 0]);
    assert_eq!(timestamp.bytes, create_timestamp(1000).bytes);

    // Wait for worker threads to process
    thread::sleep(Duration::from_millis(200));

    // Get the pixel
    let result = canvas_db.get_pixel(100);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.key, 100);
    assert_eq!(pixel.color, [255, 0, 0]);
    assert_eq!(timestamp.bytes, create_timestamp(1000).bytes);
}

#[test]
fn test_get_nonexistent_pixel() {
    let canvas_db = create_test_canvas_db("get_nonexistent_pixel");
    canvas_db.start_worker_threads(2);
    
    // Try to get a pixel that doesn't exist
    let result = canvas_db.get_pixel(999);
    assert!(result.is_none());
}

#[test]
fn test_update_pixel_with_newer_timestamp() {
    let canvas_db = create_test_canvas_db("update_pixel_with_newer_timestamp");
    canvas_db.start_worker_threads(2);
    
    // Set initial pixel
    let pixel_entry1 = create_pixel_entry(200, 255, 0, 0, 1000);
    canvas_db.set_pixel(pixel_entry1, None);
    
    thread::sleep(Duration::from_millis(200));
    
    // Update with newer timestamp
    let pixel_entry2 = create_pixel_entry(200, 0, 255, 0, 2000);
    canvas_db.set_pixel(pixel_entry2, None);
    
    thread::sleep(Duration::from_millis(200));
    
    // Get the pixel - should have the newer value
    let result = canvas_db.get_pixel(200);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.color, [0, 255, 0]);
    assert_eq!(timestamp.bytes, create_timestamp(2000).bytes);
}

#[test]
fn test_update_pixel_with_older_timestamp_ignored() {
    let canvas_db = create_test_canvas_db("update_pixel_with_older_timestamp_ignored");
    canvas_db.start_worker_threads(2);
    
    // Set initial pixel with newer timestamp
    let pixel_entry1 = create_pixel_entry(300, 255, 0, 0, 2000);
    canvas_db.set_pixel(pixel_entry1, None);
    
    thread::sleep(Duration::from_millis(200));
    
    // Try to update with older timestamp
    let pixel_entry2 = create_pixel_entry(300, 0, 255, 0, 1000);
    canvas_db.set_pixel(pixel_entry2, None);
    
    thread::sleep(Duration::from_millis(200));
    
    // Get the pixel - should still have the original value
    let result = canvas_db.get_pixel(300);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.color, [255, 0, 0]);
    assert_eq!(timestamp.bytes, create_timestamp(2000).bytes);
}

#[test]
fn test_multiple_pixels() {
    let canvas_db = create_test_canvas_db("multiple_pixels");
    canvas_db.start_worker_threads(4);
    
    // Set multiple pixels
    for i in 0..10 {
        let pixel_entry = create_pixel_entry(i, (i * 25) as u8, (i * 20) as u8, (i * 15) as u8, (i + 1000) as u128);
        canvas_db.set_pixel(pixel_entry, None);
    }
    
    thread::sleep(Duration::from_millis(300));
    
    // Verify all pixels
    for i in 0..10 {
        let result = canvas_db.get_pixel(i);
        assert!(result.is_some());
        
        let (pixel, timestamp) = result.unwrap();
        assert_eq!(pixel.key, i);
        assert_eq!(pixel.color, [(i * 25) as u8, (i * 20) as u8, (i * 15) as u8]);
        assert_eq!(timestamp.bytes, create_timestamp((i + 1000) as u128).bytes);
    }
}

#[test]
fn test_iterate_pixels_returns_persisted_entries() {
    let canvas_db = create_test_canvas_db("iterate_pixels_returns_persisted_entries");
    canvas_db.start_worker_threads(4);

    let entries = vec![
        create_pixel_entry(1000, 10, 20, 30, 12000),
        create_pixel_entry(1001, 40, 50, 60, 12001),
        create_pixel_entry(1002, 70, 80, 90, 12002),
        create_pixel_entry(1003, 100, 110, 120, 12003),
    ];

    for entry in entries.iter() {
        canvas_db.set_pixel(entry.clone(), None);
    }

    // Allow worker threads to move WAL entries into the B-tree before iterating.
    thread::sleep(Duration::from_millis(400));

    let mut iterated: Vec<(u32, [u8; 3])> = canvas_db
        .iterate_pixels()
        .expect("iterate_pixels should succeed")
        .map(|pixel| (pixel.key, pixel.color))
        .collect();

    iterated.sort_by_key(|(key, _)| *key);

    let mut expected: Vec<(u32, [u8; 3])> = entries
        .iter()
        .map(|e| (e.pixel.key, e.pixel.color))
        .collect();

    expected.sort_by_key(|(key, _)| *key);

    assert_eq!(iterated, expected);
}

#[test]
fn test_listener_callback() {
    let canvas_db = create_test_canvas_db("listener_callback");
    canvas_db.start_worker_threads(2);
    
    let callback_called = Arc::new(Mutex::new(false));
    let callback_called_clone = Arc::clone(&callback_called);
    
    let listener = Box::new(move || {
        let mut called = callback_called_clone.lock().unwrap();
        *called = true;
    });
    
    let pixel_entry = create_pixel_entry(400, 128, 128, 128, 5000);
    canvas_db.set_pixel(pixel_entry, Some(listener));
    
    // Wait for listener to be called (listener latency is 100ms)
    thread::sleep(Duration::from_millis(1000));
    
    let called = *callback_called.lock().unwrap();
    assert!(called);
}

#[test]
fn test_multiple_listeners() {
    let canvas_db = create_test_canvas_db("multiple_listeners");
    canvas_db.start_worker_threads(2);
    
    let counter = Arc::new(Mutex::new(0));
    
    // Create multiple pixels with listeners
    for _ in 0..5 {
        let counter_clone = Arc::clone(&counter);
        let listener = Box::new(move || {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        });
        
        let pixel_entry = create_pixel_entry(500, 100, 100, 100, 6000);
        canvas_db.set_pixel(pixel_entry, Some(listener));
    }
    
    // Wait for all listeners to be called
    thread::sleep(Duration::from_millis(1000));
    
    let count = *counter.lock().unwrap();
    assert_eq!(count, 5);
}

#[test]
fn test_concurrent_pixel_updates() {
    let canvas_db = Arc::new(create_test_canvas_db("concurrent_pixel_updates"));
    canvas_db.start_worker_threads(4);
    
    let mut handles = vec![];
    
    // Spawn multiple threads updating different pixels
    for thread_id in 0..10 {
        let canvas_db_clone = Arc::clone(&canvas_db);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = (thread_id * 10000 + i) as u32;
                let pixel_entry = create_pixel_entry(key, thread_id as u8, i as u8, 128, (key + 7000) as u128);
                canvas_db_clone.set_pixel(pixel_entry, None);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Wait for worker threads to process
    thread::sleep(Duration::from_millis(1000));
    
    // Verify all pixels were set correctly
    for thread_id in 0..10 {
        for i in 0..1000 {
            let key = (thread_id * 10000 + i) as u32;
            let result = canvas_db.get_pixel(key);
            assert!(result.is_some());
            
            let (pixel, _) = result.unwrap();
            assert_eq!(pixel.key, key);
            assert_eq!(pixel.color[0], thread_id as u8);
            assert_eq!(pixel.color[1], i as u8);
        }
    }
}

#[test]
fn test_timestamp_ordering() {
    let canvas_db = create_test_canvas_db("timestamp_ordering");
    canvas_db.start_worker_threads(2);
    
    let key = 600;
    
    // Set pixels with different timestamps in random order
    let entries = vec![
        (3000, [255, 0, 0]),
        (1000, [0, 255, 0]),
        (5000, [0, 0, 255]),
        (2000, [255, 255, 0]),
        (4000, [255, 0, 255]),
    ];
    
    for (timestamp, color) in entries {
        let pixel_entry = create_pixel_entry(key, color[0], color[1], color[2], timestamp);
        canvas_db.set_pixel(pixel_entry, None);
    }
    
    thread::sleep(Duration::from_millis(300));
    
    // Should have the pixel with timestamp 5000 (newest)
    let result = canvas_db.get_pixel(key);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.color, [0, 0, 255]);
    assert_eq!(timestamp.bytes, create_timestamp(5000).bytes);
}

#[test]
fn test_get_pixel_from_wal_before_persistence() {
    let canvas_db = create_test_canvas_db("get_pixel_from_wal_before_persistence");
    // Don't start worker threads to keep data in WAL
    
    let pixel_entry = create_pixel_entry(700, 200, 100, 50, 8000);
    canvas_db.set_pixel(pixel_entry, None);
    
    // Immediately get the pixel - should be retrieved from WAL
    let result = canvas_db.get_pixel(700);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.key, 700);
    assert_eq!(pixel.color, [200, 100, 50]);
    assert_eq!(timestamp.bytes, create_timestamp(8000).bytes);
}

#[test]
fn test_high_volume_pixel_updates() {
    let canvas_db = create_test_canvas_db("high_volume_pixel_updates");
    canvas_db.start_worker_threads(8);
    
    // Set a large number of pixels
    let num_pixels = 1000;
    for i in 0..num_pixels {
        let pixel_entry = create_pixel_entry(i, (i % 256) as u8, ((i * 2) % 256) as u8, ((i * 3) % 256) as u8, (i + 9000) as u128);
        canvas_db.set_pixel(pixel_entry, None);
    }
    
    // Wait for processing
    thread::sleep(Duration::from_millis(1000));
    
    // Verify a sample of pixels
    for i in (0..num_pixels).step_by(100) {
        let result = canvas_db.get_pixel(i);
        assert!(result.is_some());
        
        let (pixel, _) = result.unwrap();
        assert_eq!(pixel.key, i);
        assert_eq!(pixel.color, [(i % 256) as u8, ((i * 2) % 256) as u8, ((i * 3) % 256) as u8]);
    }
}

#[test]
fn test_pixel_color_values() {
    let canvas_db = create_test_canvas_db("pixel_color_values");
    canvas_db.start_worker_threads(2);
    
    // Test edge cases for color values
    let test_cases = vec![
        (800, [0, 0, 0]),       // Black
        (801, [255, 255, 255]), // White
        (802, [255, 0, 0]),     // Red
        (803, [0, 255, 0]),     // Green
        (804, [0, 0, 255]),     // Blue
        (805, [128, 128, 128]), // Gray
    ];
    
    for (key, color) in test_cases.iter() {
        let pixel_entry = create_pixel_entry(*key, color[0], color[1], color[2], 10000);
        canvas_db.set_pixel(pixel_entry, None);
    }
    
    thread::sleep(Duration::from_millis(300));
    
    for (key, expected_color) in test_cases.iter() {
        let result = canvas_db.get_pixel(*key);
        assert!(result.is_some());
        
        let (pixel, _) = result.unwrap();
        assert_eq!(pixel.color, *expected_color);
    }
}

#[test]
fn test_same_pixel_rapid_updates() {
    let canvas_db = create_test_canvas_db("same_pixel_rapid_updates");
    canvas_db.start_worker_threads(4);
    
    let key = 900;
    
    // Rapidly update the same pixel with increasing timestamps
    for i in 0..50 {
        let pixel_entry = create_pixel_entry(key, (i * 5) as u8, (i * 3) as u8, (i * 2) as u8, (11000 + i) as u128);
        canvas_db.set_pixel(pixel_entry, None);
    }
    
    thread::sleep(Duration::from_millis(500));
    
    // Should have the last update
    let result = canvas_db.get_pixel(key);
    assert!(result.is_some());
    
    let (pixel, timestamp) = result.unwrap();
    assert_eq!(pixel.color, [(49 * 5) as u8, (49 * 3) as u8, (49 * 2) as u8]);
    assert_eq!(timestamp.bytes, create_timestamp(11000 + 49).bytes);
}

