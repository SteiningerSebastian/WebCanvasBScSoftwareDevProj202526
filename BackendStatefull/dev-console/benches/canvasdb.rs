use criterion::{criterion_group, criterion_main, BenchmarkId, BatchSize, Criterion, Throughput, black_box};
use std::time::Duration;
use std::sync::Arc;
use std::thread;
use noredb::canvasdb::{CanvasDB, Pixel, PixelEntry, TimeStamp, CanvasDBTrait};

static mut TEST: u16 = 0;
const PATH: &str = "D:/tmp/canvasdb/bench/";

fn make_canvasdb(width: usize, height: usize, wal_size: usize) -> CanvasDB {
    // wait a moment to avoid collisions in quick successive calls
    // Make sure previous threads have fully exited - the canvasdb has shutdown successfully
    std::thread::sleep(std::time::Duration::from_millis(500));

    let path = PATH;
    // Make sure the directory exists
    std::fs::create_dir_all(&path).unwrap();
    // remove all existing files in the directory
    std::fs::remove_dir_all(&path).unwrap();
    std::fs::create_dir_all(&path).unwrap();

    let value = unsafe { TEST };

    let path = format!("{}canvasdb_{}", path, value);
    unsafe {
        TEST += 1;
    }

    let db  = CanvasDB::new(width, height, &path, wal_size);
    db.start_worker_threads(16);
    db
}

fn bench_canvasdb_set(c: &mut Criterion) {
    let mut g = c.benchmark_group("canvasdb_set_concurrent");
    g.sample_size(16);
    g.measurement_time(Duration::from_secs(20));

    const NUM_THREADS: usize = 8;

    for &cap in &[1<<15usize,1<<16usize,1<<17usize] {
        g.throughput(Throughput::Elements(cap as u64));
        g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_batched(
                || {
                    let db = Arc::new(make_canvasdb(cap, 1usize, cap * 2));
                    db
                },
                |db| {
                    let ops_per_thread = cap / NUM_THREADS;
                    
                    let handles: Vec<_> = (0..NUM_THREADS)
                        .map(|thread_id| {
                            let db_clone = Arc::clone(&db);
                            thread::spawn(move || {
                                let start = thread_id * ops_per_thread;
                                let end = start + ops_per_thread;
                                for i in start..end {
                                    let ts = TimeStamp { bytes: (i as u128).to_le_bytes() };
                                    let entry = PixelEntry {
                                        pixel: Pixel { 
                                            key: i as u32, 
                                            color: [((i & 0xFF) as u8), 0xAA, 0x55] 
                                        },
                                        timestamp: ts,
                                    };
                                    db_clone.set_pixel(entry, None);
                                }
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    black_box(&db);
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

fn bench_canvasdb_get(c: &mut Criterion) {
    let mut g = c.benchmark_group("canvasdb_get_concurrent");
    g.sample_size(16);
    g.measurement_time(Duration::from_secs(15));

    const NUM_THREADS: usize = 8;

    for &cap in &[1<<15usize, 1<<16usize,1<<17usize] {
        g.throughput(Throughput::Elements(cap as u64));
        g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_batched(
                || {
                    let db = Arc::new(make_canvasdb(cap, 1usize, cap * 2));
                    // Pre-populate with data
                    for i in 0..cap as u32 {
                        let ts = TimeStamp { bytes: (i as u128).to_le_bytes() };
                        let entry = PixelEntry {
                            pixel: Pixel { key: i, color: [((i & 0xFF) as u8), 0xAA, 0x55] },
                            timestamp: ts,
                        };
                        db.set_pixel(entry, None);
                    }

                    // Wait for data to be moved from WAL to main store
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    
                    db
                },
                |db| {
                    let ops_per_thread = cap / NUM_THREADS;
                    
                    let handles: Vec<_> = (0..NUM_THREADS)
                        .map(|thread_id| {
                            let db_clone = Arc::clone(&db);
                            thread::spawn(move || {
                                let start = thread_id * ops_per_thread;
                                let end = start + ops_per_thread;
                                let mut acc = 0u64;
                                for i in start..end {
                                    if let Some((pixel, _ts)) = db_clone.get_pixel(i as u32) {
                                        acc = acc.wrapping_add(pixel.key as u64);
                                    }
                                }
                                acc
                            })
                        })
                        .collect();
                    
                    let mut total_acc = 0u64;
                    for handle in handles {
                        total_acc = total_acc.wrapping_add(handle.join().unwrap());
                    }
                    black_box(total_acc);
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

criterion_group!(canvasdb_benches, bench_canvasdb_get, bench_canvasdb_set);
criterion_main!(canvasdb_benches);