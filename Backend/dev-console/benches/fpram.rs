use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use general::persistent_random_access_memory::PersistentRandomAccessMemory;

const PAGE_SIZE: usize = 4096; // For benchmarks: 4KiB normally 64KiB
// LRU-related constants removed for new API

// Simple pseudo-random number generator for reproducible tests
struct SimpleRandom {
    state: u64,
}
impl SimpleRandom {
    fn new(seed: u64) -> Self { Self { state: seed } }
    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}

fn bench_malloc_free(c: &mut Criterion) {
    let mut group = c.benchmark_group("malloc_free");
    // Reduce sample count and give more measurement time to avoid long warnings
    group.sample_size(16);
    group.measurement_time(Duration::from_secs(32));
    const MEMORY_SIZE: usize = PAGE_SIZE * 200; // 200 pages
    const ALLOCS_PER_SAMPLE: usize = 1_000; // time is per 1000 allocs/frees
    for &size in &[64, 2048] {
        group.throughput(Throughput::Elements(ALLOCS_PER_SAMPLE as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &alloc_size| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = dir.path().join("f.ignore.pram");
                    let path_str = path.to_string_lossy().to_string();
                    let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                    (dir, fpram, alloc_size)
                },
                |(dir, fpram, alloc_size)| {
                    let _keep_dir = dir; // ensure directory outlives fpram during this run
                    let mut ptrs = Vec::with_capacity(ALLOCS_PER_SAMPLE);
                    for _ in 0..ALLOCS_PER_SAMPLE {
                        if let Ok(p) = fpram.malloc::<u8>(alloc_size) {
                            ptrs.push(p);
                        } else {
                            break;
                        }
                    }
                    for p in ptrs.into_iter().rev() {
                        let _ = fpram.free(p.address, alloc_size);
                    }
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_salloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("salloc");
    group.sample_size(16);
    group.measurement_time(Duration::from_secs(32));
    const MEMORY_SIZE: usize = PAGE_SIZE * 200; // 200 pages
    const SALLOC_SIZE: usize = 64;
    const OPS_PER_SAMPLE: usize = 1_000;

    group.throughput(Throughput::Elements(OPS_PER_SAMPLE as u64));
    group.bench_function("salloc_64B", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                (dir, fpram)
            },
            |(dir, fpram)| {
                let _keep_dir = dir;
                for i in 0..OPS_PER_SAMPLE {
                    let pointer = (i * SALLOC_SIZE) as u64;
                    let _ = fpram.smalloc::<u8>(pointer, SALLOC_SIZE).expect("salloc");
                    // p.write(&0u64).expect("write"); // optional write to touch page
                }
                black_box(());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_access");
    group.sample_size(16);
    group.measurement_time(Duration::from_secs(32));
    const MEMORY_SIZE: usize = PAGE_SIZE * 500; // 2MB
    const TOTAL_PTRS: usize = 20_000; // pre-allocated pointers
    const OPS_PER_SAMPLE: usize = 1_000; // random ops per timed sample

    // Random writes
    group.throughput(Throughput::Elements(OPS_PER_SAMPLE as u64));
    group.bench_function("random_write_u64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                let mut ptrs = Vec::with_capacity(TOTAL_PTRS);
                for _ in 0..TOTAL_PTRS {
                    let p = fpram.malloc::<u64>(std::mem::size_of::<u64>()).expect("malloc");
                    ptrs.push(p);
                }
                (dir, fpram, ptrs)
            },
            |(dir, mut fpram, mut ptrs)| {
                let _keep_dir = dir;
                let mut rng = SimpleRandom::new(12345);
                for _ in 0..OPS_PER_SAMPLE {
                    let idx = (rng.next() as usize) % ptrs.len();
                    let val: u64 = rng.next();
                    ptrs[idx].set(&val).expect("write");
                }
                // keep fpram alive
                black_box((&mut fpram, &mut ptrs));
            },
            BatchSize::SmallInput,
        );
    });

    // Random reads
    group.bench_function("random_read_u64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                let mut ptrs = Vec::with_capacity(TOTAL_PTRS);
                for _ in 0..TOTAL_PTRS {
                    let mut p = fpram.malloc::<u64>(std::mem::size_of::<u64>()).expect("malloc");
                    // init to make sure pages exist
                    p.set(&0u64).expect("init");
                    ptrs.push(p);
                }
                (dir, fpram, ptrs)
            },
            |(dir, mut fpram,  ptrs)| {
                let _keep_dir = dir;
                let mut rng = SimpleRandom::new(54321);
                let mut sum = 0u64;
                for _ in 0..OPS_PER_SAMPLE {
                    let idx = (rng.next() as usize) % ptrs.len();
                    let v: u64 = ptrs[idx].deref().expect("read");
                    sum = sum.wrapping_add(v);
                }
                black_box(sum);
                black_box(&mut fpram);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}
 
fn bench_sequential_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_access");
    group.sample_size(16);
    group.measurement_time(Duration::from_secs(32));
    const MEMORY_SIZE: usize = PAGE_SIZE * 500; // 2MB
    const ELEMENTS: usize = 200_000; // 1.6MB for u64
    const OPS_PER_SAMPLE: usize = 1_000;

    // Sequential writes
    group.throughput(Throughput::Elements(OPS_PER_SAMPLE as u64));
    group.bench_function("sequential_write_u64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                let array = fpram
                    .malloc::<u64>(ELEMENTS * std::mem::size_of::<u64>())
                    .expect("malloc array");
                (dir, fpram, array)
            },
            |(dir, mut fpram, array)| {
                let _keep_dir = dir;
                for i in 0..OPS_PER_SAMPLE {
                    let idx = i % ELEMENTS;
                    let mut elem = array.at(idx);
                    let v = i as u64;
                    elem.set(&v).expect("seq write");
                }
                black_box(&mut fpram);
            },
            BatchSize::SmallInput,
        );
    });

    // Sequential reads (init small prefix to avoid lazy faults)
    group.bench_function("sequential_read_u64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                let array = fpram
                    .malloc::<u64>(ELEMENTS * std::mem::size_of::<u64>())
                    .expect("malloc array");
                for i in 0..OPS_PER_SAMPLE {
                    array.at(i % ELEMENTS).set(&0u64).expect("init");
                }
                (dir, fpram, array)
            },
            |(dir, mut fpram, array)| {
                let _keep_dir = dir;
                let mut sum = 0u64;
                for i in 0..OPS_PER_SAMPLE {
                    let idx = i % ELEMENTS;
                    let v: u64 = array.at(idx).deref().expect("seq read");
                    sum = sum.wrapping_add(v);
                }
                black_box(sum);
                black_box(&mut fpram);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_mixed_hot_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_hot_cold");
    group.sample_size(16);
    group.measurement_time(Duration::from_secs(32));
    const MEMORY_SIZE: usize = PAGE_SIZE * 500; // 2MB
    const ELEMENT_SIZE: usize = std::mem::size_of::<u64>();
    const HOT_LEN: usize = 256;
    const READ_HEAVY_LEN: usize = 2048;
    const WRITE_HEAVY_LEN: usize = 2048;
    const COLD_LEN: usize = 256;
    const ITERS: usize = 1_000; // per-sample work

    group.throughput(Throughput::Elements(ITERS as u64));
    group.bench_function("mixed_pattern_u64", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("f.ignore.pram");
                let path_str = path.to_string_lossy().to_string();
                let fpram = PersistentRandomAccessMemory::new(MEMORY_SIZE, &path_str);
                let hot = fpram.malloc(HOT_LEN * ELEMENT_SIZE).expect("hot alloc");
                let rh = fpram.malloc(READ_HEAVY_LEN * ELEMENT_SIZE).expect("rh alloc");
                let wh = fpram.malloc(WRITE_HEAVY_LEN * ELEMENT_SIZE).expect("wh alloc");
                let cold = fpram.malloc(COLD_LEN * ELEMENT_SIZE).expect("cold alloc");
                // init small portions to touch pages
                for i in 0..HOT_LEN { hot.at(i).set(&0u64).expect("init hot"); }
                for i in 0..READ_HEAVY_LEN { rh.at(i).set(&0u64).expect("init rh"); }
                for i in 0..WRITE_HEAVY_LEN { wh.at(i).set(&0u64).expect("init wh"); }
                for i in 0..COLD_LEN { cold.at(i).set(&0u64).expect("init cold"); }
                (dir, fpram, hot, rh, wh, cold)
            },
            |(dir, mut fpram, hot, rh, wh, cold)| {
                let _keep_dir = dir;
                for i in 0..ITERS {
                    // HOT: frequent read-write (+1)
                    let idx = i % HOT_LEN;
                    let mut p = hot.at(idx);
                    let v: u64 = p.deref().expect("hot rd");
                    p.set(&(v + 1)).expect("hot wr");

                    // READ-HEAVY: 3 reads, occasional write every 16
                    let r1 = i % READ_HEAVY_LEN;
                    let r2 = (i.wrapping_mul(7)) % READ_HEAVY_LEN;
                    let r3 = (i.wrapping_mul(13)) % READ_HEAVY_LEN;
                    let _ = rh.at(r1).deref().expect("rh rd1");
                    let _ = rh.at(r2).deref().expect("rh rd2");
                    let _ = rh.at(r3).deref().expect("rh rd3");
                    if (i & 0xF) == 0 {
                        let mut p = rh.at(r1);
                        let v: u64 = p.deref().expect("rh wr rd");
                        p.set(&(v + 1)).expect("rh wr");
                    }

                    // WRITE-HEAVY: two writes per iter, rare reads
                    let w1 = i % WRITE_HEAVY_LEN;
                    let w2 = (i.wrapping_mul(3)) % WRITE_HEAVY_LEN;
                    let mut p1 = wh.at(w1);
                    let v1: u64 = p1.deref().expect("wh rd1");
                    p1.set(&(v1 + 1)).expect("wh wr1");
                    let mut p2 = wh.at(w2);
                    let v2: u64 = p2.deref().expect("wh rd2");
                    p2.set(&(v2 + 1)).expect("wh wr2");
                    if (i & 0x1F) == 0 {
                        let _ = p1.deref().expect("wh occasional rd");
                    }

                    // COLD: rare write (every 128), else read
                    let c = i % COLD_LEN;
                    if (i & 0x7F) == 0 {
                        let mut p = cold.at(c);
                        let v: u64 = p.deref().expect("cold rd");
                        p.set(&(v + 1)).expect("cold wr");
                    } else {
                        let _ = cold.at(c).deref().expect("cold read");
                    }
                }
                black_box(&mut fpram);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_random_access, bench_sequential_access, bench_mixed_hot_cold, bench_malloc_free, bench_salloc,);
criterion_main!(benches);
