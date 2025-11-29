use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, BatchSize, black_box};
use general::persistent_random_access_memory::FilePersistentRandomAccessMemory;
use general::write_ahead_log::{PRAMWriteAheadLog, WriteAheadLog};
use std::time::Duration;

const PAGE_SIZE: usize = 4096; // match other benches
const LRU_CAPACITY: usize = 64;
const LRU_HISTORY_LENGTH: usize = 3;
const LRU_PARDON: usize = 16;

#[derive(Clone, Copy)]
struct Small{ _a: u64 }
#[derive(Clone, Copy)]
struct Medium { a: u64, _b: u64, _c: u64, _d: u64 }
#[derive(Clone, Copy)]
struct Large { _buf: [u8; 128] }

fn align_to_page(len: usize) -> usize { ((len + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE }

fn make_wal<T: Sized>(entries: usize, path: &str) -> PRAMWriteAheadLog<T> {
	// memory required for static region + data
	let data_bytes = entries * std::mem::size_of::<T>();
	let raw = 16 + data_bytes; // head+tail+data start
	let mem_size = align_to_page(raw.max(PAGE_SIZE));
	let fpram = FilePersistentRandomAccessMemory::new(mem_size, path, PAGE_SIZE, LRU_CAPACITY, LRU_HISTORY_LENGTH, LRU_PARDON);
	PRAMWriteAheadLog::new(fpram, entries)
}

fn bench_wal_append(c: &mut Criterion) {
	let mut g = c.benchmark_group("wal_append");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));
	for &cap in &[256usize, 1024usize] {
		g.throughput(Throughput::Elements(cap as u64));
		g.bench_with_input(BenchmarkId::new("small", cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("wal_small.pram").to_string_lossy().to_string();
					let wal = make_wal::<Small>(cap, &path);
					(dir, wal)
				},
				|(dir, mut wal)| {
					let _keep = dir;
					for _ in 0..cap { let _ = wal.append(&Small { _a: 0 }); }
					black_box(&mut wal);
				},
				BatchSize::SmallInput,
			);
		});

		g.bench_with_input(BenchmarkId::new("medium", cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("wal_medium.pram").to_string_lossy().to_string();
					let wal = make_wal::<Medium>(cap, &path);
					(dir, wal)
				},
				|(dir, mut wal)| {
					let _keep = dir;
					for i in 0..cap { let entry = Medium { a: i as u64, _b: 2, _c: 3, _d: 4 }; let _ = wal.append(&entry); }
					black_box(&mut wal);
				},
				BatchSize::SmallInput,
			);
		});

		g.bench_with_input(BenchmarkId::new("large", cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("wal_large.pram").to_string_lossy().to_string();
					let wal = make_wal::<Large>(cap, &path);
					(dir, wal)
				},
				|(dir, mut wal)| {
					let _keep = dir;
					let entry = Large { _buf: [7u8; 128] }; // reuse same entry
					for _ in 0..cap { let _ = wal.append(&entry); }
					black_box(&mut wal);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g.finish();
}

fn bench_wal_append_commit(c: &mut Criterion) {
	let mut g = c.benchmark_group("wal_append_commit");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));
	const CAP: usize = 2048;
	g.throughput(Throughput::Elements(CAP as u64));
	g.bench_function("append_commit_every_64", |b| {
		b.iter_batched(
			|| {
				let dir = tempfile::tempdir().unwrap();
				let path = dir.path().join("wal_commit.pram").to_string_lossy().to_string();
				let wal = make_wal::<Small>(CAP, &path);
				(dir, wal)
			},
			|(dir, mut wal)| {
				let _keep = dir;
				for i in 0..CAP { let _ = wal.append(&Small { _a: 0 }); if (i & 63) == 63 { let _ = wal.commit(); } }
				black_box(&mut wal);
			},
			BatchSize::SmallInput,
		);
	});
	g.finish();
}

fn bench_wal_pop(c: &mut Criterion) {
	let mut g = c.benchmark_group("wal_pop");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));
	for &cap in &[256usize, 1024usize] {
		g.throughput(Throughput::Elements(cap as u64));
		g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("wal_pop.pram").to_string_lossy().to_string();
					let mut wal = make_wal::<Small>(cap, &path);
					for _ in 0..cap { let _ = wal.append(&Small { _a: 0 }); }
					(dir, wal)
				},
				|(dir, mut wal)| {
					let _keep = dir;
					let mut popped = 0usize;
					while let Ok(_) = wal.pop() { popped += 1; }
					black_box(popped);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g.finish();
}

fn bench_wal_peak(c: &mut Criterion) {
	let mut g = c.benchmark_group("wal_peak");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(15));
	const CAP: usize = 512;
	g.throughput(Throughput::Elements(CAP as u64));
	g.bench_function("peak_loop", |b| {
		b.iter_batched(
			|| {
				let dir = tempfile::tempdir().unwrap();
				let path = dir.path().join("wal_peak.pram").to_string_lossy().to_string();
				let mut wal = make_wal::<Medium>(CAP, &path);
				for i in 0..CAP { let entry = Medium { a: i as u64, _b: 1, _c: 2, _d: 3 }; let _ = wal.append(&entry); }
				(dir, wal)
			},
			|(dir, mut wal)| {
				let _keep = dir;
				let mut sum = 0u64;
				for _ in 0..CAP { if let Ok(v) = wal.peak() { sum = sum.wrapping_add(v.a); } }
				black_box(sum);
			},
			BatchSize::SmallInput,
		);
	});
	g.finish();
}

criterion_group!(wal_benches, bench_wal_append, bench_wal_append_commit, bench_wal_pop, bench_wal_peak);
criterion_main!(wal_benches);
