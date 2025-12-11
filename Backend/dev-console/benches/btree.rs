use criterion::{criterion_group, criterion_main, BenchmarkId, BatchSize, Criterion, Throughput, black_box};
use general::persistent_random_access_memory::PersistentRandomAccessMemory;
use general::pram_btree_index::{ConcurrentBTreeIndexPRAM, BTreeIndex};
// no additional imports needed
use std::time::Duration;

const PAGE_SIZE: usize = 4*1024; // to fit the set b tree node size
// LRU-related constants removed for new PRAM API

fn make_index(path: &str) -> ConcurrentBTreeIndexPRAM {
	// Allocate a modest PRAM backing file (16 MiB) matching typical page size
	let pram = PersistentRandomAccessMemory::new(
		16 * 1024 * 1024,
		path,
	);
	ConcurrentBTreeIndexPRAM::new(pram)
}

fn bench_btree_insert(c: &mut Criterion) {
	let mut g = c.benchmark_group("btree_insert");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));

	for &cap in &[256usize, 1024usize, 4096usize] {
		g.throughput(Throughput::Elements(cap as u64));
		g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("btree_insert.pram").to_string_lossy().to_string();
					let idx = make_index(&path);
					(dir, idx)
				},
				|(dir, mut idx)| {
					let _keep = dir; // ensure tempdir lives for duration
					for i in 0..cap as u64 {
						let _ = idx.set(i, i.wrapping_mul(3));
					}
					black_box(&mut idx);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g.finish();
}

fn bench_btree_get(c: &mut Criterion) {
	let mut g = c.benchmark_group("btree_get");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(15));

	for &cap in &[256usize, 1024usize, 4096usize] {
		g.throughput(Throughput::Elements(cap as u64));
		g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join("btree_get.pram").to_string_lossy().to_string();
					let mut idx = make_index(&path);
					for i in 0..cap as u64 { let _ = idx.set(i, i ^ 0xDEADBEEF); }
					(dir, idx)
				},
				|(dir, idx)| {
					let _keep = dir;
					let mut sum = 0u64;
					for i in 0..cap as u64 {
						if let Ok(v) = idx.get(i) { sum = sum.wrapping_add(v); }
					}
					black_box(sum);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g.finish();
}

fn bench_btree_update_commit(c: &mut Criterion) {
	let mut g = c.benchmark_group("btree_update_commit");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));
	const CAP: usize = 1024;
	g.throughput(Throughput::Elements(CAP as u64));
	g.bench_function("update_commit_every_64", |b| {
		b.iter_batched(
			|| {
				let dir = tempfile::tempdir().unwrap();
				let path = dir.path().join("btree_update_commit.pram").to_string_lossy().to_string();
				let mut idx = make_index(&path);
				for i in 0..CAP as u64 { let _ = idx.set(i, i); }
				(dir, idx)
			},
			|(dir, mut idx)| {
				let _keep = dir;
				for i in 0..CAP as u64 {
					let _ = idx.set(i, i.wrapping_mul(7));
					if (i & 63) == 63 { let _ = idx.persist(); }
				}
				black_box(&mut idx);
			},
			BatchSize::SmallInput,
		);
	});
	g.finish();
}

fn bench_btree_remove(c: &mut Criterion) {
	let mut g = c.benchmark_group("btree_remove");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(20));
	for &cap in &[256usize, 1024usize, 4096usize] {
		g.throughput(Throughput::Elements(cap as u64));
		g.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
			b.iter_batched(
				|| {
					let dir = tempfile::tempdir().unwrap();
					let path = dir.path().join(format!("btree_remove{}.pram", cap)).to_string_lossy().to_string();
					let mut idx = make_index(&path);
					for i in 0..cap as u64 { let _ = idx.set(i, i); }
					(dir, idx)
				},
				|(dir, mut idx)| {
					let _keep = dir;
					let mut removed = 0usize;
					for i in 0..cap as u64 { if idx.remove(i).is_ok() { removed += 1; } }
					black_box(removed);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g.finish();
}

fn bench_btree_iter(c: &mut Criterion) {
	let mut g = c.benchmark_group("btree_iter");
	g.sample_size(16);
	g.measurement_time(Duration::from_secs(15));
	const CAP: usize = 8192;
	g.throughput(Throughput::Elements(CAP as u64));
	g.bench_function("iterate_sum_keys", |b| {
		b.iter_batched(
			|| {
				let dir = tempfile::tempdir().unwrap();
				let path = dir.path().join("btree_iter.pram").to_string_lossy().to_string();
				let mut idx = make_index(&path);
				for i in 0..CAP as u64 { let _ = idx.set(i, i ^ 0xA5A5_A5A5_A5A5_A5A5); }
				(dir, idx)
			},
			|(dir, idx)| {
				let _keep = dir;
				let mut acc = 0u64;
				if let Ok(iter) = idx.iter() {
					for (k, v) in iter { acc = acc.wrapping_add(k ^ v); }
				}
				black_box(acc);
			},
			BatchSize::SmallInput,
		);
	});
	g.finish();
}

criterion_group!(btree_benches, 
	bench_btree_insert, 
	bench_btree_get, 
	bench_btree_update_commit, 
	bench_btree_remove, 
	bench_btree_iter
);
criterion_main!(btree_benches);

