use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// A simple thread pool for executing jobs concurrently.
/// Each worker thread waits for jobs and executes them as they arrive.
/// The pool can be gracefully shut down by dropping it.
/// # Example
/// ```ignore
/// let pool = ThreadPool::new(4);
/// pool.execute(|| {
///     // some work here
/// });
/// // When `pool` goes out of scope, all worker threads are joined.
/// ```
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

/// Type alias for a job to be executed by the thread pool.
type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    /// Create a new ThreadPool with the given number of worker threads.
    ///
    /// Panics if size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0, "ThreadPool size must be > 0");

        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    /// Execute a job on the thread pool.
    ///
    /// Panics if the pool has been shut down.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        let sender = self
            .sender
            .as_ref()
            .expect("ThreadPool has been shut down");
        sender.send(job).expect("Failed to send job to worker threads");
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Drop the sender so worker receiver loops will get an error and exit.
        self.sender.take();

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                // Join each worker thread to ensure graceful shutdown.
                let _ = thread.join();
            }
        }
    }
}

/// A worker thread in the thread pool.
/// Each worker waits for jobs and executes them.
struct Worker {
    _id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(_id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            // Lock the receiver and wait for a job. When all senders are dropped,
            // recv() returns Err and the worker exits.
            let message = receiver.lock().expect("Receiver lock poisoned").recv();
            match message {
                Ok(job) => {
                    job();
                }
                Err(_) => {
                    break;
                }
            }
        });

        Worker {
            _id,
            thread: Some(thread),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    };

    #[test]
    fn new_creates_correct_number_of_workers_and_sender() {
        let size = 4;
        let pool = ThreadPool::new(size);
        assert_eq!(pool.workers.len(), size);
        assert!(pool.sender.is_some());
        // drop to join threads cleanly
        drop(pool);
    }

    #[test]
    fn execute_runs_job() {
        let pool = ThreadPool::new(2);
        let (tx, rx) = mpsc::channel();

        pool.execute(move || {
            tx.send(42u32).unwrap();
        });

        // wait for the job to complete
        let received = rx.recv_timeout(Duration::from_secs(1)).expect("job did not run");
        assert_eq!(received, 42);
        drop(pool);
    }

    #[test]
    fn execute_runs_multiple_jobs_and_updates_counter() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let jobs = 20;

        for _ in 0..jobs {
            let c = Arc::clone(&counter);
            pool.execute(move || {
                // do a little work
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        // wait up to 1s for all jobs to complete
        for _ in 0..100 {
            if counter.load(Ordering::SeqCst) == jobs {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(counter.load(Ordering::SeqCst), jobs);
        drop(pool);
    }

    #[test]
    #[should_panic(expected = "ThreadPool has been shut down")]
    fn execute_after_sender_taken_panics() {
        let pool = ThreadPool::new(2);
        // simulate shutdown by taking the sender
        let mut pool = pool;
        pool.sender.take();
        pool.execute(|| {
            // should not get here
        });
    }
}
