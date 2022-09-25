//!

use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::{BlockData, Difference, HashMap128, HashResult, Segment, Signature, SignatureBuilder};

std::thread_local!(
    ///
    pub static WORKER_POOL: WorkerPool = WorkerPool::new(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4),
    );
);
type SendableFunction = Option<Box<dyn FnOnce() + Send>>;
///
#[derive(Debug)]
pub struct WorkerPool {
    sender: mpsc::Sender<SendableFunction>,
    workers: usize,
}
impl WorkerPool {
    fn new(workers: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<SendableFunction>();
        let receiver = Arc::new(Mutex::new(receiver));
        // send `None` to all `workers` when we want to quit.
        for _ in 0..workers {
            let receiver = Arc::clone(&receiver);
            thread::spawn(move || loop {
                let action = { receiver.lock().unwrap().recv() };
                match action {
                    Err(_) | Ok(None) => break,
                    Ok(Some(v)) => {
                        v();
                    }
                }
            });
        }

        Self { sender, workers }
    }
    /// Kill all the threads of this worker pool.
    /// Does not wait for the threads to finish. [`Self::wait`] can be used after this to achieve
    /// that behaviour.
    pub fn kill(&mut self) {
        for _ in 0..self.workers {
            self.sender.send(None).expect("workers are already killed");
        }
    }
    // fn send(&self, f: impl FnOnce() + Send + 'static) {
    // self.sender
    // .send(Some(Box::new(f)))
    // .expect("children are killed");
    // }
    // fn spawn<T: Send + 'static>(
    // &self,
    // f: impl FnOnce() -> T + Send + 'static,
    // sender: &mpsc::Sender<T>,
    // ) {
    // let sender = sender.clone();
    // self.send(move || drop(sender.send(f())));
    // }
    fn scope<'a, T: Send + 'static>(
        &'a self,
        scope: impl FnOnce(&mut PoolScope<'a, T>),
    ) -> Vec<(usize, T)> {
        let (sender, rx) = mpsc::channel();
        let mut s = PoolScope {
            sender,
            rx,
            left: 0,
            wp: self,
            output: Vec::new(),
        };
        scope(&mut s);
        s.wait()
    }
}
impl Clone for WorkerPool {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            workers: 0,
        }
    }
}
struct PoolScope<'a, T: Send + 'static> {
    rx: mpsc::Receiver<(usize, T)>,
    left: usize,
    wp: &'a WorkerPool,
    sender: mpsc::Sender<(usize, T)>,
    output: Vec<(usize, T)>,
}
impl<'a, T: Send + 'static> PoolScope<'a, T> {
    fn spawn(&mut self, f: impl FnOnce() -> T + Send + 'a) {
        // inline WorkerPool's spawn and send methods to remove a boxing of the function
        let sender = self.sender.clone();
        let i = self.left;
        let f = move || drop(sender.send((i, f())));

        // SAFETY: we wait for all values to be received before returning from the function.
        let f = unsafe {
            std::mem::transmute::<Box<dyn FnOnce() + Send + 'a>, Box<dyn FnOnce() + Send + 'static>>(
                Box::new(f),
            )
        };

        self.wp.sender.send(Some(f)).expect("children are killed");

        self.left += 1;
    }
    /// should only be called once
    ///
    /// The usize is the index it can be ignored as it's an artefact from the message merging.
    fn wait(&mut self) -> Vec<(usize, T)> {
        while self.left > 0 {
            self.output.push(self.rx.recv().unwrap());
            self.left -= 1;
        }
        let mut v = std::mem::take(&mut self.output);
        v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        v
    }
}
impl<'a, T: Send + 'static> Drop for PoolScope<'a, T> {
    fn drop(&mut self) {
        self.wait();
    }
}

const PARALLEL_DATA_SIZE_THRESHOLD: usize = 32 * 1024;
const PARALLEL_BLOCK_SIZE: usize = 16 * 1024;

impl SignatureBuilder {
    /// Appends data to the hasher, hashing in parallel.
    /// Useful when `data` is large AND when `block_size` is > `8*1024`.
    ///
    /// This can be called multiple times to write the resource bit-by-bit.
    pub fn parallel_write(&mut self, data: &[u8], wp: &WorkerPool) {
        self.parallel_write_with_options(data, wp, PARALLEL_DATA_SIZE_THRESHOLD, 200);
    }
    ///
    pub fn parallel_write_with_options(
        &mut self,
        data: &[u8],
        wp: &WorkerPool,
        data_size_threshold: usize,
        block_size_threshold: usize,
    ) {
        enum HashOrBuilder {
            Hash(HashResult),
            Builder(Box<crate::HashBuilder>),
        }

        if data.len() < data_size_threshold || self.block_size < block_size_threshold {
            self.write(data);
            return;
        }

        self.total += data.len();

        let mut start = 0;
        let mut end = start + self.block_size;
        let hashes = wp.scope(|scope| {
            while start < data.len() {
                let first = start == 0;
                if first {
                    end = self.block_available();
                } else {
                    end = start + self.block_size;
                }

                let first_hash_builder = if first {
                    Some(Box::new(core::mem::replace(
                        &mut self.current,
                        // temporary value
                        crate::HashBuilder::None4(crate::StackSlice::default()),
                    )))
                } else {
                    None
                };

                let last = end >= data.len();
                end = end.min(data.len());

                let get_builder = || self.algo.builder(self.block_size);

                scope.spawn(move || {
                    let mut builder = first_hash_builder.map_or_else(get_builder, |b| *b);
                    builder.write(&data[start..end], None);

                    if last {
                        HashOrBuilder::Builder(Box::new(builder))
                    } else {
                        HashOrBuilder::Hash(builder.finish())
                    }
                });
                if !first {
                    self.len = 0;
                }
                if last {
                    self.len += end - start;
                }
                start = end;
            }
        });
        for (_, hash) in hashes {
            match hash {
                HashOrBuilder::Builder(b) => self.current = *b,
                HashOrBuilder::Hash(hash) => self.blocks.push(hash),
            }
        }
        if self.len == self.block_size {
            self.finish_hash();
        }
    }
}
impl Signature {
    /// Calculate the [`Self::diff`] in parallel using multiple OS threads.
    pub fn parallel_diff(&self, data: &[u8], wp: &WorkerPool) -> Difference {
        self.parallel_diff_with_options(data, wp, PARALLEL_DATA_SIZE_THRESHOLD, PARALLEL_BLOCK_SIZE)
    }
    /// Calculate the [`Self::diff`] in parallel using multiple OS threads.
    ///
    /// `data_size_threshold` is the threshold for how long `data` has to be before using the
    /// parallel implementation. If `data` is shorter than `data_size_threshold`, [`Self::diff`] is
    /// used instead.
    ///
    /// `parallel_block_size` is the size of the blocks `data` is split into before being sent to
    /// other threads.
    ///
    /// # Panics
    ///
    /// Panics if `parallel_block_size == 0`.
    #[allow(clippy::too_many_lines)]
    pub fn parallel_diff_with_options(
        &self,
        data: &[u8],
        wp: &WorkerPool,
        data_size_threshold: usize,
        parallel_block_size: usize,
    ) -> Difference {
        if data.len() < data_size_threshold {
            return self.diff(data);
        }
        assert!(parallel_block_size > 0);

        let mut map = HashMap128::new();

        let block_size = self.block_size();

        // Special case: Signature contains no hashes.
        // Just send the whole input.
        if self.blocks().is_empty() {
            let segments = if data.is_empty() {
                vec![]
            } else {
                vec![Segment::unknown(data)]
            };
            return Difference {
                segments,
                block_size,
                original_data_len: data.len(),
                parallel_data: None,
            };
        }

        for (nr, block) in self.blocks().iter().enumerate() {
            let bytes = block.to_bytes();

            let start = nr * block_size;
            let block_data = BlockData { start };

            map.insert(bytes, block_data);
        }

        let f = |start: usize, end: usize| {
            let segs = Self::inner_diff(data, start, end, block_size, &map, self.algorithm());
            let diff = Difference {
                block_size: self.block_size,
                original_data_len: 0,
                segments: segs,
                parallel_data: Some(crate::ParallelData {
                    parallel_block_size: parallel_block_size.try_into().unwrap(),
                    last_block_segment_length: if end == usize::MAX { 6 } else { 0 },
                }),
            };
            println!(
                "Handle {start}..{end} got length {}",
                diff.applied_len(data)
            );
            if start == 0 {
                println!("First diff {diff:?}");
            }
            diff.segments
        };

        let mut start = 0;
        let mut tot =0;
        let parallel_segments = wp.scope(|scope| {
            while data.len() > start + parallel_block_size || start == 0 {
                let end = if data.len() > start + parallel_block_size * 2 {
                    (start + parallel_block_size).min(data.len())
                } else {
                    println!("Last seg target {}", data.len()-start);
                    // last segment
                    // (longer than `parallel_block_size` so the last bit is extended into)
                    usize::MAX
                };
                scope.spawn(move || f(start, end));
                tot += end.min(data.len())-start;
                start += parallel_block_size;
            }
        });
        println!(" do tot {tot}");

        let mut segments = Vec::new();
        let last_block_segment_length = parallel_segments
            .last()
            .map_or(0, |last_block| last_block.1.len());
        let mut segs = parallel_segments.into_iter().flat_map(|(_, v)| v);
        let last_seg = segs.next();
        if let Some(mut last_seg) = last_seg {
            for seg in segs {
                match (&mut last_seg, &seg) {
                    (Segment::Ref(r1), Segment::Ref(r2)) => {
                        if r1.end(block_size) == r2.start() {
                            r1.block_count += r2.block_count();
                        } else {
                            segments.push(core::mem::replace(&mut last_seg, seg));
                        }
                    }
                    (Segment::Unknown(_), Segment::Ref(_))
                    | (Segment::Ref(_), Segment::Unknown(_)) => {
                        segments.push(core::mem::replace(&mut last_seg, seg));
                    }
                    (Segment::Unknown(u1), Segment::Unknown(u2)) => {
                        u1.data_mut().extend_from_slice(u2.data());
                    }
                }
            }
            segments.push(last_seg);
        }
        // else, we won't get any more segments

        Difference {
            segments,
            block_size,
            original_data_len: self.original_data_len,
            parallel_data: Some(crate::ParallelData {
                parallel_block_size: parallel_block_size.try_into().unwrap(),
                last_block_segment_length,
            }),
        }
    }
}
