//! A difference library similar to `rsync`.
//!
//! # Performance
//!
//! `den` is performant enough for large files and files, exceeding 10MB (when using rolling hashes).
//! Please compile with the **release** preset for **10X** the performance.
//!
//! Allocating data and keeping it in memory is very fast compared to hashing.
//! In the future, Den will support reading data bit by bit, greatly reducing the memory usage.
//!
//! # How-to & examples
//!
//! Keep in mind this isn't guaranteed to give the exact same data.
//! Please check the data with a secure hashing algorithm (e.g. SHA-3) to ensure consistency.
//!
//! Sending the data is possible due to [`serde`] providing serialization and deserialization.
//! You serialize all the structs in this library to any format.
//!
//! > These examples should cover what rsync does.
//!
//! ## Get a remote's data
//!
//! To get someone else's data, we construct a [`Signature`] and send it.
//! The remote calculates a [`Difference`] using [`Signature::diff`].
//! The remote sends back the [`Difference`] which we [`Difference::apply`].
//!
//! ## Push my data to remote
//!
//! Request the remote's [`Signature`].
//! They calculate it and respond.
//! We calculate a [`Signature::diff`] and send it to them.
//! They [`Difference::apply`] it. Their data should now be equal to mine.
//!
//! ## Get the difference of a local file
//!
//! Gets a small diff to send to others, almost like how `git` works.
//!
//! `base_data` is considered prior knowledge. `target_data` is the modified data.
//!
//! The data segments can be any size. Performance should still be good.
//!
//! ```
//! # use den::*;
//! let base_data = b"This is a document everyone has. It's about some new difference library.";
//! let target_data = b"This is a document only I have. It's about some new difference library.";
//!
//! let mut signature = Signature::new(128);
//! signature.write(base_data);
//! let signature = signature.finish();
//!
//! let diff = signature.diff(target_data);
//!
//! // This is the small diff you could serialize with Serde and send.
//! let minified = diff.minify(8, base_data)
//!     .expect("This won't panic, as the data hasn't changed from calling the other functions.");
//! ```
//!
//! # Future improvements
//!
//! - [x] Rolling hash
//! - [x] ~~Multi-threaded [`Signature::diff`].~~
//!       There is no feasible way to implement this, as we look ahead and change which window
//!       we're looking at after each iteration. Now with rolling hash, the performance is great.
//! - [ ] Support read/write
//!     - [ ] Support to diff a reader
//!     - [ ] Support to apply to a writer
//!     - [ ] Fetch API for apply to get data on demand.
//!         - This could slow things down dramatically.
//!     - [ ] Implement Write for `HashBuilder`.
//! - [x] ~~Use SHA(1|256?) to verify integrity of data. Bundled with the [`Signature`].~~ The
//!       implementer should provide this.

#![deny(
    clippy::all,
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::ops::Range;
use twox_hash::xxh3::HasherExt;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum LargeHash<K, V> {
    Single(V),
    Multiple(BTreeMap<K, V>),
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct LargeHashKey([u8; 8]);
struct HashMap128Hasher([u8; 8]);
impl Hasher for HashMap128Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.copy_from_slice(bytes);
    }
    fn finish(&self) -> u64 {
        u64::from_le_bytes(self.0)
    }
}
struct HashMap128HashBuilder;
impl BuildHasher for HashMap128HashBuilder {
    type Hasher = HashMap128Hasher;
    fn build_hasher(&self) -> Self::Hasher {
        HashMap128Hasher([0; 8])
    }
}
struct HashMap128 {
    map: HashMap<[u8; 8], LargeHash<[u8; 16], BlockData>, HashMap128HashBuilder>,
}
impl HashMap128 {
    fn new() -> Self {
        Self {
            map: HashMap::with_hasher(HashMap128HashBuilder),
        }
    }
    fn get(&self, key: [u8; 16]) -> Option<BlockData> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&key[..8]);
        if let Some(option) = self.map.get(&bytes) {
            match option {
                LargeHash::Single(block) => Some(*block),
                LargeHash::Multiple(tree) => tree.get(&key).copied(),
            }
        } else {
            None
        }
    }
    fn insert(&mut self, key: [u8; 16], value: BlockData) {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&key[..8]);
        if let Some(option) = self.map.get_mut(&bytes) {
            match option {
                LargeHash::Single(block) => {
                    let mut tree = BTreeMap::new();
                    tree.insert(key, *block);
                    *option = LargeHash::Multiple(tree);
                }
                LargeHash::Multiple(_) => {}
            }
            let tree = match option {
                LargeHash::Single(_) => {
                    unreachable!()
                }
                LargeHash::Multiple(tree) => tree,
            };

            tree.insert(key, value);
        } else {
            self.map.insert(bytes, LargeHash::Single(value));
        }
    }
}

/// The algorithms which can be used for hashing the data.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
#[must_use]
#[allow(missing_docs)]
pub enum HashAlgorithm {
    None4,
    None8,
    None16,
    Fnv,
    XXH3_64,
    XXH3_128,
    CyclicPoly32,
    CyclicPoly64,
    Adler32,
}
impl HashAlgorithm {
    #[inline]
    fn builder(self, block_size: usize) -> HashBuilder {
        match self {
            Self::None4 => HashBuilder::None4(StackSlice::default()),
            Self::None8 => HashBuilder::None8(StackSlice::default()),
            Self::None16 => HashBuilder::None16(StackSlice::default()),
            Self::Fnv => HashBuilder::Fnv(fnv::FnvHasher::default()),
            Self::XXH3_64 => HashBuilder::XXH3_64(twox_hash::Xxh3Hash64::default()),
            Self::XXH3_128 => HashBuilder::XXH3_128(twox_hash::Xxh3Hash128::default()),
            Self::CyclicPoly32 => HashBuilder::CyclicPoly32(CyclicPoly32::new(block_size)),
            Self::CyclicPoly64 => HashBuilder::CyclicPoly64(CyclicPoly64::new(block_size)),
            Self::Adler32 => HashBuilder::Adler32(Adler32::new(block_size)),
        }
    }
}

/// Used to make [`RollingHash`] generic.
///
/// This is implemented by the [Adler32](https://docs.rs/adler32/latest/adler32/)
/// algorithm and both variants of the
/// [cyclic poly 32](https://docs.rs/cyclic-poly-23/latest/cyclic_poly_23/) algorithm. The
/// implementation of cyclic poly 23 is in a [`Box`] due to the large size of the struct
/// (1KiB and 2KiB for the 32- and 64-bit implementations respectively).
pub trait RollingHasher {
    /// The hash returned from this hasher.
    type Hash;

    /// Create a new hasher.
    fn new(block_size: usize) -> Self;
    /// Reset the inner state. If the struct provides no such functionality, consider overriding
    /// the current value with a `new` instance.
    fn reset(&mut self, block_size: usize);
    /// Write the `block` to the hasher.
    fn update(&mut self, block: &[u8], block_size: usize);
    /// Remove `old` and add `new`.
    fn rotate(&mut self, old: u8, new: u8, block_size: usize);
    /// Get the current internal hash.
    fn value(&self) -> Self::Hash;
}
impl RollingHasher for adler32::RollingAdler32 {
    type Hash = u32;

    fn new(_: usize) -> Self {
        Self::new()
    }
    fn reset(&mut self, _: usize) {
        *self = Self::new();
    }
    fn update(&mut self, block: &[u8], _: usize) {
        self.update_buffer(block);
    }
    fn rotate(&mut self, old: u8, new: u8, block_size: usize) {
        self.remove(block_size, old);
        self.update(new);
    }
    fn value(&self) -> Self::Hash {
        self.hash()
    }
}
impl RollingHasher for Box<cyclic_poly_23::CyclicPoly32> {
    type Hash = u32;

    fn new(block_size: usize) -> Self {
        Box::new(cyclic_poly_23::CyclicPoly32::new(block_size))
    }
    fn reset(&mut self, _: usize) {
        self.reset_hash();
    }
    fn update(&mut self, block: &[u8], _: usize) {
        (**self).update(block);
    }
    fn rotate(&mut self, old: u8, new: u8, _: usize) {
        (**self).rotate(old, new);
    }
    fn value(&self) -> Self::Hash {
        (**self).value()
    }
}
impl RollingHasher for Box<cyclic_poly_23::CyclicPoly64> {
    type Hash = u64;

    fn new(block_size: usize) -> Self {
        Box::new(cyclic_poly_23::CyclicPoly64::new(block_size))
    }
    fn reset(&mut self, _: usize) {
        self.reset_hash();
    }
    fn update(&mut self, block: &[u8], _: usize) {
        (**self).update(block);
    }
    fn rotate(&mut self, old: u8, new: u8, _: usize) {
        (**self).rotate(old, new);
    }
    fn value(&self) -> Self::Hash {
        (**self).value()
    }
}

/// A generic rolling hash implementation.
#[derive(Debug)]
pub struct RollingHash<T: RollingHasher> {
    inner: T,
    data: VecDeque<u8>,
    block_size: usize,
    last_position: usize,
    write_data: bool,
    reset_data: bool,
}
impl<T: RollingHasher> RollingHash<T> {
    fn new(block_size: usize) -> Self {
        Self {
            inner: T::new(block_size),
            data: VecDeque::with_capacity(block_size),
            block_size,
            last_position: 0,
            write_data: false,
            reset_data: false,
        }
    }
    fn write_data(&mut self) {
        if self.write_data {
            let data = self.data.make_contiguous();
            self.inner.reset(self.block_size);
            self.inner.update(data, self.block_size);
            self.write_data = false;
        }
    }
    fn write(&mut self, data: &[u8], position: Option<usize>) {
        if self.data.len() == self.block_size && data.len() == self.data.len() {
            if let Some(pos) = position {
                // assuming data is from the same source
                if pos == self.last_position + 1 {
                    // flush
                    self.write_data();

                    let first = self.data.pop_front().unwrap();
                    let new = *data.last().unwrap();
                    self.data.push_back(new);
                    self.inner.rotate(first, new, self.block_size);
                    self.last_position = pos;

                    return;
                }
            }
        }

        if self.reset_data {
            self.data.clear();
        }
        // normal execution
        self.data.extend(data);
        self.last_position = position.unwrap_or(0);
        self.write_data = true;
    }
    fn finish_reset(&mut self) -> T::Hash {
        let wrote_data = self.write_data;
        self.write_data();
        if wrote_data {
            self.reset_data = true;
        }
        self.inner.value()
    }
}

/// [`RollingHash`] using the 32-bit cyclic poly 23 algorithm.
///
/// Implements the trait [`RollingHasher`].
pub type CyclicPoly32 = RollingHash<Box<cyclic_poly_23::CyclicPoly32>>;
/// [`RollingHash`] using the 32-bit cyclic poly 23 algorithm.
///
/// Implements the trait [`RollingHasher`].
pub type CyclicPoly64 = RollingHash<Box<cyclic_poly_23::CyclicPoly64>>;
/// [`RollingHash`] using the Adler32 algorithm.
///
/// Implements the trait [`RollingHasher`].
pub type Adler32 = RollingHash<adler32::RollingAdler32>;

enum HashBuilder {
    None4(StackSlice<4>),
    None8(StackSlice<8>),
    None16(StackSlice<16>),
    Fnv(fnv::FnvHasher),
    XXH3_64(twox_hash::Xxh3Hash64),
    XXH3_128(twox_hash::Xxh3Hash128),
    CyclicPoly32(CyclicPoly32),
    CyclicPoly64(CyclicPoly64),
    Adler32(Adler32),
}
impl HashBuilder {
    #[inline]
    fn finish_reset(&mut self) -> HashResult {
        match self {
            Self::None4(h) => {
                let r = HashResult::None4(h.finish().to_le_bytes());
                *h = StackSlice::default();
                r
            }
            Self::None8(h) => {
                let r = HashResult::None8(h.finish().to_le_bytes());
                *h = StackSlice::default();
                r
            }
            Self::None16(h) => {
                let r = HashResult::None16(h.finish().to_le_bytes());
                *h = StackSlice::default();
                r
            }
            Self::Fnv(h) => {
                let r = HashResult::Fnv(h.finish().to_le_bytes());
                *h = fnv::FnvHasher::default();
                r
            }
            Self::XXH3_64(h) => {
                let r = HashResult::XXH3_64(h.finish().to_le_bytes());
                *h = twox_hash::Xxh3Hash64::default();
                r
            }
            Self::XXH3_128(h) => {
                let r = HashResult::XXH3_128(h.finish_ext().to_le_bytes());
                *h = twox_hash::Xxh3Hash128::default();
                r
            }
            Self::CyclicPoly32(h) => HashResult::CyclicPoly32(h.finish_reset().to_le_bytes()),
            Self::CyclicPoly64(h) => HashResult::CyclicPoly64(h.finish_reset().to_le_bytes()),
            Self::Adler32(h) => HashResult::Adler32(h.finish_reset().to_le_bytes()),
        }
    }
    #[inline]
    fn write(&mut self, data: &[u8], position: Option<usize>) {
        match self {
            Self::None4(h) => h.write(data),
            Self::None8(h) => h.write(data),
            Self::None16(h) => h.write(data),
            Self::Fnv(h) => h.write(data),
            Self::XXH3_64(h) => h.write(data),
            Self::XXH3_128(h) => h.write(data),
            Self::CyclicPoly32(h) => h.write(data, position),
            Self::CyclicPoly64(h) => h.write(data, position),
            Self::Adler32(h) => h.write(data, position),
        }
    }
}

impl Debug for HashBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HashBuilder (internal hasher data)")
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Serialize, Deserialize)]
#[must_use]
enum HashResult {
    None4([u8; 4]),
    None8([u8; 8]),
    None16([u8; 16]),
    Fnv([u8; 8]),
    XXH3_64([u8; 8]),
    XXH3_128([u8; 16]),
    CyclicPoly32([u8; 4]),
    CyclicPoly64([u8; 8]),
    Adler32([u8; 4]),
}
impl HashResult {
    fn to_bytes(self) -> [u8; 16] {
        match self {
            Self::None4(bytes) | Self::CyclicPoly32(bytes) | Self::Adler32(bytes) => {
                to_16_bytes(&bytes)
            }
            Self::None8(bytes)
            | Self::Fnv(bytes)
            | Self::XXH3_64(bytes)
            | Self::CyclicPoly64(bytes) => to_16_bytes(&bytes),
            Self::None16(bytes) | Self::XXH3_128(bytes) => to_16_bytes(&bytes),
        }
    }
}
impl Debug for HashResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HashResult({:0<16X})",
            u128::from_le_bytes(self.to_bytes())
        )
    }
}

#[derive(Debug)]
struct StackSlice<const SIZE: usize> {
    data: [u8; SIZE],
    len: u8,
}
impl<const SIZE: usize> Default for StackSlice<SIZE> {
    fn default() -> Self {
        Self {
            data: zeroed(),
            len: 0,
        }
    }
}
impl<const SIZE: usize> StackSlice<SIZE> {
    /// # Panics
    ///
    /// Panics if `data.len()` > [`Self::available`].
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn write(&mut self, data: &[u8]) {
        // SAFETY: Needed for the block below.
        assert!(
            data.len() <= self.available(),
            "Length ({}) is greater than what's available ({})",
            data.len(),
            self.available()
        );
        // SAFETY: We've checked above the guarantees hold up.
        unsafe {
            self.data
                .get_unchecked_mut(self.len as usize..self.len as usize + data.len())
        }
        .copy_from_slice(data);

        // The assert above guarantees we never reach this point if the input is too large.
        self.len += data.len() as u8;
    }
    #[inline]
    fn available(&self) -> usize {
        SIZE - self.len as usize
    }
}
impl StackSlice<4> {
    fn finish(&self) -> u32 {
        u32::from_ne_bytes(self.data)
    }
}
impl StackSlice<8> {
    fn finish(&self) -> u64 {
        u64::from_ne_bytes(self.data)
    }
}
impl StackSlice<16> {
    fn finish(&self) -> u128 {
        u128::from_ne_bytes(self.data)
    }
}

#[must_use]
fn to_16_bytes<const SIZE: usize>(bytes: &[u8; SIZE]) -> [u8; 16] {
    let mut bytes_fixed = zeroed();

    bytes_fixed[..SIZE].copy_from_slice(bytes);

    bytes_fixed
}

#[must_use]
const fn zeroed<const SIZE: usize>() -> [u8; SIZE] {
    [0; SIZE]
}

/// Builder of a [`Signature`].
/// Created using constructors on [`Signature`] (e.g. [`Signature::with_algorithm`]);
///
/// You [`Self::write`] data and then [`Self::finish`] to get a [`Signature`].
#[derive(Debug)]
#[must_use]
pub struct SignatureBuilder {
    algo: HashAlgorithm,
    blocks: Vec<HashResult>,
    block_size: usize,

    current: HashBuilder,
    len: usize,
}
impl SignatureBuilder {
    /// The `hasher` is used as the template hasher from which all other hashers are cloned.
    ///
    /// A good block size is `1024`.
    fn new(algo: HashAlgorithm, block_size: usize) -> Self {
        Self {
            algo,
            blocks: Vec::new(),
            block_size,

            current: algo.builder(block_size),
            len: 0,
        }
    }
    const fn block_available(&self) -> usize {
        self.block_size - self.len
    }
    fn finish_hash(&mut self) {
        let result = self.current.finish_reset();
        self.blocks.push(result);
        self.len = 0;
    }
    /// Appends data to the hasher.
    ///
    /// This can be called multiple times to write the resource bit-by-bit.
    pub fn write(&mut self, data: &[u8]) {
        let mut data = data;
        let mut position = 0;

        while data.len() >= self.block_available() {
            let bytes = &data[..self.block_available()];
            self.current.write(bytes, Some(position));

            data = &data[self.block_available()..];
            position += bytes.len();
            self.len += bytes.len();
            self.finish_hash();
        }
        self.len += data.len();
        // the data is now less than `self.block_available()`.
        self.current.write(data, Some(position));
    }
    /// Flushes the data from [`Self::write`] and prepares a [`Signature`].
    pub fn finish(mut self) -> Signature {
        self.finish_hash();

        let Self {
            algo,
            blocks,
            block_size,
            ..
        } = self;

        Signature {
            algo,
            blocks,
            block_size,
        }
    }
}
/// A identifier of a file, much smaller than the file itself.
///
/// See [crate-level documentation](crate) for more details.
#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Signature {
    algo: HashAlgorithm,
    blocks: Vec<HashResult>,
    block_size: usize,
}
impl Signature {
    /// Creates a new [`SignatureBuilder`] in which data can be added to create a list of hashes
    /// for blocks the size of `block_size`.
    ///
    /// I recommend block size `4096` for remote transfers.
    /// If small documents are what's mostly being transmitted, consider `512`.
    /// Consider running [`Difference::minify`] if getting the smallest diff is your concern.
    ///
    /// This creates a signature of a resource. The signature takes up much less space.
    ///
    /// The [`HashAlgorithm`] is chosen using experience with hasher's performance and heuristics.
    #[allow(clippy::new_ret_no_self)] // This returns a builder.
    pub fn new(block_size: usize) -> SignatureBuilder {
        match block_size {
            0..=4 => Signature::with_algorithm(HashAlgorithm::None4, block_size),
            5..=8 => Signature::with_algorithm(HashAlgorithm::None8, block_size),
            9..=16 => Signature::with_algorithm(HashAlgorithm::None16, block_size),
            17..=511 => Signature::with_algorithm(HashAlgorithm::XXH3_64, block_size),
            // 512..
            _ => Signature::with_algorithm(HashAlgorithm::XXH3_128, block_size),
        }
    }
    /// This will create a new [`SignatureBuilder`] with the `algorithm`. Consider using [`Self::new`]
    /// if you don't know *exactly* what you are doing, as it sets the algorithm for you.
    /// You can query the algorithm using [`Self::algorithm`].
    ///
    /// Larger `block_size`s will take more time to compute, but will be more secure.
    /// Smaller `block_size`s takes less time to compute, are less secure, and require sending more
    /// data in the [`Signature`], as more blocks are sent.
    ///
    /// # Panics
    ///
    /// Will panic if [`HashAlgorithm`] is of type `None*` and `block_size` is larger than the
    /// `HashAlgorithm`.
    /// Also panics if `block_size` is `0`.
    pub fn with_algorithm(algorithm: HashAlgorithm, block_size: usize) -> SignatureBuilder {
        assert_ne!(block_size, 0, "Block size cannot be 0.");
        match algorithm {
            HashAlgorithm::None4 => assert!(block_size <= 4),
            HashAlgorithm::None8 => assert!(block_size <= 8),
            HashAlgorithm::None16 => assert!(block_size <= 16),
            _ => {}
        }
        SignatureBuilder::new(algorithm, block_size)
    }

    /// Get the algorithm used by this signature.
    pub const fn algorithm(&self) -> HashAlgorithm {
        self.algo
    }
    /// Returns the block size of this signature.
    #[must_use]
    pub const fn block_size(&self) -> usize {
        self.block_size
    }
    /// Gets the hashes for all the blocks of the input resource.
    pub(crate) fn blocks(&self) -> &[HashResult] {
        &self.blocks
    }

    /// Get the [`Difference`] between the data the [`Signature`] represents and the local `data`.
    ///
    /// This will return a struct which when serialized (using e.g. `bincode`) is much smaller than
    /// `data`.
    #[allow(clippy::too_many_lines)] // well, this is the main implementation
    pub fn diff(&self, data: &[u8]) -> Difference {
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn push_unknown_data(
            data: &[u8],
            last_ref: usize,
            blocks_pos: usize,
            segments: &mut Vec<Segment>,
        ) {
            let unknown_data = &data[last_ref..blocks_pos - 1];
            if !unknown_data.is_empty() {
                segments.push(Segment::unknown(unknown_data));
            }
        }
        let mut map = HashMap128::new();

        let block_size = self.block_size();

        // Special case: Signature contains no hashes.
        // Just send the whole input.
        if self.blocks().is_empty() {
            let data = Segment::unknown(data);
            return Difference {
                segments: vec![data],
                block_size,
            };
        }

        for (nr, block) in self.blocks().iter().enumerate() {
            let bytes = block.to_bytes();

            let start = nr * block_size;
            let block_data = BlockData { start };

            map.insert(bytes, block_data);
        }

        let mut blocks = Blocks::new(data, block_size);

        let mut segments = Vec::new();
        let mut last_ref_block = 0;

        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            clippy::cast_sign_loss
        )]
        let lookahead_limit = block_size.min((((block_size / 8) as f64).sqrt() as usize).max(8));
        // look into if it's better to not use this.
        // That'd enable trying to find the best any distans in front, but might produce very long
        // unknown segments.
        let mut lookahead_ignore = 0;

        let mut hasher = self.algorithm().builder(block_size);

        // Iterate over data, in windows. Find hash.
        while let Some(block) = blocks.next() {
            hasher.write(block, Some(blocks.pos() - 1));
            let hash = hasher.finish_reset().to_bytes();

            // If hash matches, push previous data to unknown, and push a ref. Advance by `block_size` (actually `block_size-1` as the `next` call implicitly increments).
            if let Some(block_data) = map.get(hash) {
                // If we're looking for a ref (after a unknown, which hasn't been "committed" yet),
                // check with a offset of 0..<some small integer> for better matches, which give us
                // longer BlockRefs. This reduces jitter in output.
                if lookahead_ignore == 0 {
                    let mut best: Option<(usize, usize)> = None;
                    for offset in 0..lookahead_limit {
                        let mut blocks = blocks.clone();
                        blocks.go_back(1);
                        blocks.advance(offset);
                        let mut last_end = None;

                        let mut successive_block_count = 0;
                        for i in 0..lookahead_limit {
                            if let Some(block) = blocks.next() {
                                // check hash(clone rolling hash), if matches with end of prior,
                                // add 1 to current score & max = max.max(current)
                                // let mut hasher = self.algorithm().builder(block_size);
                                hasher.write(block, Some(blocks.pos() - 1));
                                let hash = hasher.finish_reset().to_bytes();

                                if let Some(block_data) = map.get(hash) {
                                    match &mut last_end {
                                        Some(end) => {
                                            if block_data.start == *end {
                                                // successive block

                                                debug_assert_eq!(i, successive_block_count);
                                                successive_block_count += 1;
                                                *end = block_data.start + block_size;

                                                blocks.advance(block.len() - 1);
                                            } else {
                                                // block is not directly after the other
                                                break;
                                            }
                                        }
                                        None => {
                                            successive_block_count += 1;
                                            last_end = Some(block_data.start + block_size);
                                        }
                                    }
                                } else {
                                    // no match, break
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        match &mut best {
                            Some(best) => {
                                let (_iter, successive) = *best;
                                if successive < successive_block_count {
                                    *best = (offset, successive_block_count);
                                } else {
                                    // we're worse than the prior loops
                                }
                            }
                            None => {
                                best = Some((offset, successive_block_count));
                            }
                        }
                    }
                    match best {
                        // the current one is the best, stick with this.
                        Some((0, ignore)) => lookahead_ignore = ignore,
                        // didn't find any better
                        None => {}
                        Some((n, ignore)) => {
                            // -1 as the next should not advance the position, it's naturally the
                            // next.
                            blocks.advance(n - 1);
                            lookahead_ignore = ignore;
                            continue;
                        }
                    }
                }
                // decrement the ignored
                lookahead_ignore = lookahead_ignore.saturating_sub(1);

                push_unknown_data(data, last_ref_block, blocks.pos(), &mut segments);

                if let Some(last) = segments.last_mut() {
                    match last {
                        Segment::Ref(ref_segment) => {
                            if block_data.start == ref_segment.start + block_size {
                                let mut segment = SegmentBlockRef::from(*ref_segment);
                                segment.extend(1);
                                *last = Segment::BlockRef(segment);
                            } else {
                                segments.push(Segment::reference(block_data));
                            }
                        }
                        Segment::BlockRef(block_ref_segment) => {
                            if block_data.start == block_ref_segment.end(block_size) {
                                block_ref_segment.extend(1);
                            } else {
                                segments.push(Segment::reference(block_data));
                            }
                        }
                        Segment::Unknown(_) => {
                            segments.push(Segment::reference(block_data));
                        }
                    }
                } else {
                    segments.push(Segment::reference(block_data));
                }

                blocks.advance(block.len() - 1);
                last_ref_block = blocks.pos();
            }
        }

        // If we want them to get our diff, we send our diff, with the `Unknown`s filled with data.
        // Then, we only send references to their data and our new data.
        push_unknown_data(data, last_ref_block, blocks.pos() + 1, &mut segments);

        Difference {
            segments,
            block_size,
        }
    }
}
impl Debug for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let blocks: Vec<String> = self
            .blocks()
            .iter()
            .map(|block| format!("{:0<16X}", u128::from_le_bytes(block.to_bytes())))
            .collect();
        f.debug_struct("Signature")
            .field("algo", &self.algo)
            .field("blocks", &blocks)
            .field("block_size", &self.block_size)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockData {
    start: usize,
}
/// A segment with a reference to the base data.
///
/// Use [`SegmentBlockRef`] if several blocks in succession reference the same successive data in
/// the base data.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[must_use]
pub struct SegmentRef {
    /// Start of segment with a length of the [`Signature::block_size`].
    start: usize,
}
impl SegmentRef {
    /// The start in the base resource this reference is pointing to.
    #[must_use]
    pub fn start(self) -> usize {
        self.start
    }
    /// The end of this segment.
    ///
    /// The same as [`Self::start`] + `block_size`.
    #[inline]
    #[must_use]
    pub fn end(self, block_size: usize) -> usize {
        self.start() + block_size
    }
}
/// Several [`SegmentRef`] after each other.
///
/// This is a separate struct to limit serialized size.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[must_use]
pub struct SegmentBlockRef {
    /// Start of segment with a length of [`Self::block_count`]*[`Signature::block_size`].
    start: usize,
    block_count: usize,
}
impl SegmentBlockRef {
    /// The start in the base resource this reference is pointing to.
    #[must_use]
    pub fn start(self) -> usize {
        self.start
    }
    /// The number of blocks this reference covers.
    #[must_use]
    pub fn block_count(self) -> usize {
        self.block_count
    }
    /// The end of this segment.
    ///
    /// The same as [`Self::start`] + [`Self::block_count`] * `block_size`.
    #[inline]
    #[must_use]
    pub fn end(self, block_size: usize) -> usize {
        self.start + self.block_count * block_size
    }
    #[inline]
    fn extend(&mut self, n: usize) {
        self.block_count += n;
    }
    /// Multiplies the count of blocks.
    /// Can be useful if the block size changes.
    #[inline]
    fn multiply(&mut self, n: usize) {
        self.block_count *= n;
    }
}
impl From<SegmentRef> for SegmentBlockRef {
    #[inline]
    fn from(ref_segment: SegmentRef) -> Self {
        SegmentBlockRef {
            start: ref_segment.start,
            block_count: 1,
        }
    }
}
/// A segment with unknown contents. This will transmit the data.
#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct SegmentUnknown {
    source: Vec<u8>,
}
impl SegmentUnknown {
    /// Gets a reference to the data transmitted.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.source
    }
    /// Gets a mutable reference to the internal data [`Vec`].
    pub fn data_mut(&mut self) -> &mut Vec<u8> {
        &mut self.source
    }
    /// Takes the internal data of this segment.
    #[must_use]
    pub fn into_data(self) -> Vec<u8> {
        self.source
    }
}
impl Debug for SegmentUnknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentUnknown")
            .field("source", &String::from_utf8_lossy(&self.source))
            .finish()
    }
}
/// A segment of data corresponding to a multiple of [`Difference::block_size`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum Segment {
    /// A reference to a block of data.
    ///
    /// This is separate from [`Self::BlockRef`] to save 8 bytes when only 1 ref block is found.
    /// It's feasible we add more of these, but with block counts higher than 1 in the future.
    Ref(SegmentRef),
    /// Reference to successive blocks of data.
    BlockRef(SegmentBlockRef),
    /// Data unknown to the one who sent the [`Signature`].
    Unknown(SegmentUnknown),
}
impl Segment {
    #[inline]
    fn reference(data: BlockData) -> Self {
        Self::Ref(SegmentRef { start: data.start })
    }
    /// Creates a [`Segment::Unknown`] with `data` as the unknown part.
    ///
    /// Should mainly be used in testing.
    #[inline]
    pub fn unknown(data: impl Into<Vec<u8>>) -> Self {
        Self::Unknown(SegmentUnknown {
            source: data.into(),
        })
    }
}
/// A delta between the local data and the data the [`Signature`] represents.
#[allow(clippy::unsafe_derive_deserialize)] // See SAFETY notes.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Difference {
    segments: Vec<Segment>,
    block_size: usize,
}
impl Difference {
    /// Returns a reference to all the internal [`Segment`]s.
    ///
    /// This can be used for implementing algorithms other than [`Self::apply`] to apply the data.
    ///
    /// > `agde` uses this to convert from this format to their `Section` style.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
    /// Turns this difference into it's list of segments.
    ///
    /// Prefer to use [`Self::segments`] if you don't plan on consuming the internal data.
    #[must_use]
    pub fn into_segments(self) -> Vec<Segment> {
        self.segments
    }
    /// The block size used by this diff.
    #[must_use]
    pub const fn block_size(&self) -> usize {
        self.block_size
    }

    /// Returns whether or not applying this diff is impossible to do on a single [`Vec`].
    ///
    /// If the returned value is `true`, the apply function will try to read data from parts of the
    /// `Vec` already overridden.
    /// You should be able to use a single `Vec` if the returned value is `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use den::*;
    /// let base_data = b"This is a document everyone has. It's about some new difference library.";
    /// let target_data = b"This is a document only I have. It's about some new difference library.";
    /// let mut base_data = base_data.to_vec();
    ///
    /// let mut signature = Signature::new(128);
    /// signature.write(&base_data);
    /// let signature = signature.finish();
    ///
    /// let diff = signature.diff(target_data);
    ///
    /// // This is the small diff you could serialize with Serde and send.
    /// let minified = diff.minify(8, &base_data)
    ///     .expect("This won't panic, as the data hasn't changed from calling the other functions.");
    ///
    /// let data = if minified.apply_overlaps() {
    ///     let mut data = Vec::new();
    ///     minified.apply(&base_data, &mut data);
    ///     data
    /// } else {
    ///     minified.apply_in_place(&mut base_data);
    ///     base_data
    /// };
    ///
    /// assert_eq!(data, target_data);
    /// ```

    #[must_use]
    pub fn apply_overlaps(&self) -> bool {
        let mut position = 0;
        for segment in self.segments() {
            match segment {
                Segment::Ref(seg) => {
                    if seg.start() < position {
                        return true;
                    }
                    position += self.block_size();
                }
                Segment::BlockRef(seg) => {
                    if seg.start() < position {
                        return true;
                    }
                    position += seg.block_count() * self.block_size();
                }
                Segment::Unknown(seg) => {
                    position += seg.data().len();
                }
            }
        }
        false
    }

    /// Changes the block size to `block_size`, shrinking the [`Segment::Unknown`]s in the process.
    /// This results in a smaller diff.
    ///
    /// # Errors
    ///
    /// tl;dr, you can [`Option::unwrap`] this if it comes straight from [`Signature::diff`] or
    /// this very function. If it's untrusted data, handle the errors.
    ///
    /// Returns [`MinifyError::NewLarger`] if `block_size` >= [`Self::block_size`] and
    /// [`MinifyError::NotMultiple`] if [`Self::block_size`] is not a multiple of `block_size`.
    /// [`MinifyError::SuccessiveUnknowns`] is returned if two [`Segment::Unknown`] are after
    /// each other.
    #[allow(clippy::too_many_lines)]
    pub fn minify(&self, block_size: usize, base: &[u8]) -> Result<Self, MinifyError> {
        #[cold]
        #[inline(never)]
        fn unreachable_reference_segment() {
            unreachable!("This function should only get BlockRef or Unknown. Report this bug.");
        }
        fn push_segment(segments: &mut Vec<Segment>, item: Segment, block_size: usize) {
            match item {
                Segment::BlockRef(item) => {
                    if let Some(last) = segments.last_mut() {
                        match last {
                            Segment::BlockRef(seg) => {
                                if seg.end(block_size) == item.start {
                                    // The previous end is this start.
                                    seg.extend(item.block_count());
                                } else {
                                    // Check if block segment is only reference.
                                    // This will never happen above, as we always add to it.
                                    // This assumes [`SegmentBlockRef::block_count`] is >= 1
                                    // if item.block_count() == 1 {
                                    // segments
                                    // .push(Segment::Ref(SegmentRef { start: item.start() }));
                                    // } else {
                                    segments.push(Segment::BlockRef(item));
                                    // }
                                    // let seg = *seg;
                                    // if seg.block_count() == 1 {
                                    // segments
                                    // .push(Segment::Ref(SegmentRef { start: seg.start() }));
                                    // } else {
                                    // segments.push(Segment::BlockRef(seg));
                                    // }
                                }
                            }
                            Segment::Unknown(_) => segments.push(Segment::BlockRef(item)),
                            Segment::Ref(_) => unreachable_reference_segment(),
                        }
                    } else {
                        segments.push(Segment::BlockRef(item));
                    }
                }
                Segment::Unknown(_) => segments.push(item),
                Segment::Ref(_) => unreachable_reference_segment(),
            }
        }

        if self.block_size() <= block_size {
            return Err(MinifyError::NewLarger);
        }
        let block_size_shrinkage = self.block_size() / block_size;
        let block_size_shrinkage_remainder = self.block_size() % block_size;
        if block_size_shrinkage_remainder != 0 {
            return Err(MinifyError::NotMultiple);
        }

        // `self` can be applied to `base` to get `target`.
        // We want to minify the `unknown` segments of `self`.

        // Use heuristics to allocate what we have * 1.2, if any more segments are added.
        let mut segments = Vec::with_capacity(self.segments().len() * 6 / 5);
        for (last, current, next) in PrePostWindow::new(self.segments()) {
            match current {
                Segment::Ref(seg) => {
                    let mut block_seg: SegmentBlockRef = (*seg).into();
                    // Multiply all block lengths by `block_size_shrinkage` to make them the length
                    // of new `block_size`.
                    block_seg.multiply(block_size_shrinkage);
                    push_segment(&mut segments, Segment::BlockRef(block_seg), block_size);
                }
                Segment::BlockRef(seg) => {
                    let mut seg = *seg;
                    seg.multiply(block_size_shrinkage);
                    push_segment(&mut segments, Segment::BlockRef(seg), block_size);
                }
                Segment::Unknown(seg) => {
                    let target_data = seg.data();
                    let base_data = {
                        // Start = previous end
                        // end = next's start.
                        // If it's a reference, that's start. Else, ~~simply the length of
                        // `target_data`.~~ that shouldn't happen!
                        //
                        // Get a peek to the next segment.
                        let start = if let Some(last) = last {
                            match last {
                                // Start = previous end
                                // end = next's start.
                                // If it's a reference, that's start. Else, simply the length of
                                // `target_data`.
                                Segment::Ref(seg) => seg.end(block_size),
                                Segment::BlockRef(seg) => seg.end(block_size),
                                // Improbable, two Unknowns can't be after each other.
                                // Only through external modification of data.
                                Segment::Unknown(_) => return Err(MinifyError::SuccessiveUnknowns),
                            }
                        } else {
                            0
                        };
                        let mut end = match next {
                            Some(Segment::Ref(seg)) => seg.start,
                            Some(Segment::BlockRef(seg)) => seg.start,
                            Some(Segment::Unknown(_)) => {
                                return Err(MinifyError::SuccessiveUnknowns)
                            }
                            None => base.len(),
                        };
                        if end < start {
                            end = start + seg.data().len();
                        }
                        end = cmp::min(end, base.len());

                        base.get(start..end).map(|slice| (slice, start))
                    };

                    if let Some((base_data, start)) = base_data {
                        let mut builder = Signature::new(block_size);
                        builder.write(base_data);
                        let signature = builder.finish();

                        let diff = signature.diff(target_data);

                        for mut segment in diff.segments {
                            match &mut segment {
                                Segment::Ref(seg) => {
                                    seg.start += start;
                                    segment = Segment::BlockRef((*seg).into());
                                }
                                Segment::BlockRef(seg) => seg.start += start,
                                Segment::Unknown(_) => {}
                            }
                            push_segment(&mut segments, segment, block_size);
                        }
                    } else {
                        segments.push(Segment::Unknown(seg.clone()));
                    }
                }
            }
        }

        let mut cursor = 0;
        for segment in &mut segments {
            match segment {
                Segment::BlockRef(seg) => {
                    let new = cursor + seg.block_count() * block_size;
                    if base.len() <= new {
                        let diff = new - base.len();
                        // +7 to ceil the value
                        let remove_blocks = (diff + 7) / block_size;
                        seg.block_count -= remove_blocks;
                    }
                }
                Segment::Unknown(seg) => cursor += seg.data().len(),
                Segment::Ref(_) => unreachable_reference_segment(),
            }
        }

        Ok(Difference {
            segments,
            block_size,
        })
    }
    /// Apply `diff` to the `base` data base, appending the result to `out`.
    ///
    /// # Security
    ///
    /// The `diff` should be sanitized if input is suspected to be malicious.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError::RefOutOfBounds`] if a reference is out of bounds of the `base`.
    pub fn apply(&self, base: &[u8], out: &mut Vec<u8>) -> Result<(), ApplyError> {
        fn extend_vec_slice<T: Copy>(vec: &mut Vec<T>, slice: &[T]) {
            // SAFETY: This guarantees `vec.capacity()` >= `vec.len() + slice.len()`
            vec.reserve(slice.len());
            let len = vec.len();
            // SAFETY: We get uninitialized bytes and write to them. This is fine.
            // The length is guaranteed to be allocated from above.
            let destination = unsafe { vec.get_unchecked_mut(len..len + slice.len()) };
            destination.copy_from_slice(slice);
            // SAFETY: We set the length to that we've written to above.
            unsafe { vec.set_len(vec.len() + slice.len()) };
        }
        use ApplyError::RefOutOfBounds as Roob;

        let block_size = self.block_size();
        for segment in self.segments() {
            match segment {
                Segment::Ref(ref_segment) => {
                    let start = ref_segment.start;
                    let end = cmp::min(ref_segment.end(block_size), base.len());

                    let data = base.get(start..end).ok_or(Roob)?;
                    extend_vec_slice(out, data);
                }
                Segment::BlockRef(block_ref_segment) => {
                    let start = block_ref_segment.start;
                    let end = cmp::min(block_ref_segment.end(block_size), base.len());
                    // Check that only the last ref goes past the end.
                    debug_assert!(if end == base.len() {
                        block_ref_segment.end(block_size) - block_size < base.len()
                    } else {
                        true
                    });

                    let data = base.get(start..end).ok_or(Roob)?;
                    extend_vec_slice(out, data);
                }
                Segment::Unknown(unknown_segment) => {
                    let data = &unknown_segment.source;
                    extend_vec_slice(out, data);
                }
            }
        }

        Ok(())
    }
    /// Apply `diff` to the `base` data base, mutating the `base` data.
    /// You **MUST** check that this is possible using [`Self::apply_overlaps`].
    /// Neglecting that step will result in data loss and erroneous data.
    /// This WILL NOT give an error in case of breaking that contract.
    ///
    /// # Security
    ///
    /// The `diff` should be sanitized if input is suspected to be malicious.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError::RefOutOfBounds`] if a reference is out of bounds of the `base`.
    ///
    /// # Examples
    ///
    /// See [`Self::apply_overlaps`].
    pub fn apply_in_place(&self, base: &mut Vec<u8>) -> Result<(), ApplyError> {
        #[allow(clippy::uninit_vec)] // we know what we're doing
        fn copy_within_vec<T: Copy>(vec: &mut Vec<T>, range: Range<usize>, position: usize) {
            let range_len = range.len();
            let new_len = (position + range_len).max(vec.len());
            // SAFETY: This guarantees `vec.capacity()` >= `vec.len() + slice.len()`
            vec.reserve(new_len - vec.len());
            // SAFETY: We set the length to what we write below.
            unsafe { vec.set_len(new_len) };
            vec.copy_within(range, position);
        }
        #[allow(clippy::uninit_vec)] // we know what we're doing
        fn extend_vec_slice<T: Copy>(vec: &mut Vec<T>, slice: &[T], position: usize) {
            let new_len = (position + slice.len()).max(vec.len());
            // SAFETY: This guarantees `vec.capacity()` >= `vec.len() + slice.len()`
            vec.reserve(new_len - vec.len());
            // SAFETY: We set the length to what we write below.
            unsafe { vec.set_len(new_len) };
            let destination = &mut vec[position..(position + slice.len())];
            destination.copy_from_slice(slice);
        }
        use ApplyError::RefOutOfBounds as Roob;

        let block_size = self.block_size();
        let mut position = 0;
        for segment in self.segments() {
            match segment {
                Segment::Ref(ref_segment) => {
                    let start = ref_segment.start;
                    let end = cmp::min(ref_segment.end(block_size), base.len());

                    let range = start..end;
                    base.get(range.clone()).ok_or(Roob)?;
                    copy_within_vec(base, range, position);
                    position += end - start;
                }
                Segment::BlockRef(block_ref_segment) => {
                    let start = block_ref_segment.start;
                    let end = cmp::min(block_ref_segment.end(block_size), base.len());
                    // Check that only the last ref goes past the end.
                    debug_assert!(if end == base.len() {
                        block_ref_segment.end(block_size) - block_size < base.len()
                    } else {
                        true
                    });

                    let range = start..end;
                    base.get(range.clone()).ok_or(Roob)?;
                    copy_within_vec(base, range, position);
                    position += end - start;
                }
                Segment::Unknown(unknown_segment) => {
                    let data = &unknown_segment.source;
                    extend_vec_slice(base, data, position);
                    position += data.len();
                }
            }
        }
        // shorten the vec if the new length is less than old
        base.truncate(position);

        Ok(())
    }
}

/// An error during [`Difference::minify`].
#[derive(Debug, PartialEq, Eq)]
pub enum MinifyError {
    /// New block size is larger than previous.
    NewLarger,
    /// Old block size is not a multiple of the new.
    NotMultiple,
    /// Successive unknown segments are not allowed and **SHOULD** never occur.
    SuccessiveUnknowns,
}

/// An error during [`Difference::apply`].
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyError {
    /// The reference is out of bounds.
    ///
    /// The data might be malicious or corrupted or the `base` data has changed from constructing
    /// [`Signature`] and [`Difference::apply`].
    RefOutOfBounds,
}

#[derive(Debug, Clone)]
struct Blocks<'a, T> {
    slice: &'a [T],
    block_size: usize,
    pos: usize,
}
impl<'a, T> Blocks<'a, T> {
    fn new(slice: &'a [T], block_size: usize) -> Self {
        Self {
            slice,
            block_size,
            pos: 0,
        }
    }
    #[inline]
    fn advance(&mut self, n: usize) {
        self.pos += n;
    }
    /// Decrease the inner position. If `n == 1`, you revert the `.next()` call, resulting in
    /// repeated slices being returned.
    #[inline]
    fn go_back(&mut self, n: usize) {
        self.pos -= n;
    }
    /// Clamped to `slice.len()`.
    #[inline]
    fn pos(&self) -> usize {
        cmp::min(self.pos, self.slice.len())
    }
}
impl<'a, T> Iterator for Blocks<'a, T> {
    type Item = &'a [T];
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 1 > self.slice.len() {
            return None;
        }

        let start = self.pos;
        let end = cmp::min(start + self.block_size, self.slice.len());

        self.advance(1);

        Some(&self.slice[start..end])
    }
}
struct PrePostWindow<'a, T> {
    slice: &'a [T],
    pos: usize,
}
impl<'a, T> PrePostWindow<'a, T> {
    fn new(slice: &'a [T]) -> Self {
        Self { slice, pos: 0 }
    }
}
impl<'a, T> Iterator for PrePostWindow<'a, T> {
    type Item = (Option<&'a T>, &'a T, Option<&'a T>);
    fn next(&mut self) -> Option<Self::Item> {
        let current = self.slice.get(self.pos)?;
        let before = self.pos.checked_sub(1).and_then(|pos| self.slice.get(pos));
        let after = self.slice.get(self.pos + 1);
        self.pos += 1;
        Some((before, current, after))
    }
}
