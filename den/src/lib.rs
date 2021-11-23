//! A difference library similar to `rsync`.
//!
//! # Performance
//!
//! This library (for now) does not use a rolling hash algorithm, but use the theory.
//! This is performant enough for like files and files under 10MiB.
//! Please compile with the **release** preset for **10X** the performance.
//!
//! Allocating data and keeping it in memory is very fast compared to hashing.
//! The reason to move to more readers is for memory space.
//! Even then, the implementation could abstract the file system to give this library only 64KiB chunks.
//!
//! # How-to
//!
//! Keep in mind this isn't guaranteed to give the exact same data.
//! Please check the data with for example SHA-3 to ensure consistency.
//!
//! ## Get a remote's data
//!
//! To get someone else's data, we construct a [`Signature`] and send it.
//! The remote [calculates](diff) a [`Difference`] using the `Signature`.
//! The remote sends back the [`Difference`] which we [`apply`].
//!
//! ## Push my data to remote
//!
//! > This is what `rsync` does.
//!
//! Send to the remote the request of their [`Signature`].
//! They calculate it and send it back.
//! We calculate a [`diff`] and send it to them.
//! They [`apply`] it. Their data should now be equal to mine.
//!
//! # Future improvements
//!
//! - [ ] Rolling hash
//! - [ ] Multi-threaded [`diff`]
//! - [ ] Support read/write
//!     - [ ] Support to diff a reader
//!     - [ ] Support to apply to a writer
//!     - [ ] Fetch API for apply to get data on demand.
//!         - This could slow things down dramatically.
//!     - [ ] Implement Write for [`HashBuilder`].

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hasher;
use twox_hash::xxh3::HasherExt;

macro_rules! hashers {
    ($macro: tt!) => {
        $macro!(
            (None4, [u8; 4], StackSlice<4>, finish),
            (None8, [u8; 8], StackSlice<8>, finish),
            (None16, [u8; 16], StackSlice<16>, finish),
            (Fnv, [u8; 8], fnv::FnvHasher, finish),
            (XXH3_64, [u8; 8], twox_hash::Xxh3Hash64, finish),
            (XXH3_128, [u8; 16], twox_hash::Xxh3Hash128, finish_ext),
        );
    };
}

macro_rules! hash_algorithm {
    (
        $(
            (
                $name: ident,
                $result: ty,
                $builder: ty,
                $finish: ident
            ),
        )+
    ) => {
        /// The algorithms which can be used for hashing the data.
        #[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
        #[must_use]
        #[allow(missing_docs)]
        pub enum HashAlgorithm {
            $(
                $name,
            )+
        }
        impl HashAlgorithm {
            #[inline]
            fn builder(&self) -> HashBuilder {
                match self {
                    $(
                        Self::$name => HashBuilder::$name(Default::default()),
                    )+
                }
            }
        }
    };
}

hashers!(hash_algorithm!);

macro_rules! hash_builder {
    (
        $(
            (
                $name: ident,
                $result: ty,
                $builder: ty,
                $finish: ident
            ),
        )+
    ) => {
        enum HashBuilder {
            $(
                $name($builder),
            )+
        }

        impl HashBuilder {
            #[inline]
            fn finish(self) -> HashResult {
                match self {
                    $(
                        Self::$name(hasher) => HashResult::$name(hasher.$finish().to_le_bytes()),
                    )+
                }
            }
            #[inline]
            fn write(&mut self, data: &[u8]) {
                match self {
                    $(
                        Self::$name(hasher) => hasher.write(data),
                    )+
                }
            }
        }
    };
}

hashers!(hash_builder!);

impl Debug for HashBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HashBuilder (internal hasher data)")
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
    fn finish(self) -> u32 {
        u32::from_ne_bytes(self.data)
    }
}
impl StackSlice<8> {
    fn finish(self) -> u64 {
        u64::from_ne_bytes(self.data)
    }
}
impl StackSlice<16> {
    fn finish(self) -> u128 {
        u128::from_ne_bytes(self.data)
    }
}

macro_rules! hash_result {
    (
        $(
            (
                $name: ident,
                $result: ty,
                $builder: ty,
                $finish: ident
            ),
        )+
    ) => {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[repr(u8)]
        #[must_use]
        enum HashResult {
            $(
                $name($result),
            )+
        }
        impl HashResult {
            fn to_bytes(&self) -> [u8; 16] {
                match self {
                    $(
                        Self::$name(bytes) => to_16_bytes(bytes),
                    )+
                }
            }
        }
    };
}

hashers!(hash_result!);

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
/// Created using [`Signature::new`];
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
    fn new(algo: HashAlgorithm) -> Self {
        Self {
            algo,
            blocks: Vec::new(),
            block_size: 1024,

            current: algo.builder(),
            len: 0,
        }
    }
    fn block_available(&self) -> usize {
        self.block_size - self.len
    }
    fn finish_hash(&mut self) {
        let builder = std::mem::replace(&mut self.current, self.algo.builder());
        let result = builder.finish();
        self.blocks.push(result);
        self.len = 0;
    }
    /// Appends data to the hasher.
    ///
    /// This can be called multiple times to write the resource bit-by-bit.
    pub fn write(&mut self, data: &[u8]) {
        let mut data = data;

        while data.len() > self.block_available() {
            let bytes = &data[..self.block_available()];
            self.current.write(bytes);

            data = &data[self.block_available()..];
            self.finish_hash();
        }
    }
    /// Sets the block size of the hashes to be `block_size` bytes.
    ///
    /// The default is `1024`.
    fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[must_use]
pub struct Signature {
    algo: HashAlgorithm,
    blocks: Vec<HashResult>,
    block_size: usize,
}
impl Signature {
    /// Larger `block_size`s will take more time to compute, but will be more secure.
    /// Smaller `block_size`s takes less time to compute, are less secure, and require sending more
    /// data in the [`Signature`], as more blocks are sent.
    ///
    /// # Panics
    ///
    /// Will panic if [`HashAlgorithm`] is of type `None*` and `block_size` isn't the same number.
    pub fn with_algorithm(algo: HashAlgorithm, block_size: usize) -> SignatureBuilder {
        match algo {
            HashAlgorithm::None4 => assert_eq!(block_size, 4),
            HashAlgorithm::None8 => assert_eq!(block_size, 8),
            HashAlgorithm::None16 => assert_eq!(block_size, 16),
            _ => {}
        }
        SignatureBuilder::new(algo).with_block_size(block_size)
    }

    /// Get the algorithm used by this signature.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.algo
    }
    /// Returns the block size of this signature.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.block_size
    }
    pub(crate) fn blocks(&self) -> &[HashResult] {
        &self.blocks
    }
}

#[derive(Debug)]
struct BlockData {
    start: usize,
}
/// A segment with a reference to the base data.
///
/// Use [`SegmentBlockRef`] if several blocks in succession reference the same successive data in
/// the base data.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[must_use]
pub struct SegmentRef {
    /// Start of segment with a length of the [`Signature::block_size`].
    start: usize,
}
impl SegmentRef {
    #[inline]
    fn end(self, block_size: usize) -> usize {
        self.start + block_size
    }
}
/// Several [`SegmentRef`] after each other.
///
/// This is a separate struct to limit serialized size.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[must_use]
pub struct SegmentBlockRef {
    /// Start of segment with a length of [`Self::block_count`]*[`Signature::block_size`].
    start: usize,
    block_count: usize,
}
impl SegmentBlockRef {
    #[inline]
    fn extend(&mut self, n: usize) {
        self.block_count += n;
    }
    #[inline]
    fn end(self, block_size: usize) -> usize {
        self.start + self.block_count * block_size
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
#[derive(Debug, PartialEq, Eq)]
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
}
/// A segment of data corresponding to a multiple of [`Difference::block_size`].
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum Segment {
    /// A reference to a block of data.
    Ref(SegmentRef),
    /// Reference to successive blocks of data.
    BlockRef(SegmentBlockRef),
    /// Data unknown to the one who sent the [`Signature`].
    Unknown(SegmentUnknown),
}
impl Segment {
    #[inline]
    fn reference(data: &BlockData) -> Self {
        Self::Ref(SegmentRef { start: data.start })
    }
    #[inline]
    fn unknown(data: &[u8]) -> Self {
        Self::Unknown(SegmentUnknown {
            source: data.to_vec(),
        })
    }
}
/// A delta between the local data and the data the [`Signature`] represents.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub struct Difference {
    segments: Vec<Segment>,
    block_size: usize,
}
impl Difference {
    /// Returns a reference to all the internal [`Segment`]s.
    ///
    /// This can be used for implementing algorithms other than [`apply`] to apply the data.
    ///
    /// > `agde` uses this to convert from this format to their `Section` style.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }
    /// The block size used by this diff.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.block_size
    }
}

/// Get the [`Difference`] between the data the [`Signature`] represents and the local `data`.
///
/// This will return a struct which when serialized (using e.g. `bincode`) is much smaller than
/// `data`.
pub fn diff(data: &[u8], signature: &Signature) -> Difference {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn test_unknown_data(
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
    let mut map = BTreeMap::new();

    let block_size = signature.block_size();

    // Special case 2: Signature contains no hashes.
    // Just send the whole input.
    if signature.blocks().is_empty() {
        let data = Segment::unknown(data);
        return Difference {
            segments: vec![data],
            block_size,
        };
    }

    for (nr, block) in signature.blocks().iter().enumerate() {
        let bytes = block.to_bytes();

        let start = nr * block_size;
        let block_data = BlockData { start };

        map.insert(bytes, block_data);
    }

    let mut blocks = Blocks::new(data, signature.block_size());

    let mut segments = Vec::new();
    let mut last_ref = 0;

    // Iterate over data, in windows. Find hash.
    while let Some(block) = blocks.next() {
        // Special case 1: block size is larger than input.
        // Just send the whole input.
        if block_size > block.len() {
            segments.push(Segment::unknown(&data[last_ref..]));
            blocks.advance(block_size);
            last_ref = data.len();
            continue;
        }

        // let now = std::time::Instant::now();
        let mut hasher = signature.algorithm().builder();
        hasher.write(block);
        let hash = hasher.finish().to_bytes();

        // If hash matches, push previous data to unknown, and push a ref. Advance by `block_size`.
        if let Some(block_data) = map.get(&hash) {
            test_unknown_data(data, last_ref, blocks.pos(), &mut segments);

            if let Some(last) = segments.last_mut() {
                match last {
                    Segment::Ref(ref_segment) => {
                        if block_data.start == ref_segment.start + block_size {
                            let mut segment: SegmentBlockRef = (*ref_segment).into();
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
            last_ref = blocks.pos();
        }
    }

    // If we want them to get our diff, we send our diff, with the `Unknown`s filled with data.
    // Then, we only send references to their data and our new data.
    test_unknown_data(data, last_ref, blocks.pos() + 1, &mut segments);

    Difference {
        segments,
        block_size,
    }
}

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
    /// Clamped to `slice.len()`.
    #[inline]
    fn pos(&self) -> usize {
        std::cmp::min(self.pos, self.slice.len())
    }
}
impl<'a> Iterator for Blocks<'a, u8> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 1 > self.slice.len() {
            return None;
        }

        let start = self.pos;
        let end = std::cmp::min(start + self.block_size, self.slice.len());

        self.advance(1);

        Some(&self.slice[start..end])
    }
}

/// An error during [`apply`].
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyError {
    /// The reference is out of bounds.
    ///
    /// The data might be malicious or corrupted or the `base` data has changed from calling
    /// [`Signature::new`] and [`apply`].
    RefOutOfBounds,
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
pub fn apply(base: &[u8], diff: &Difference, out: &mut Vec<u8>) -> Result<(), ApplyError> {
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
    let block_size = diff.block_size();
    for segment in diff.segments() {
        match segment {
            Segment::Ref(ref_segment) => {
                let start = ref_segment.start;
                let end = ref_segment.end(block_size);

                let data = base.get(start..end).ok_or(Roob)?;
                extend_vec_slice(out, data);
            }
            Segment::BlockRef(block_ref_segment) => {
                let start = block_ref_segment.start;
                let end = block_ref_segment.end(block_size);

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

#[cfg(test)]
mod tests {
    use super::*;

    fn lorem_ipsum() -> &'static str {
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna ultrices gravida quis in felis. Mauris ac rutrum enim. Nulla auctor lacus at tellus sagittis dictum non id nunc. Donec ac nisl molestie, egestas dui vitae, consectetur sapien. Vivamus vel aliquet magna, ut malesuada mauris. Curabitur eu erat at lorem rhoncus cursus ac at mauris. Curabitur ullamcorper diam sed leo pellentesque, ac rhoncus quam mattis. Suspendisse potenti. Pellentesque risus ex, egestas in ex nec, sollicitudin accumsan dolor. Donec elementum id odio eget pharetra. Morbi aliquet accumsan vestibulum. Suspendisse eros dui, condimentum sagittis magna non, eleifend egestas dui. Ut pulvinar vestibulum lorem quis laoreet. Nam aliquam ante in placerat volutpat. Sed ac imperdiet ex. Nullam ut neque vel augue dignissim semper."
    }

    #[test]
    fn difference() {
        // This is the data we have
        let local_data =
            "Lorem ipsum dolor sit amet, don't really know Rust elit. Cras nec justo eu magna.";
        // This is the data we want to get.
        let remote_data =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna.";

        let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 8);
        signature.write(local_data.as_bytes());
        let signature = signature.finish();

        let now = std::time::Instant::now();
        let diff = diff(remote_data.as_bytes(), &signature);
        println!("Segments {:#?}", diff.segments());
        println!("Took {:?}", now.elapsed());
        assert_eq!(diff.segments().len(), 4);

        let segment = &diff.segments()[1];
        assert_eq!(
            segment,
            &Segment::unknown(b"et, consectetur adipiscing elit.".as_ref())
        );
    }
    #[test]
    fn block_size_larger_than_input() {
        // This is the data we have
        let local_data = lorem_ipsum().replace("Cras nec justo", "I don't know");
        // This is the data we want to get.
        let remote_data = lorem_ipsum();

        let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 4096);
        signature.write(local_data.as_bytes());
        let signature = signature.finish();

        let diff = diff(remote_data.as_bytes(), &signature);
        assert_eq!(diff.segments().len(), 1);
    }
    #[test]
    fn raw_bytes() {
        let local_data = lorem_ipsum().replace("Cras nec justo", "I don't know");
        // This is the data we want to get.
        let remote_data = lorem_ipsum();
        let mut signature = Signature::with_algorithm(HashAlgorithm::None16, 16);
        signature.write(local_data.as_bytes());
        let signature = signature.finish();

        drop(diff(remote_data.as_bytes(), &signature));
    }
    #[test]
    fn empty() {
        let local_data = "";
        // This is the data we want to get.
        let remote_data = "";
        let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 512);
        signature.write(local_data.as_bytes());
        let signature = signature.finish();

        let diff = diff(remote_data.as_bytes(), &signature);
        assert_eq!(diff.segments(), []);
    }
    #[test]
    fn sync() {
        let local_data =
            "Lorem ipsum dolor sit amet, don't really know Rust elit. Cras nec justo eu magna.";
        let remote_data =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras nec justo eu magna.";

        let mut signature = Signature::with_algorithm(HashAlgorithm::XXH3_64, 8);
        signature.write(local_data.as_bytes());
        let signature = signature.finish();

        let diff = diff(remote_data.as_bytes(), &signature);

        let mut out = Vec::new();
        apply(local_data.as_bytes(), &diff, &mut out).unwrap();
        assert_eq!(&out, remote_data.as_bytes());
    }
}
