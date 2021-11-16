//! A difference library similar to `rsync`.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    // missing_docs
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
        #[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
        #[must_use]
        pub enum HashAlgorithm {
            $(
                $name,
            )+
        }
        impl HashAlgorithm {
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
            fn finish(self) -> HashResult {
                match self {
                    $(
                        Self::$name(hasher) => HashResult::$name(hasher.$finish().to_le_bytes()),
                    )+
                }
            }
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
    fn write(&mut self, data: &[u8]) {
        assert!(data.len() <= self.available());
        self.data[self.len as usize..self.len as usize + data.len()].copy_from_slice(data);

        // The assert above guarantees we never reach this point if the input is too large.
        self.len += data.len() as u8;
    }
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
    pub fn with_algo(algo: HashAlgorithm, block_size: usize) -> SignatureBuilder {
        SignatureBuilder::new(algo).with_block_size(block_size)
    }

    pub fn algorithm(&self) -> HashAlgorithm {
        self.algo
    }
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
#[derive(Debug)]
#[must_use]
pub struct SegmentRef {
    hash: [u8; 16],
}
#[derive(Debug)]
#[must_use]
pub struct SegmentUnknown {
    len: usize,
}
#[derive(Debug)]
#[must_use]
pub enum Segment {
    Ref(SegmentRef),
    Unknown(SegmentUnknown),
}
#[derive(Debug)]
#[must_use]
pub struct Difference {
    segments: Vec<Segment>,
}

pub fn diff(data: &[u8], signature: &Signature) -> Difference {
    let mut map = BTreeMap::new();

    for (nr, block) in signature.blocks().iter().enumerate() {
        let bytes = block.to_bytes();

        let start = nr * signature.block_size();
        let block_data = BlockData { start };

        map.insert(bytes, block_data);
    }

    // Iterate over data, in windows. Find hash.
    //
    // If hash matches, push previous data to unknown, and push a ref. Advance by `block_size`.
    //
    // If you calculate the diff and want the other's data, we beg to send all their blocks which
    // we don't have.
    // If we want them to get our diff, we send our diff, with the `Unknown`s filled with data.
    // Then, we only send references to their data and our new data.

    Difference { segments: vec![] }
}
