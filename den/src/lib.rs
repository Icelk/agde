//! A difference library similar to `rsync`.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    // missing_docs
)]

use serde::{Deserialize, Serialize};
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
enum HashResult {
    None4([u8; 4]),
    None8([u8; 8]),
    None16([u8; 16]),
    Fnv([u8; 8]),
    XXH3_64([u8; 8]),
    XXH3_128([u8; 16]),
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
    pub fn with_algo(algo: HashAlgorithm, block_size: usize) -> SignatureBuilder {
        SignatureBuilder::new(algo).with_block_size(block_size)
    }
}
