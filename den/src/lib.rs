use serde::{Deserialize, Serialize};
use twox_hash::xxh3::HasherExt;
use std::hash::Hasher;

// macro_rules! hashers {
// () => {
// None4, None8, None16, Fnv, XXH3_64, XXH3_128,
// };
// }

pub enum HashBuilder {
    None4([u8; 4], u8),
    None8([u8; 8], u8),
    None16([u8; 16], u8),
    Fnv(fnv::FnvHasher),
    XXH3_64(twox_hash::Xxh3Hash64),
    XXH3_128(twox_hash::Xxh3Hash128),
}
impl HashBuilder {
    pub fn finish(self) -> HashAlgorithm {
        use HashBuilder::*;
        match self {
            None4(v, _) => HashAlgorithm::None4(v),
            None8(v, _) => HashAlgorithm::None8(v),
            None16(v, _) => HashAlgorithm::None16(v),
            Fnv(h) =>HashAlgorithm::Fnv(h.finish().to_le_bytes()),
            XXH3_64(h) =>HashAlgorithm::XXH3_64(h.finish().to_le_bytes()),
            XXH3_128(h) =>HashAlgorithm::XXH3_128(h.finish_ext().to_le_bytes()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum HashAlgorithm {
    None4([u8; 4]),
    None8([u8; 8]),
    None16([u8; 16]),
    Fnv([u8; 8]),
    XXH3_64([u8; 8]),
    XXH3_128([u8; 16]),
}
impl HashAlgorithm {
    fn same_algo(&self) -> HashBuilder {
        use HashAlgorithm::*;

        match self {
            None4(_) => HashBuilder::None4(Default::default(), 0),
            None8(_) => HashBuilder::None8(Default::default(), 0),
            None16(_) => HashBuilder::None16(Default::default(), 0),
            Fnv(_) => HashBuilder::Fnv(Default::default()),
            XXH3_64(_) => HashBuilder::XXH3_64(Default::default()),
            XXH3_128(_) => HashBuilder::XXH3_128(Default::default()),
        }
    }
}

pub const fn zeroed<const SIZE: usize>() -> [u8; SIZE] {
    [0; SIZE]
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    hasher: HashAlgorithm,
    hashes: Vec<HashAlgorithm>,
}
impl Signature {
    /// The `hasher` is used as the template hasher from which all other hashers are cloned.
    fn new(hasher: HashAlgorithm) -> Self {
        Self {
            hasher,
            hashes: Vec::new(),
        }
    }
    pub fn fnv() -> Self {
        Self::new(HashAlgorithm::Fnv(zeroed()))
    }
    pub fn xxh3_64() -> Self {
        Self::new(HashAlgorithm::XXH3_64(zeroed()))
    }
    pub fn xxh3_128() -> Self {
        Self::new(HashAlgorithm::XXH3_128(zeroed()))
    }
    pub fn hash(&self) -> HashBuilder {
        self.hasher.same_algo()
    }
    pub fn add(&mut self, hash: HashBuilder) {
        self.hashes.push(hash.finish())
    }
}
