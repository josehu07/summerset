//! Reed-Solomon erasure coding helpers.

use std::fmt;
use std::io;
use std::marker::PhantomData;

use crate::utils::{SummersetError, Bitmap};

use get_size::GetSize;

use bytes::{BytesMut, BufMut};

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::write as encode_write;
use rmp_serde::decode::from_read as decode_from_read;

use reed_solomon_erasure::galois_8::ReedSolomon;

/// A Reed-Solomon codeword with original data of type `T`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct RSCodeword<T> {
    /// Number of data shards.
    num_data_shards: u8,

    /// Number of parity shards.
    num_parity_shards: u8,

    /// Exact length of original data in bytes.
    data_len: usize,

    /// Length in bytes of a shard.
    shard_len: usize,

    /// Shards content. All `BytesMut` chunks are allocated contiguously by
    /// `.split_to()` to minimize possible `.unsplit()` overhead.
    shards: Vec<Option<BytesMut>>,

    /// Optional copy of original data to avoid doing cloned deserialization
    /// in some cases.
    data_copy: Option<T>,

    /// Zero-sized phantom marker to make this struct act as if it owns a data
    /// of type `T` (yet actually in the form of vec of bytes).
    phantom: PhantomData<T>,
}

// implement `GetSize` trait for `RSCodeword`; the heap size is approximated
// simply by the sum of sizes of present shards
impl<T> GetSize for RSCodeword<T>
where
    T: fmt::Debug + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn get_heap_size(&self) -> usize {
        self.shards
            .iter()
            .map(|s| if let Some(b) = s { b.len() } else { 0 })
            .sum()
    }
}

impl<T> RSCodeword<T>
where
    T: fmt::Debug + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    /// Internal method for creating a new RSCodeword from original data or
    /// empty bytes.
    fn internal_new(
        data_copy: Option<T>,
        data_bytes: Option<BytesMut>,
        data_len: usize,
        num_data_shards: u8,
        num_parity_shards: u8,
    ) -> Result<Self, SummersetError> {
        if num_data_shards == 0 {
            return Err(SummersetError("num_data_shards is zero".into()));
        }

        let num_total_shards = num_data_shards + num_parity_shards;
        let shard_len = if data_len % num_data_shards as usize == 0 {
            data_len / num_data_shards as usize
        } else {
            (data_len / num_data_shards as usize) + 1
        };

        let shards = if let Some(mut data_bytes) = data_bytes {
            // if newing from original data
            debug_assert_eq!(data_bytes.len(), data_len);

            // pad length to multiple of num_data_shards and compute shard size
            let padded_len = shard_len * num_data_shards as usize;
            data_bytes.resize(padded_len, 0);

            // split the bytes representation into contiguously stored shards
            let mut shards = Vec::with_capacity(num_data_shards as usize);
            for _ in 0..(num_data_shards - 1) {
                let shard = data_bytes.split_to(shard_len);
                debug_assert_eq!(shard.len(), shard_len);
                shards.push(Some(shard));
            }
            debug_assert_eq!(data_bytes.len(), shard_len);
            shards.push(Some(data_bytes)); // the last shard
            debug_assert_eq!(shards.len(), num_data_shards as usize);
            for _ in num_data_shards..num_total_shards {
                shards.push(None);
            }
            debug_assert_eq!(shards.len(), num_total_shards as usize);
            shards
        } else {
            // if newing from empty
            vec![None; num_total_shards as usize]
        };

        Ok(RSCodeword {
            num_data_shards,
            num_parity_shards,
            data_len,
            shard_len,
            shards,
            data_copy,
            phantom: PhantomData,
        })
    }

    /// Creates a new RSCodeword from original data.
    pub fn from_data(
        data: T,
        num_data_shards: u8,
        num_parity_shards: u8,
    ) -> Result<Self, SummersetError> {
        // serialize original data into bytes
        let mut data_writer = BytesMut::new().writer();
        encode_write(&mut data_writer, &data)?;
        let data_len = data_writer.get_ref().len();
        Self::internal_new(
            Some(data),
            Some(data_writer.into_inner()),
            data_len,
            num_data_shards,
            num_parity_shards,
        )
    }

    /// Creates a new RSCodeword from empty bytes.
    pub fn from_null(
        num_data_shards: u8,
        num_parity_shards: u8,
    ) -> Result<Self, SummersetError> {
        Self::internal_new(None, None, 0, num_data_shards, num_parity_shards)
    }

    /// Creates an `RSCodeword` struct that owns a copy of a subset of the
    /// shards, and a complete copy of the original data if required.
    pub fn subset_copy(
        &self,
        subset: &Bitmap,
        copy_data: bool,
    ) -> Result<Self, SummersetError> {
        if self.data_len == 0 {
            return Err(SummersetError("codeword is null".into()));
        }

        let mut shards = vec![None; self.num_shards() as usize];
        for i in
            subset.iter().filter_map(
                |(i, flag)| {
                    if flag {
                        Some(i as usize)
                    } else {
                        None
                    }
                },
            )
        {
            if i >= shards.len() {
                return Err(SummersetError(format!(
                    "shard index {} out-of-bound",
                    i
                )));
            }
            shards[i].clone_from(&self.shards[i]);
        }

        let data_copy = if copy_data {
            self.data_copy.clone() // could be `None` if originally None
        } else {
            None
        };

        Ok(RSCodeword {
            num_data_shards: self.num_data_shards,
            num_parity_shards: self.num_parity_shards,
            data_len: self.data_len,
            shard_len: self.shard_len,
            shards,
            data_copy,
            phantom: PhantomData,
        })
    }

    /// Absorbs another `RSCodeword` struct, taking its available shards.
    pub fn absorb_other(
        &mut self,
        mut other: RSCodeword<T>,
    ) -> Result<(), SummersetError> {
        // must have configuration parameters matching
        if self.num_data_shards != other.num_data_shards() {
            return Err(SummersetError(format!(
                "num_data_shards mismatch: expected {}, other {}",
                self.num_data_shards,
                other.num_data_shards()
            )));
        }
        if self.num_parity_shards != other.num_parity_shards() {
            return Err(SummersetError(format!(
                "num_parity_shards mismatch: expected {}, other {}",
                self.num_parity_shards,
                other.num_parity_shards()
            )));
        }
        if self.data_len != 0 && self.data_len != other.data_len() {
            return Err(SummersetError(format!(
                "data_len mismatch: expected {}, other {}",
                self.data_len,
                other.data_len()
            )));
        }
        if self.shard_len != 0 && self.shard_len != other.shard_len() {
            return Err(SummersetError(format!(
                "shard_len mismatch: expected {}, other {}",
                self.shard_len,
                other.shard_len()
            )));
        }

        // if I am null at this time, set data_len and shard_len to be the
        // same as input
        if self.data_len == 0 {
            self.data_len = other.data_len;
            self.shard_len = other.shard_len;
        }

        for i in 0..other.shards.len() {
            if let Some(shard) = other.shards[i].take() {
                if self.shards[i].is_none() {
                    self.shards[i] = Some(shard);
                }
            }
        }
        Ok(())
    }

    /// Gets number of data shards.
    #[inline]
    pub fn num_data_shards(&self) -> u8 {
        self.num_data_shards
    }

    /// Gets number of parity shards.
    #[allow(dead_code)]
    #[inline]
    pub fn num_parity_shards(&self) -> u8 {
        self.num_parity_shards
    }

    /// Gets total number of shards.
    #[inline]
    pub fn num_shards(&self) -> u8 {
        self.shards.len() as u8
    }

    /// Gets number of currently available data shards.
    #[inline]
    pub fn avail_data_shards(&self) -> u8 {
        self.shards
            .iter()
            .take(self.num_data_shards as usize)
            .filter(|s| s.is_some())
            .count() as u8
    }

    /// Gets number of currently available parity shards.
    #[allow(dead_code)]
    #[inline]
    pub fn avail_parity_shards(&self) -> u8 {
        self.shards
            .iter()
            .skip(self.num_data_shards as usize)
            .filter(|s| s.is_some())
            .count() as u8
    }

    /// Gets total number of currently available shards.
    #[inline]
    pub fn avail_shards(&self) -> u8 {
        self.shards.iter().filter(|s| s.is_some()).count() as u8
    }

    /// Gets a bitmap of available shard indexes set true.
    #[inline]
    pub fn avail_shards_map(&self) -> Bitmap {
        let mut map = Bitmap::new(self.num_shards(), false);
        for (i, s) in self.shards.iter().enumerate() {
            if s.is_some() {
                map.set(i as u8, true).unwrap();
            }
        }
        map
    }

    /// Gets length of original data in bytes.
    #[inline]
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    /// Gets length of a shard in bytes.
    #[inline]
    pub fn shard_len(&self) -> usize {
        self.shard_len
    }

    /// Helper checker to ensure that the given ReedSolomon coder has the same
    /// shard splits config as me.
    fn shard_splits_match(
        &self,
        rs: &ReedSolomon,
    ) -> Result<(), SummersetError> {
        if rs.data_shard_count() != self.num_data_shards as usize {
            Err(SummersetError(format!(
                "num_data_shards mismatch: expected {}, rs {}",
                self.num_data_shards,
                rs.data_shard_count()
            )))
        } else if rs.parity_shard_count() != self.num_parity_shards as usize {
            Err(SummersetError(format!(
                "num_parity_shards mismatch: expected {}, rs {}",
                self.num_parity_shards,
                rs.parity_shard_count()
            )))
        } else {
            Ok(())
        }
    }

    /// Computes the parity shards from data shards. Must have all data shards
    /// present.
    pub fn compute_parity(
        &mut self,
        rs: Option<&ReedSolomon>,
    ) -> Result<(), SummersetError> {
        if self.data_len == 0 {
            return Err(SummersetError("codeword is null".into()));
        }
        if self.num_parity_shards == 0 {
            return Ok(());
        }
        if let Some(rs) = rs {
            self.shard_splits_match(rs)?;
        } else {
            return Err(SummersetError("ReedSolomon coder is None".into()));
        }

        if self.avail_data_shards() < self.num_data_shards {
            return Err(SummersetError(format!(
                "not all data shards present: {} / {}",
                self.avail_data_shards(),
                self.num_data_shards
            )));
        }

        // allocate space for parity shards if haven't
        for shard in self.shards.iter_mut().skip(self.num_data_shards as usize)
        {
            if shard.is_none() {
                *shard = Some(BytesMut::zeroed(self.shard_len));
            }
        }

        let slices: Vec<&mut BytesMut> = self
            .shards
            .iter_mut()
            .map(|s| s.as_mut().unwrap())
            .collect();
        rs.unwrap().encode(slices)?;
        Ok(())
    }

    /// Internal method for reconstructing all shards or data shards from
    /// currently available shards.
    fn reconstruct(
        &mut self,
        rs: Option<&ReedSolomon>,
        data_only: bool,
    ) -> Result<(), SummersetError> {
        if self.data_len == 0 {
            return Err(SummersetError("codeword is null".into()));
        }
        if self.num_parity_shards == 0 {
            if self.avail_data_shards() == self.num_data_shards {
                return Ok(());
            }
            return Err(SummersetError(format!(
                "insufficient data shards: {}/ {}",
                self.avail_data_shards(),
                self.num_data_shards
            )));
        }
        if let Some(rs) = rs {
            self.shard_splits_match(rs)?;
        } else {
            return Err(SummersetError("ReedSolomon coder is None".into()));
        }

        if data_only {
            rs.unwrap().reconstruct_data(&mut self.shards)?;
        } else {
            rs.unwrap().reconstruct(&mut self.shards)?;
        }
        Ok(())
    }

    /// Reconstructs all shards from currently available shards.
    #[allow(dead_code)]
    pub fn reconstruct_all(
        &mut self,
        rs: Option<&ReedSolomon>,
    ) -> Result<(), SummersetError> {
        self.reconstruct(rs, false)
    }

    /// Reconstructs data shards from currently available shards.
    pub fn reconstruct_data(
        &mut self,
        rs: Option<&ReedSolomon>,
    ) -> Result<(), SummersetError> {
        self.reconstruct(rs, true)
    }

    /// Verifies if the currently parity shards are correct. Must have all data
    /// & parity shards available.
    #[allow(dead_code)]
    pub fn verify_parity(
        &mut self,
        rs: Option<&ReedSolomon>,
    ) -> Result<bool, SummersetError> {
        if self.data_len == 0 {
            return Err(SummersetError("codeword is null".into()));
        }
        if self.num_parity_shards == 0 {
            return Ok(self.avail_data_shards() == self.num_data_shards);
        }
        if let Some(rs) = rs {
            self.shard_splits_match(rs)?;
        } else {
            return Err(SummersetError("ReedSolomon is None".into()));
        }

        if self.avail_shards() < self.num_shards() {
            return Err(SummersetError(format!(
                "not all shards present: {} / {}",
                self.avail_shards(),
                self.num_shards()
            )));
        }

        let slices: Vec<&BytesMut> =
            self.shards.iter().map(|s| s.as_ref().unwrap()).collect();
        Ok(rs.unwrap().verify(&slices)?)
    }

    /// Get a reference to original data, requiring that all data shards are
    /// present. If data_copy is available, a reference to it is returned;
    /// otherwise, a cloned deserialization is performed to produce data_copy.
    pub fn get_data(&mut self) -> Result<&T, SummersetError> {
        if self.data_len == 0 {
            return Err(SummersetError("codeword is null".into()));
        }
        if self.avail_data_shards() < self.num_data_shards {
            return Err(SummersetError(format!(
                "not all data shards present: {} / {}",
                self.avail_data_shards(),
                self.num_data_shards
            )));
        }

        if self.data_copy.is_none() {
            let reader = ShardsReader::new(
                &self.shards,
                self.num_data_shards,
                self.shard_len,
            )?;
            self.data_copy = Some(decode_from_read(reader)?);
        }

        Ok(self.data_copy.as_ref().unwrap())
    }
}

/// Helper type containing an immutable reference to a vector of `BytesMut`
/// and a cursor; implements the `io::Read` trait to ease decoding from the
/// vector without the need of merging them into a single `BytesMut`.
struct ShardsReader<'a> {
    /// Immutable reference to the split bytes.
    shards: &'a Vec<Option<BytesMut>>,

    /// Number of data shards in vec.
    num_data_shards: u8,

    /// Length in bytes of a shard.
    shard_len: usize,

    /// Composite cursor: (shard_idx, byte_idx).
    cursor: (u8, usize),
}

impl<'a> ShardsReader<'a> {
    /// Creates a new temporary reader.
    fn new(
        shards: &'a Vec<Option<BytesMut>>,
        num_data_shards: u8,
        shard_len: usize,
    ) -> Result<Self, SummersetError> {
        for shard in shards.iter().take(num_data_shards as usize) {
            if shard.is_none() {
                return Err(SummersetError("some data shard is None".into()));
            }
            debug_assert_eq!(shard.as_ref().unwrap().len(), shard_len);
        }

        Ok(ShardsReader {
            shards,
            num_data_shards,
            shard_len,
            cursor: (0, 0),
        })
    }
}

impl<'a> io::Read for ShardsReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut total_nread = 0;

        while self.cursor.0 < self.num_data_shards {
            let mut slice = &(self.shards[self.cursor.0 as usize]
                .as_ref()
                .unwrap())[self.cursor.1..];
            let (_, buf_tail) = buf.split_at_mut(total_nread);

            let shard_nread = slice.read(buf_tail).unwrap();
            if shard_nread == 0 {
                // perhaps destination buf has been filled, early return
                return Ok(total_nread);
            }

            total_nread += shard_nread;
            self.cursor.1 += shard_nread;

            if self.cursor.1 > self.shard_len {
                panic!(
                    "impossible shard cursor: {} / {}",
                    self.cursor.1, self.shard_len
                );
            }
            if self.cursor.1 == self.shard_len {
                // progress shard index
                self.cursor.0 += 1;
                self.cursor.1 = 0;
            }
        }

        Ok(total_nread)
    }
}

#[cfg(test)]
mod rscoding_tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestData(String);

    #[test]
    fn new_from_data() -> Result<(), SummersetError> {
        let data = TestData("interesting_value".into());
        let mut data_writer = BytesMut::new().writer();
        encode_write(&mut data_writer, &data)?;
        let data_len = data_writer.get_ref().len();
        let shard_len = if data_len % 3 == 0 {
            data_len / 3
        } else {
            (data_len / 3) + 1
        };
        // invalid num_data_shards
        assert!(RSCodeword::from_data(data.clone(), 0, 0).is_err());
        // valid with num_parity_shards == 0
        let cw = RSCodeword::from_data(data.clone(), 3, 0)?;
        assert_eq!(cw.num_data_shards(), 3);
        assert_eq!(cw.num_parity_shards(), 0);
        assert_eq!(cw.num_shards(), 3);
        assert_eq!(cw.avail_data_shards(), 3);
        assert_eq!(cw.avail_parity_shards(), 0);
        assert_eq!(cw.avail_shards(), 3);
        assert_eq!(cw.avail_shards_map(), Bitmap::from(3, vec![0, 1, 2]));
        assert_eq!(cw.data_len(), data_len);
        assert_eq!(cw.shard_len(), shard_len);
        // valid with num_parity_shards > 0
        let cw = RSCodeword::from_data(data.clone(), 3, 2)?;
        assert_eq!(cw.num_data_shards(), 3);
        assert_eq!(cw.num_parity_shards(), 2);
        assert_eq!(cw.num_shards(), 5);
        assert_eq!(cw.avail_data_shards(), 3);
        assert_eq!(cw.avail_parity_shards(), 0);
        assert_eq!(cw.avail_shards(), 3);
        assert_eq!(cw.avail_shards_map(), Bitmap::from(5, vec![0, 1, 2]));
        assert_eq!(cw.data_len(), data_len);
        assert_eq!(cw.shard_len(), shard_len);
        Ok(())
    }

    #[test]
    fn new_from_null() -> Result<(), SummersetError> {
        // invalid num_data_shards
        assert!(RSCodeword::<TestData>::from_null(0, 0).is_err());
        // valid
        let cw = RSCodeword::<TestData>::from_null(3, 2)?;
        assert_eq!(cw.num_data_shards(), 3);
        assert_eq!(cw.num_parity_shards(), 2);
        assert_eq!(cw.num_shards(), 5);
        assert_eq!(cw.avail_data_shards(), 0);
        assert_eq!(cw.avail_parity_shards(), 0);
        assert_eq!(cw.avail_shards(), 0);
        assert_eq!(cw.avail_shards_map(), Bitmap::new(5, false));
        assert_eq!(cw.data_len(), 0);
        assert_eq!(cw.shard_len(), 0);
        Ok(())
    }

    #[test]
    fn subset_absorb() -> Result<(), SummersetError> {
        let data = TestData("interesting_value".into());
        let cwa = RSCodeword::from_data(data.clone(), 3, 2)?;
        // invalid subset
        assert!(cwa
            .subset_copy(&Bitmap::from(6, vec![0, 5]), false)
            .is_err());
        // valid subsets
        let cw01 = cwa.subset_copy(&Bitmap::from(5, vec![0, 1]), false)?;
        assert_eq!(cw01.avail_data_shards(), 2);
        let cw02 = cwa.subset_copy(&Bitmap::from(5, vec![0, 2]), true)?;
        assert_eq!(cw02.avail_data_shards(), 2);
        assert!(cw02.data_copy.is_some());
        // valid absorbing
        let mut cwb = RSCodeword::<TestData>::from_null(3, 2)?;
        cwb.absorb_other(cw02)?;
        assert_eq!(cwb.avail_shards(), 2);
        assert_eq!(cwb.avail_shards_map(), Bitmap::from(5, vec![0, 2]));
        cwb.absorb_other(cw01)?;
        assert_eq!(cwb.avail_shards(), 3);
        assert_eq!(cwb.avail_shards_map(), Bitmap::from(5, vec![0, 1, 2]));
        assert_eq!(*cwb.get_data()?, data);
        // invalid absorbing
        assert!(cwb
            .absorb_other(RSCodeword::from_data(data, 5, 3)?)
            .is_err());
        Ok(())
    }

    #[test]
    fn compute_verify() -> Result<(), SummersetError> {
        let rs32 = ReedSolomon::new(3, 2)?;
        let data = TestData("interesting_value".into());
        // not enough shards
        let mut cw_null = RSCodeword::<TestData>::from_null(3, 2)?;
        assert!(cw_null.compute_parity(Some(&rs32)).is_err());
        assert!(cw_null.verify_parity(Some(&rs32)).is_err());
        let mut cw_part =
            RSCodeword::<TestData>::from_data(data.clone(), 3, 2)?;
        cw_part.shards[1] = None;
        assert!(cw_part.compute_parity(Some(&rs32)).is_err());
        assert!(cw_part.verify_parity(Some(&rs32)).is_err());
        // valid with num_parity_shards == 0
        let mut cw = RSCodeword::from_data(data.clone(), 3, 0)?;
        cw.compute_parity(None)?;
        assert_eq!(cw.avail_parity_shards(), 0);
        assert!(cw.verify_parity(None)?);
        // valid with num_parity_shards > 0
        let mut cw = RSCodeword::from_data(data.clone(), 3, 2)?;
        cw.compute_parity(Some(&rs32))?;
        assert_eq!(cw.avail_parity_shards(), 2);
        assert!(cw.verify_parity(Some(&rs32))?);
        // shard splits mismatch
        let rs53 = ReedSolomon::new(5, 3)?;
        assert!(cw.compute_parity(None).is_err());
        assert!(cw.compute_parity(Some(&rs53)).is_err());
        assert!(cw.verify_parity(None).is_err());
        assert!(cw.verify_parity(Some(&rs53)).is_err());
        Ok(())
    }

    #[test]
    fn reconstruction() -> Result<(), SummersetError> {
        let rs32 = ReedSolomon::new(3, 2)?;
        let data = TestData("interesting_value".into());
        // not enough shards
        let mut cw_null = RSCodeword::<TestData>::from_null(3, 2)?;
        assert!(cw_null.reconstruct_all(Some(&rs32)).is_err());
        assert!(cw_null.reconstruct_data(Some(&rs32)).is_err());
        let mut cw_part =
            RSCodeword::<TestData>::from_data(data.clone(), 3, 2)?;
        cw_part.shards[1] = None;
        assert!(cw_part.reconstruct_all(Some(&rs32)).is_err());
        assert!(cw_part.reconstruct_data(Some(&rs32)).is_err());
        // valid with num_parity_shards == 0
        let mut cw = RSCodeword::from_data(data.clone(), 3, 0)?;
        cw.reconstruct_all(None)?;
        assert_eq!(cw.avail_shards(), 3);
        cw.shards[1] = None;
        assert!(cw.reconstruct_all(None).is_err());
        assert!(cw.reconstruct_data(None).is_err());
        // valid with num_parity_shards > 0
        let mut cw = RSCodeword::from_data(data.clone(), 3, 2)?;
        cw.reconstruct_all(Some(&rs32))?;
        assert_eq!(cw.avail_shards(), 5);
        cw.shards[1] = None;
        cw.shards[3] = None;
        cw.reconstruct_all(Some(&rs32))?;
        assert_eq!(cw.avail_shards(), 5);
        cw.shards[0] = None;
        cw.shards[2] = None;
        cw.reconstruct_data(Some(&rs32))?;
        assert_eq!(cw.avail_data_shards(), 3);
        cw.shards[0] = None;
        cw.shards[1] = None;
        cw.shards[4] = None;
        assert!(cw.reconstruct_all(Some(&rs32)).is_err());
        assert!(cw.reconstruct_data(Some(&rs32)).is_err());
        // shard splits mismatch
        let rs53 = ReedSolomon::new(5, 3)?;
        assert!(cw.reconstruct_all(None).is_err());
        assert!(cw.reconstruct_all(Some(&rs53)).is_err());
        Ok(())
    }

    #[test]
    fn get_data() -> Result<(), SummersetError> {
        let rs32 = ReedSolomon::new(3, 2)?;
        let data = TestData("interesting_value".into());
        let mut cw = RSCodeword::from_data(data.clone(), 3, 2)?;
        assert_eq!(*cw.get_data()?, data);
        cw.compute_parity(Some(&rs32))?;
        cw.shards[0] = None;
        assert!(cw.get_data().is_err());
        cw.reconstruct_data(Some(&rs32))?;
        assert_eq!(*cw.get_data()?, data);
        Ok(())
    }
}
