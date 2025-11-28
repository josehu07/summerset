//! Bitmap data structure helper.

use std::collections::HashSet;
use std::fmt;
use std::hash::BuildHasher;
use std::ops::Range;

use bincode::{Decode, Encode};
use fixedbitset::FixedBitSet;
use get_size::GetSize;
use serde::{Deserialize, Serialize};

use crate::utils::SummersetError;

/// Compact bitmap for u8 ID -> bool mapping.
#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bitmap(FixedBitSet);

// implement `Encode` and `Decode` traits for `Bitmap`
impl Encode for Bitmap {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let len = self.0.len();
        // Encode logical bit length, then the backing word slice (no copy).
        len.encode(encoder)?;
        self.0.as_slice().encode(encoder)
    }
}

impl<Ctx> Decode<Ctx> for Bitmap {
    fn decode<D: bincode::de::Decoder<Context = Ctx>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let len: usize = usize::decode(decoder)?;
        // Decode backing words and build without extra copies.
        let words: Vec<usize> = Vec::<usize>::decode(decoder)?;
        Ok(Bitmap(FixedBitSet::with_capacity_and_blocks(len, words)))
    }
}

impl<'de, Ctx> bincode::BorrowDecode<'de, Ctx> for Bitmap {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Ctx>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Decode::decode(decoder)
    }
}

// implement `GetSize` trait for `Bitmap`; the heap size is approximated as
// #bits rounded up to multiple of 4 bytes
impl GetSize for Bitmap {
    fn get_heap_size(&self) -> usize {
        self.0.len().div_ceil(32)
    }
}

impl Bitmap {
    /// Creates a new bitmap of given size. If `ones` is true, all slots are
    /// marked true initially; otherwise, all slots are initially false.
    #[must_use]
    pub fn new(size: u8, ones: bool) -> Self {
        assert!(size != 0, "invalid bitmap size {}", size);
        let mut bitset = FixedBitSet::with_capacity(size as usize);

        if ones {
            bitset.set_range(.., true);
        }

        Bitmap(bitset)
    }

    /// Sets bit at index to given flag.
    #[inline]
    pub fn set(&mut self, idx: u8, flag: bool) -> Result<(), SummersetError> {
        if idx as usize >= self.0.len() {
            return Err(SummersetError::msg(format!(
                "index {} out of bound",
                idx
            )));
        }
        self.0.set(idx as usize, flag);
        Ok(())
    }

    /// Gets the bit flag at index.
    #[inline]
    pub fn get(&self, idx: u8) -> Result<bool, SummersetError> {
        if idx as usize >= self.0.len() {
            return Err(SummersetError::msg(format!(
                "index {} out of bound",
                idx
            )));
        }
        Ok(self.0[idx as usize])
    }

    /// Returns the size of the bitmap.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn size(&self) -> u8 {
        self.0.len() as u8
    }

    /// Returns the number of trues in the bitmap.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn count(&self) -> u8 {
        self.0.count_ones(..) as u8
    }

    /// Flips all flags in the bitmap.
    #[inline]
    pub fn flip(&mut self) {
        self.0.toggle_range(..);
    }

    /// Unions with another same-size bitmap.
    #[inline]
    pub fn union(&mut self, other: &Self) -> Result<(), SummersetError> {
        if self.size() == other.size() {
            self.0.union_with(&other.0);
            Ok(())
        } else {
            Err(SummersetError::msg(format!(
                "unioning sizes mismatch: {} != {}",
                self.size(),
                other.size()
            )))
        }
    }

    /// Clears all flags in the bitmap to 0.
    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Allows `for (id, bit) in map.iter()`.
    #[inline]
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> BitmapIter<'_> {
        BitmapIter { map: self, idx: 0 }
    }
}

// Convert <- (size, range of contiguous indexes where the flag is true).
impl From<(u8, Range<u8>)> for Bitmap {
    fn from(tup: (u8, Range<u8>)) -> Self {
        let (size, ones) = tup;
        let mut bitmap = Self::new(size, false);
        for idx in ones {
            bitmap.set(idx, true).unwrap();
        }
        bitmap
    }
}

// Convert <- (size, vec of indexes where the flag is true).
impl From<(u8, Vec<u8>)> for Bitmap {
    fn from(tup: (u8, Vec<u8>)) -> Self {
        let (size, ones) = tup;
        let mut bitmap = Self::new(size, false);
        for idx in ones {
            bitmap.set(idx, true).unwrap();
        }
        bitmap
    }
}

// Convert <- (size, vec of indexes where the flag is true).
impl From<(u8, &Vec<u8>)> for Bitmap {
    fn from(tup: (u8, &Vec<u8>)) -> Self {
        let (size, ones) = tup;
        let mut bitmap = Self::new(size, false);
        for &idx in ones {
            bitmap.set(idx, true).unwrap();
        }
        bitmap
    }
}

// Convert <- (size, set of indexes where the flag is true).
impl From<(u8, HashSet<u8>)> for Bitmap {
    fn from(tup: (u8, HashSet<u8>)) -> Self {
        let (size, ones) = tup;
        let mut bitmap = Self::new(size, false);
        for idx in ones {
            bitmap.set(idx, true).unwrap();
        }
        bitmap
    }
}

// Convert <- (size, set of indexes where the flag is true).
impl From<(u8, &HashSet<u8>)> for Bitmap {
    fn from(tup: (u8, &HashSet<u8>)) -> Self {
        let (size, ones) = tup;
        let mut bitmap = Self::new(size, false);
        for &idx in ones {
            bitmap.set(idx, true).unwrap();
        }
        bitmap
    }
}

// Convert -> vec of indexes where the flag is true.
impl From<Bitmap> for Vec<u8> {
    fn from(bitmap: Bitmap) -> Self {
        bitmap
            .iter()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
            .collect()
    }
}

// Convert -> vec of indexes where the flag is true.
impl From<&Bitmap> for Vec<u8> {
    fn from(bitmap: &Bitmap) -> Self {
        bitmap
            .iter()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
            .collect()
    }
}

// Convert -> set of indexes where the flag is true.
impl<S: BuildHasher + Default> From<Bitmap> for HashSet<u8, S> {
    fn from(bitmap: Bitmap) -> Self {
        bitmap
            .iter()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
            .collect()
    }
}

// Convert -> set of indexes where the flag is true.
impl<S: BuildHasher + Default> From<&Bitmap> for HashSet<u8, S> {
    fn from(bitmap: &Bitmap) -> Self {
        bitmap
            .iter()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
            .collect()
    }
}

/// Immutable iterator over `Bitmap`, yielding `(id, bit)` pairs.
#[derive(Debug, Clone)]
pub struct BitmapIter<'m> {
    map: &'m Bitmap,
    idx: usize,
}

impl Iterator for BitmapIter<'_> {
    type Item = (u8, bool);

    fn next(&mut self) -> Option<Self::Item> {
        #[allow(clippy::cast_possible_truncation)]
        let id: u8 = self.idx as u8;
        if id < self.map.size() {
            self.idx += 1;
            Some((id, self.map.get(id).unwrap()))
        } else {
            None
        }
    }
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        let mut first_idx = true;
        for i in self
            .iter()
            .filter_map(|(i, flag)| if flag { Some(i) } else { None })
        {
            if first_idx {
                write!(f, "{}", i)?;
                first_idx = false;
            } else {
                write!(f, ",{}", i)?;
            }
        }
        write!(f, "}}")
    }
}

// NOTE: keeping the more verbose formatting commented out for now...
// impl fmt::Display for Bitmap {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{{{}; [", self.size())?;
//         let mut first_idx = true;
//         for i in self
//             .iter()
//             .filter_map(|(i, flag)| if flag { Some(i) } else { None })
//         {
//             if !first_idx {
//                 write!(f, ", {}", i)?;
//             } else {
//                 write!(f, "{}", i)?;
//                 first_idx = false;
//             }
//         }
//         write!(f, "]}}")
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic = "bitmap initialization with empty size should panic"]
    fn new_invalid() {
        let _ = Bitmap::new(0, true);
    }

    #[test]
    fn conversions() {
        let ref_map = Bitmap::from((5, 1..4));
        assert_eq!(Bitmap::from((5, vec![1, 2, 3])), ref_map);
        assert_eq!(Bitmap::from((5, &vec![1, 2, 3])), ref_map);
        assert_eq!(Bitmap::from((5, HashSet::from([1, 2, 3]))), ref_map);
        assert_eq!(Bitmap::from((5, &HashSet::from([1, 2, 3]))), ref_map);
        assert_eq!(Vec::<u8>::from(ref_map.clone()), vec![1, 2, 3]);
        assert_eq!(Vec::<u8>::from(&ref_map), vec![1, 2, 3]);
        assert_eq!(
            HashSet::<u8>::from(ref_map.clone()),
            HashSet::from([1, 2, 3])
        );
        assert_eq!(HashSet::<u8>::from(&ref_map), HashSet::from([1, 2, 3]));
    }

    #[test]
    fn bitmap_set_get() {
        let mut map = Bitmap::new(7, false);
        assert!(map.set(0, true).is_ok());
        assert!(map.set(1, false).is_ok());
        assert!(map.set(2, true).is_ok());
        assert!(map.set(7, true).is_err());
        assert_eq!(map.get(0), Ok(true));
        assert_eq!(map.get(1), Ok(false));
        assert_eq!(map.get(2), Ok(true));
        assert_eq!(map.get(3), Ok(false));
        assert!(map.get(7).is_err());
    }

    #[test]
    fn bitmap_flip() {
        let mut map = Bitmap::new(5, false);
        assert!(map.set(1, true).is_ok());
        map.flip();
        assert_eq!(map, Bitmap::from((5, vec![0, 2, 3, 4])));
    }

    #[test]
    fn bitmap_union() {
        let mut map_a = Bitmap::from((5, vec![0, 1, 3]));
        let map_b = Bitmap::from((5, vec![0, 4]));
        assert!(map_a.union(&map_b).is_ok());
        assert_eq!(map_a, Bitmap::from((5, vec![0, 1, 3, 4])));
    }

    #[test]
    fn bitmap_count() {
        let mut map = Bitmap::new(7, false);
        assert_eq!(map.count(), 0);
        assert!(map.set(0, true).is_ok());
        assert!(map.set(2, true).is_ok());
        assert!(map.set(3, true).is_ok());
        assert_eq!(map.count(), 3);
    }

    #[test]
    fn bitmap_iter() {
        let ref_map = [true, true, false, true, true];
        let mut map = Bitmap::new(5, true);
        assert!(map.set(2, false).is_ok());
        for (id, flag) in map.iter() {
            assert_eq!(ref_map[id as usize], flag);
        }
        assert_eq!(Vec::<u8>::from(map), [0, 1, 3, 4]);
    }

    #[test]
    fn bincode_encode_decode() -> Result<(), SummersetError> {
        let mut map = Bitmap::from((10, vec![0, 2, 3, 9]));
        map.set(5, true)?;
        let bytes = bincode::encode_to_vec(&map, bincode::config::standard())?;
        let (decoded, read) = bincode::decode_from_slice::<Bitmap, _>(
            &bytes,
            bincode::config::standard(),
        )?;
        assert_eq!(read, bytes.len());
        assert_eq!(decoded, map);
        Ok(())
    }

    #[test]
    fn bincode_length_bits() -> Result<(), SummersetError> {
        let mut map = Bitmap::new(24, false);
        for i in [1u8, 5, 7, 12, 17, 23] {
            map.set(i, true)?;
        }
        let bytes = bincode::encode_to_vec(&map, bincode::config::standard())?;
        let (decoded, _) = bincode::decode_from_slice::<Bitmap, _>(
            &bytes,
            bincode::config::standard(),
        )?;
        assert_eq!(decoded.size(), 24);
        for i in 0..24 {
            assert_eq!(decoded.get(i)?, map.get(i)?);
        }
        Ok(())
    }
}
