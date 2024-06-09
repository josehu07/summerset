//! Bitmap data structure helper.

use std::fmt;

use crate::utils::SummersetError;

use fixedbitset::FixedBitSet;

use get_size::GetSize;

use serde::{Serialize, Deserialize};

/// Compact bitmap for u8 ID -> bool mapping.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bitmap(FixedBitSet);

// implement `GetSize` trait for `Bitmap`; the heap size is approximated as
// #bits rounded up to multiple of 4 bytes
impl GetSize for Bitmap {
    fn get_heap_size(&self) -> usize {
        (self.0.len() + 32 - 1) / 32
    }
}

impl Bitmap {
    /// Creates a new bitmap of given size. If `ones` is true, all slots are
    /// marked true initially; otherwise, all slots are initially false.
    pub fn new(size: u8, ones: bool) -> Self {
        if size == 0 {
            panic!("invalid bitmap size {}", size);
        }
        let mut bitset = FixedBitSet::with_capacity(size as usize);

        if ones {
            bitset.set_range(.., true);
        }

        Bitmap(bitset)
    }

    /// Creates a new bitmap of given size from vec literal. Indices in the
    /// vec are bits to be set as true.
    pub fn from(size: u8, ones: Vec<u8>) -> Self {
        let mut bitmap = Self::new(size, false);

        for idx in ones {
            if let Err(e) = bitmap.set(idx, true) {
                panic!("{}", e);
            }
        }

        bitmap
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
    pub fn size(&self) -> u8 {
        self.0.len() as u8
    }

    /// Returns the number of trues in the bitmap.
    #[inline]
    pub fn count(&self) -> u8 {
        self.0.count_ones(..) as u8
    }

    /// Flips all flags in the bitmap.
    #[inline]
    pub fn flip(&mut self) {
        self.0.toggle_range(..)
    }

    /// Allows `for (id, bit) in map.iter()`.
    #[inline]
    pub fn iter(&self) -> BitmapIter {
        BitmapIter { map: self, idx: 0 }
    }

    /// Convenience method for converting the bitmap to a vec of indexes where
    /// the flag is true.
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        self.iter()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
            .collect()
    }
}

/// Iterator over `Bitmap`, yielding `(id, bit)` pairs.
#[derive(Debug, Clone)]
pub struct BitmapIter<'m> {
    map: &'m Bitmap,
    idx: usize,
}

impl Iterator for BitmapIter<'_> {
    type Item = (u8, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let id: u8 = self.idx as u8;
        if id < self.map.size() {
            self.idx += 1;
            Some((id, self.map.get(id).unwrap()))
        } else {
            None
        }
    }
}

// Implement `Debug` trait manually for better trace printing.
impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{{}; [", self.size())?;
        let mut first_idx = true;
        for i in self
            .iter()
            .filter_map(|(i, flag)| if flag { Some(i) } else { None })
        {
            if !first_idx {
                write!(f, ", {}", i)?;
            } else {
                write!(f, "{}", i)?;
                first_idx = false;
            }
        }
        write!(f, "]}}")
    }
}

// Implement alternative compact printing.
impl Bitmap {
    #[inline]
    pub fn compact_str(&self) -> String {
        self.to_vec()
            .into_iter()
            .map(|i| i.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

#[cfg(test)]
mod bitmap_tests {
    use super::*;

    #[test]
    #[should_panic]
    fn bitmap_new_panic() {
        Bitmap::new(0, true);
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
        assert_eq!(map, Bitmap::from(5, vec![0, 2, 3, 4]));
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
        assert_eq!(map.to_vec(), [0, 1, 3, 4]);
    }
}
