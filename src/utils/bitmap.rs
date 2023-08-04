//! Bitmap data structure helper.

use crate::utils::SummersetError;
use crate::server::ReplicaId;

use fixedbitset::FixedBitSet;

/// Compact bitmap for replica ID -> bool mapping.
#[derive(Debug, Clone)]
pub struct ReplicaMap(FixedBitSet);

impl ReplicaMap {
    /// Creates a new bitmap of given size. If `ones` is true, all slots are
    /// marked true initially; otherwise, all slots are initially false.
    pub fn new(size: u8, ones: bool) -> Result<Self, SummersetError> {
        if size == 0 {
            return Err(SummersetError(format!(
                "invalid bitmap size {}",
                size
            )));
        }
        let mut bitset = FixedBitSet::with_capacity(size as usize);
        if ones {
            bitset.set_range(.., true);
        }
        Ok(ReplicaMap(bitset))
    }

    /// Sets bit at index to given flag.
    pub fn set(
        &mut self,
        idx: ReplicaId,
        flag: bool,
    ) -> Result<(), SummersetError> {
        if idx as usize >= self.0.len() {
            return Err(SummersetError(format!("index {} out of bound", idx)));
        }
        self.0.set(idx as usize, flag);
        Ok(())
    }

    /// Gets the bit flag at index.
    pub fn get(&self, idx: ReplicaId) -> Result<bool, SummersetError> {
        if idx as usize >= self.0.len() {
            return Err(SummersetError(format!("index {} out of bound", idx)));
        }
        Ok(self.0[idx as usize])
    }

    /// Returns the size of the bitmap.
    pub fn size(&self) -> u8 {
        self.0.len() as u8
    }

    /// Returns the number of trues in the bitmap.
    pub fn count(&self) -> u8 {
        self.0.count_ones(..) as u8
    }

    /// Allows `for (id, bit) in map.iter()`.
    pub fn iter(&self) -> ReplicaMapIter {
        ReplicaMapIter { map: self, idx: 0 }
    }
}

/// Iterator over `ReplicaMap`, yielding `(id, bit)` pairs.
#[derive(Debug, Clone)]
pub struct ReplicaMapIter<'m> {
    map: &'m ReplicaMap,
    idx: usize,
}

impl Iterator for ReplicaMapIter<'_> {
    type Item = (ReplicaId, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let id: ReplicaId = self.idx as ReplicaId;
        if id < self.map.size() {
            self.idx += 1;
            Some((id, self.map.get(id).unwrap()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod bitmap_tests {
    use super::*;

    #[test]
    fn bitmap_new() {
        assert!(ReplicaMap::new(0, true).is_err());
        assert!(ReplicaMap::new(3, true).is_ok());
        assert!(ReplicaMap::new(5, false).is_ok());
    }

    #[test]
    fn bitmap_set_get() {
        let mut map = ReplicaMap::new(7, false).unwrap();
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
    fn bitmap_count() {
        let mut map = ReplicaMap::new(7, false).unwrap();
        assert_eq!(map.count(), 0);
        assert!(map.set(0, true).is_ok());
        assert!(map.set(2, true).is_ok());
        assert!(map.set(3, true).is_ok());
        assert_eq!(map.count(), 3);
    }

    #[test]
    fn bitmap_iter() {
        let ref_map = vec![true, true, false, true, true];
        let mut map = ReplicaMap::new(5, true).unwrap();
        assert!(map.set(2, false).is_ok());
        for (id, flag) in map.iter() {
            assert_eq!(ref_map[id as usize], flag);
        }
    }
}
