//! Supporters for special integer keys and key range maps.
//!
//! NOTE: currently only keys in format `k<number>` can be range-partitioned.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::mem::size_of;

use bincode::{Decode, Encode};
use get_size::GetSize;
use rangemap::RangeInclusiveMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError};

/// Responders configuration identifier number type.
pub type ConfNum = u64;

/// Responders configuration struct. (for relevant protocols only)
/// The associated index type is optional and, if used, could for example be
/// lease group ID.
#[derive(
    PartialEq, Eq, Default, Clone, Serialize, Deserialize, Encode, Decode,
)]
pub struct RespondersConf<Idx: Clone + Eq + Hash> {
    /// If not `None`, supposed leader.
    pub leader: Option<ReplicaId>,

    /// Map from integer key -> bitmap of responder nodes and an optional
    /// associated (unique) index.
    pub responders: KeyRangeMap<(Bitmap, Option<Idx>)>,

    /// Map from optional index -> bitmap of responder nodes. All inserted
    /// ranges that have an index will also be locatable from this map.
    // NOTE: this API is currently not settled down yet
    custom_map: HashMap<Idx, Bitmap>,
}

impl<Idx> RespondersConf<Idx>
where
    Idx: fmt::Debug
        + Eq
        + Hash
        + Default
        + Copy
        + Serialize
        + Encode
        + DeserializeOwned
        + Decode<()>
        + GetSize
        + 'static,
{
    /// Creates a new empty responders configuration.
    #[inline]
    pub(crate) fn empty(population: u8) -> Self {
        RespondersConf {
            leader: None,
            responders: KeyRangeMap::new((
                Bitmap::new(population, false),
                None,
            )),
            custom_map: HashMap::new(),
        }
    }

    /// Returns if a server is the supposed leader.
    #[inline]
    pub fn is_leader(&self, id: ReplicaId) -> bool {
        self.leader.is_some_and(|leader| leader == id)
    }

    /// Returns if a server is a supposed responder for a key. Always returns
    /// `false` for non-integer-mappable keys.
    #[inline]
    pub fn is_responder_by_key(&self, key: &String, id: ReplicaId) -> bool {
        if let Ok((responders, _)) = self.responders.get(key) {
            responders.get(id).unwrap_or(false)
        } else {
            // key not mappable
            false
        }
    }

    /// Returns if a server is a supposed responder in the bitmap at custom
    /// index. Always returns `false` for not-found indices.
    #[inline]
    #[allow(dead_code)]
    pub fn is_responder_by_idx(&self, idx: &Idx, id: ReplicaId) -> bool {
        if let Some(responders) = self.custom_map.get(idx) {
            responders.get(id).unwrap_or(false)
        } else {
            false
        }
    }

    /// Returns the responders bitmap (along with its associated index) for a
    /// key, or `None` if key not integer-mappable.
    #[inline]
    pub fn get_responders_by_key(
        &self,
        key: &String,
    ) -> Option<&(Bitmap, Option<Idx>)> {
        self.responders.get(key).ok()
    }

    /// Returns the responders bitmap at custom index, or `None` if index not
    /// found.
    #[inline]
    pub fn get_responders_by_idx(&self, idx: &Idx) -> Option<&Bitmap> {
        self.custom_map.get(idx)
    }

    /// Sets the supposed leader node.
    #[inline]
    pub(crate) fn set_leader(&mut self, leader: ReplicaId) {
        self.leader = Some(leader);
    }

    /// Sets the set of responders (along with an associated index) for a range
    /// of keys. If `range` is `None`, sets it for all keys.
    pub(crate) fn set_responders(
        &mut self,
        range: Option<&(String, String)>,
        responders: Bitmap,
        custom_idx: Option<Idx>,
    ) -> Result<(), SummersetError> {
        if let Some((start, end)) = range {
            // insertion with custom index only supported for clean ranges
            // NOTE: could have better implementation for this little helper
            if let Some(idx) = custom_idx {
                if !self.range_clean(start, end)? {
                    return logged_err!(
                        "inserting non-clean range {}-{} with custom index",
                        start,
                        end
                    );
                }
                if let Some(old_idx) = self.responders.get(start)?.1 {
                    self.custom_map.remove(&old_idx);
                }
                self.custom_map.insert(idx, responders.clone());
            }
            self.responders.set(start, end, (responders, custom_idx))
        } else {
            // inserting a full range, so clear custom_map first
            self.custom_map.clear();
            if let Some(idx) = custom_idx {
                self.custom_map.insert(idx, responders.clone());
            }
            self.responders.set_full((responders, custom_idx));
            Ok(())
        }
    }

    /// Resets state to default for all keys; this includes resetting the
    /// supposed leader to `None`.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn reset_full(&mut self) {
        self.leader = None;
        self.responders.reset_full();
        self.custom_map.clear();
    }

    /// Checks that a keys range is settable, defined as either not overlapping
    /// with any previously inserted ranges, or would exactly overwrite one
    /// existing range. Some protocol implementations may need to enforce this
    /// when setting a keys range.
    pub(crate) fn range_clean(
        &self,
        start: &String,
        end: &String,
    ) -> Result<bool, SummersetError> {
        let overlaps: Vec<_> =
            self.responders.get_overlaps(start, end)?.collect();
        match overlaps.len().cmp(&1) {
            Ordering::Less => Ok(true),
            Ordering::Equal => {
                let (ostart, oend, _) = overlaps[0];
                Ok(key_to_inty(start)? == *ostart && key_to_inty(end)? == *oend)
            }
            Ordering::Greater => Ok(false),
        }
    }

    /// Returns a clone of myself where a given node is unmarked from any
    /// special roles in the config. This clone is needed due to the current
    /// lack of support for mutable ref to values in `RangeMap`s.
    pub(crate) fn clone_filtered(
        &self,
        id: ReplicaId,
    ) -> Result<Self, SummersetError> {
        self.clone().into_filtered(id)
    }

    /// Same as `.clone_filtered()` except consuming `self`.
    pub(crate) fn into_filtered(
        self,
        id: ReplicaId,
    ) -> Result<Self, SummersetError> {
        let mut new_conf = RespondersConf {
            leader: if self.leader == Some(id) {
                None
            } else {
                self.leader
            },
            responders: KeyRangeMap::new(self.responders.default),
            custom_map: self.custom_map,
        };

        for (range, (mut responders, custom_idx)) in self.responders.map {
            responders.set(id, false)?;
            new_conf
                .responders
                .map
                .insert(range, (responders, custom_idx));
        }
        new_conf.responders.default.0.set(id, false)?;

        for responders in new_conf.custom_map.values_mut() {
            responders.set(id, false)?;
        }

        Ok(new_conf)
    }
}

impl<Idx> fmt::Debug for RespondersConf<Idx>
where
    Idx: fmt::Debug
        + Eq
        + Hash
        + Default
        + Copy
        + Serialize
        + Encode
        + DeserializeOwned
        + Decode<()>
        + GetSize
        + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<{}>",
            if let Some(leader) = self.leader {
                leader.to_string()
            } else {
                "_".into()
            }
        )?;
        for (start, end, (responders, _)) in self.responders.iter() {
            if *start == IntyKey::MIN {
                write!(f, " kmin")?;
            } else {
                write!(f, " k{}", start)?;
            }
            if *end == IntyKey::MAX {
                write!(f, "~kmax")?;
            } else {
                write!(f, "~k{}", end)?;
            }
            write!(f, "=>{:?}", responders)?;
        }
        Ok(())
    }
}

impl<Idx> GetSize for RespondersConf<Idx>
where
    Idx: fmt::Debug
        + Eq
        + Hash
        + Default
        + Copy
        + Serialize
        + Encode
        + DeserializeOwned
        + Decode<()>
        + GetSize
        + 'static,
{
    fn get_heap_size(&self) -> usize {
        self.leader.get_heap_size()
            + self.responders.get_heap_size()
            + self.custom_map.get_heap_size()
    }
}

/// If a string key is of the format 'k<number>' it is allowed to be converted
/// into an integer '<number>' to support certain operations, e.g., key ranges.
type IntyKey = u128;

/// Converts a string key to an integer key.
fn key_to_inty(key: &str) -> Result<IntyKey, SummersetError> {
    Ok(key.trim_start_matches(['k', 'K']).parse::<IntyKey>()?)
}

/// Converts an integer key to a string key. Be warned that the converted
/// string key will not contain leading zeros, so should not be used for being
/// compared with original string keys.
#[allow(dead_code)]
fn inty_to_key(key: IntyKey) -> String {
    format!("k{}", key)
}

/// Inclusive range map for special keys that can be treated as integers.
//
// NOTE: this map works by starting from a default value for all keys and only
//       allowing insertions, no removals of ranges (unless reset).
#[derive(Debug, PartialEq, Eq, Default, Clone, Serialize, Deserialize)]
pub struct KeyRangeMap<T: Clone + Eq> {
    /// Internal range map.
    map: RangeInclusiveMap<IntyKey, T>,

    /// Default value for keys not in any inserted range.
    default: T,
}

impl<T> Encode for KeyRangeMap<T>
where
    T: Encode + Clone + Eq,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // encode default value
        self.default.encode(encoder)?;
        // encode entries as Vec<(start, end, value)>
        let entries: Vec<(IntyKey, IntyKey, &T)> = self
            .map
            .iter()
            .map(|(range, value)| (*range.start(), *range.end(), value))
            .collect();
        entries.encode(encoder)
    }
}

impl<T, Ctx> Decode<Ctx> for KeyRangeMap<T>
where
    T: Decode<Ctx> + Clone + Eq,
{
    fn decode<D: bincode::de::Decoder<Context = Ctx>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let default = T::decode(decoder)?;
        let entries: Vec<(IntyKey, IntyKey, T)> =
            Vec::<(IntyKey, IntyKey, T)>::decode(decoder)?;
        let mut map = RangeInclusiveMap::new();
        for (start, end, value) in entries {
            map.insert(start..=end, value);
        }
        Ok(KeyRangeMap { map, default })
    }
}

impl<'de, T, Ctx> bincode::BorrowDecode<'de, Ctx> for KeyRangeMap<T>
where
    T: bincode::BorrowDecode<'de, Ctx> + Clone + Eq,
{
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Ctx>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let default = T::borrow_decode(decoder)?;
        let entries: Vec<(IntyKey, IntyKey, T)> =
            Vec::<(IntyKey, IntyKey, T)>::borrow_decode(decoder)?;
        let mut map = RangeInclusiveMap::new();
        for (start, end, value) in entries {
            map.insert(start..=end, value);
        }
        Ok(KeyRangeMap { map, default })
    }
}

impl<T> KeyRangeMap<T>
where
    T: fmt::Debug
        + Eq
        + Default
        + Clone
        + Serialize
        + Encode
        + DeserializeOwned
        + Decode<()>
        + GetSize
        + 'static,
{
    /// Creates a new `KeyRangeMap` with all keys mapped to a default value.
    fn new(default: T) -> Self {
        KeyRangeMap {
            map: RangeInclusiveMap::new(),
            default,
        }
    }

    /// Gets the value for given key, or returns the default value if not found.
    pub fn get(&self, key: impl AsRef<str>) -> Result<&T, SummersetError> {
        let key_int = key_to_inty(key.as_ref())?;
        if let Some(value) = self.map.get(&key_int) {
            Ok(value)
        } else {
            Ok(&self.default)
        }
    }

    /// Returns an iterator over previously inserted ranges that overlap with
    /// given input.
    pub fn get_overlaps(
        &self,
        start: impl AsRef<str>,
        end: impl AsRef<str>,
    ) -> Result<impl Iterator<Item = (&IntyKey, &IntyKey, &T)>, SummersetError>
    {
        let start_int = key_to_inty(start.as_ref())?;
        let end_int = key_to_inty(end.as_ref())?;
        if start_int > end_int {
            logged_err!(
                "start key '{}' is numerically > end key '{}'",
                start_int,
                end_int
            )
        } else {
            Ok(self
                .map
                .overlapping(start_int..=end_int)
                .map(|(range, value)| (range.start(), range.end(), value)))
        }
    }

    /// Returns a wrapped iterator of the inner range map.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&IntyKey, &IntyKey, &T)> {
        self.map
            .iter()
            .map(|(range, value)| (range.start(), range.end(), value))
    }

    /// Sets a key range to map to given value.
    fn set(
        &mut self,
        start: impl AsRef<str>,
        end: impl AsRef<str>,
        value: T,
    ) -> Result<(), SummersetError> {
        let start_int = key_to_inty(start.as_ref())?;
        let end_int = key_to_inty(end.as_ref())?;
        if start_int > end_int {
            logged_err!(
                "start key '{}' is numerically > end key '{}'",
                start_int,
                end_int
            )
        } else {
            self.map.insert(start_int..=end_int, value);
            Ok(())
        }
    }

    /// Sets the full range of all keys to map to given value.
    fn set_full(&mut self, value: T) {
        let full_range = IntyKey::MIN..=IntyKey::MAX;
        self.map.insert(full_range, value);
    }

    /// Resets the full range of all keys to default value (by clearing the
    /// inner range map).
    fn reset_full(&mut self) {
        self.map.clear();
    }
}

impl<T> GetSize for KeyRangeMap<T>
where
    T: fmt::Debug
        + Eq
        + Default
        + Clone
        + Serialize
        + Encode
        + DeserializeOwned
        + Decode<()>
        + GetSize
        + 'static,
{
    fn get_heap_size(&self) -> usize {
        self.map
            .iter()
            .map(|(_, value)| value.get_heap_size() + 2 * size_of::<IntyKey>())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_default() -> Result<(), SummersetError> {
        let map = KeyRangeMap::<String>::new("default".to_string());
        assert_eq!(map.get("k123")?, "default");
        Ok(())
    }

    #[test]
    fn krmap_set_get() -> Result<(), SummersetError> {
        let mut map = KeyRangeMap::<String>::new("default".to_string());
        map.set("k0", "k123", "low_val".to_string())?;
        map.set("k456", "k777", "high_val".to_string())?;
        assert_eq!(map.get("k0")?, "low_val");
        assert_eq!(map.get("k123")?, "low_val");
        assert_eq!(map.get("k99")?, "low_val");
        assert_eq!(map.get("k456")?, "high_val");
        assert_eq!(map.get("k777")?, "high_val");
        assert_eq!(map.get("k500")?, "high_val");
        assert_eq!(map.get("k321")?, "default");
        map.set("k124", "k500", "low_val".to_string())?;
        assert_eq!(map.get("k123")?, "low_val");
        assert_eq!(map.get("k124")?, "low_val");
        assert_eq!(map.get("k456")?, "low_val");
        assert_eq!(map.get("k500")?, "low_val");
        assert_eq!(map.get("k567")?, "high_val");
        assert_eq!(map.get("k700")?, "high_val");
        assert_eq!(map.get("k777")?, "high_val");
        assert_eq!(map.get("k900")?, "default");
        Ok(())
    }

    #[test]
    fn krmap_set_full() -> Result<(), SummersetError> {
        let mut map = KeyRangeMap::<String>::new("default".to_string());
        map.set("k0", "k123", "low_val".to_string())?;
        assert_eq!(map.get_overlaps("k0", "k123")?.count(), 1);
        assert_eq!(map.get_overlaps("k100", "k500")?.count(), 1);
        map.set("k345", "k456", "low_val".to_string())?;
        assert_eq!(map.get_overlaps("k100", "k500")?.count(), 2);
        map.set_full("full_val".to_string());
        assert_eq!(map.get("k100")?, "full_val");
        assert_eq!(map.get_overlaps("k100", "k500")?.count(), 1);
        map.reset_full();
        assert_eq!(map.get("k100")?, "default");
        assert_eq!(map.get_overlaps("k100", "k500")?.count(), 0);
        Ok(())
    }

    #[test]
    fn krmap_invalid() {
        let mut map = KeyRangeMap::<String>::new("default".to_string());
        assert!(map.get("somerandkey").is_err());
        assert!(map.set("k123", "k0", "low_val".to_string()).is_err());
        assert!(map.set("k0", "somerandkey", "low_val".to_string()).is_err());
        assert!(map.get_overlaps("k123", "k0").is_err());
        assert!(map.get_overlaps("k0", "somerandkey").is_err());
    }

    #[test]
    fn conf_range_clean() -> Result<(), SummersetError> {
        let mut conf = RespondersConf::<()>::empty(5);
        assert!(conf.range_clean(&"k100".to_string(), &"k200".to_string())?);
        conf.set_responders(
            Some(&("k0".to_string(), "k123".to_string())),
            Bitmap::from((5, vec![0, 1, 4])),
            None,
        )?;
        conf.set_responders(
            Some(&("k345".to_string(), "k456".to_string())),
            Bitmap::from((5, vec![0, 1, 4])),
            None,
        )?;
        assert!(conf.range_clean(&"k200".to_string(), &"k300".to_string())?);
        assert!(conf.range_clean(&"k0".to_string(), &"k123".to_string())?);
        assert!(conf.range_clean(&"k345".to_string(), &"k456".to_string())?);
        assert!(!conf.range_clean(&"k100".to_string(), &"k200".to_string())?);
        assert!(!conf.range_clean(&"k300".to_string(), &"k400".to_string())?);
        assert!(!conf.range_clean(&"k0".to_string(), &"k456".to_string())?);
        conf.reset_full();
        assert!(conf.range_clean(&"k100".to_string(), &"k200".to_string())?);
        Ok(())
    }

    #[test]
    fn conf_custom_idx() -> Result<(), SummersetError> {
        let mut conf = RespondersConf::<u32>::empty(5);
        conf.set_responders(
            Some(&("k0".to_string(), "k123".to_string())),
            Bitmap::from((5, vec![0, 1, 4])),
            None,
        )?;
        assert!(!conf.is_responder_by_idx(&7, 4));
        assert!(conf.get_responders_by_idx(&7).is_none());
        conf.set_responders(
            Some(&("k345".to_string(), "k456".to_string())),
            Bitmap::from((5, vec![0, 2, 3])),
            Some(7),
        )?;
        assert!(!conf.is_responder_by_idx(&7, 1));
        assert!(conf.is_responder_by_idx(&7, 2));
        assert!(conf.get_responders_by_idx(&7).is_some());
        conf.set_responders(
            Some(&("k345".to_string(), "k456".to_string())),
            Bitmap::from((5, vec![1, 3, 4])),
            Some(8),
        )?;
        assert!(conf.is_responder_by_idx(&8, 1));
        assert!(!conf.is_responder_by_idx(&8, 2));
        assert!(conf.get_responders_by_idx(&8).is_some());
        assert!(conf.get_responders_by_idx(&7).is_none());
        conf.reset_full();
        assert!(conf.get_responders_by_idx(&7).is_none());
        assert!(conf.get_responders_by_idx(&8).is_none());
        Ok(())
    }

    #[test]
    fn conf_into_filtered() -> Result<(), SummersetError> {
        let mut conf = RespondersConf::<u32>::empty(5);
        conf.set_responders(
            Some(&("k345".to_string(), "k456".to_string())),
            Bitmap::from((5, vec![0, 2, 3])),
            Some(7),
        )?;
        let filtered = conf.into_filtered(3)?;
        assert!(!filtered.is_responder_by_key(&"k0".to_string(), 3));
        assert!(!filtered.is_responder_by_key(&"k400".to_string(), 3));
        assert!(!filtered.is_responder_by_idx(&7, 3));
        Ok(())
    }
}
