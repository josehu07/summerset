//! Serialization codec configuration and helper trait (backed by `wincode`).

use wincode::config::Configuration;
use wincode::int_encoding::{FixInt, LittleEndian};
use wincode::len::BincodeLen;
use wincode::{SchemaRead, SchemaWrite};

/// Generous safety net on the size of any decoded object or collection, far
/// beyond any real one, so that a corrupted length fails decoding instead of
/// causing a huge allocation.
pub(crate) const DECODE_SIZE_LIMIT: usize = 1 << 34; // 16 GiB

/// Codec configuration for all wire messages and durable entries: fixed-width
/// little-endian integers, `u8` enum tags, and a generous preallocation size
/// limit.
pub type CodecConfig = Configuration<true, DECODE_SIZE_LIMIT, BincodeLen, LittleEndian, FixInt, u8>;

/// Instance of [`CodecConfig`] to pass to `wincode::config` functions.
pub const CODEC_CONFIG: CodecConfig = CodecConfig::new();

/// Shorthand trait for types that can be (de)serialized under [`CodecConfig`].
pub trait Codec:
    SchemaWrite<CodecConfig, Src = Self> + for<'de> SchemaRead<'de, CodecConfig, Dst = Self>
{
}

impl<T> Codec for T where
    T: SchemaWrite<CodecConfig, Src = T> + for<'de> SchemaRead<'de, CodecConfig, Dst = T>
{
}
