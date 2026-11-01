//! Serialization codec configuration and helper trait (backed by `wincode`).

use wincode::config::{Configuration, PREALLOCATION_SIZE_LIMIT_DISABLED};
use wincode::int_encoding::{FixInt, LittleEndian};
use wincode::len::BincodeLen;
use wincode::{SchemaRead, SchemaWrite};

/// Codec configuration for all wire messages and durable entries: fixed-width
/// little-endian integers, `u8` enum tags, and no preallocation size limit
/// (peers are trusted and values/batches can be large).
pub type CodecConfig =
    Configuration<true, PREALLOCATION_SIZE_LIMIT_DISABLED, BincodeLen, LittleEndian, FixInt, u8>;

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
