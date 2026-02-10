//! Safe TCP bind/connect/read/write helper functions.

use std::io::ErrorKind;
use std::marker::Unpin;
use std::net::{Ipv4Addr, SocketAddr};
use std::process::Command;

use bincode::{Decode, Encode};
use bytes::{Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::{self, Duration};

use crate::utils::SummersetError;

/// Receives an object of type `T` from TCP readable connection `conn_read`,
/// using `read_buf` as buffer storage for partial reads. Returns:
///   - `Ok(obj)` if successful; upon returning, the read buffer is cleared
///   - `Err(err)` if any unexpected error occurs
///
/// CANCELLATION SAFETY: we cannot use `read_u64()` and `read_exact()` here
/// because this function is intended to be used as a `tokio::select!` branch
/// and that those two methods are not cancellation-safe. Instead, in the case
/// of being cancelled midway before receiving the entire object (note that
/// such cancellation can only happen at `.await` points), bytes already read
/// are stored in the read buffer and will continue to be appended by future
/// invocations until successful returning.
pub(crate) async fn safe_tcp_read<T, Conn>(
    read_buf: &mut BytesMut,
    conn_read: &mut Conn,
) -> Result<T, SummersetError>
where
    T: DeserializeOwned + Decode<()>,
    Conn: AsyncReadExt + Unpin,
{
    // read length of obj first
    if read_buf.capacity() < 8 {
        read_buf.reserve(8 - read_buf.capacity());
    }
    while read_buf.len() < 8 {
        // obj_len not wholesomely read from socket before last cancellation
        conn_read.read_buf(read_buf).await?;
    }
    let obj_len = u64::from_be_bytes(read_buf[..8].try_into().unwrap());

    // then read the obj itself
    #[allow(clippy::cast_possible_truncation)]
    let obj_end = 8 + obj_len as usize;
    if read_buf.capacity() < obj_end {
        // capacity not big enough, reserve more space
        read_buf.reserve(obj_end - read_buf.capacity());
    }
    while read_buf.len() < obj_end {
        conn_read.read_buf(read_buf).await?;
    }
    let (obj, obj_len_read) = bincode::decode_from_slice(
        &read_buf[8..obj_end],
        bincode::config::standard(),
    )?;
    debug_assert_eq!(usize::try_from(obj_len).unwrap(), obj_len_read);

    // if reached this point, no further cancellation to this call is
    // possible (because there are no more awaits ahead); discard bytes
    // used in this call
    // TODO: may want to use a ring buffer to avoid potential memmove
    if read_buf.len() > obj_end {
        let buf_tail = Bytes::copy_from_slice(&read_buf[obj_end..]);
        read_buf.clear();
        read_buf.extend_from_slice(&buf_tail);
    } else {
        read_buf.clear();
    }

    Ok(obj)
}

/// Sends an object of type `T` to TCP writable connection `conn_write`, using
/// `write_buf` as buffer storage for partial writes. Returns:
///   - `Ok(true)` if successful
///   - `Ok(false)` if socket full and may block; in this case, bytes of the
///     input object is saved in the write buffer, and the next
///     calls to `send_req()` must give arg `obj == None` to
///     indicate retrying (typically after doing a few reads on the
///     same socket to free up some buffer space), until the
///     function returns success
///   - `Err(err)` if any unexpected error occurs
///
/// DEADLOCK AVOIDANCE: we avoid using `write_u64()` and `write_all()` here
/// because, in the case of TCP buffers being full, if both ends of the
/// connection are trying to write, they may both be blocking on either of
/// these two methods, resulting in a circular deadlock.
pub(crate) fn safe_tcp_write<T, Conn>(
    write_buf: &mut BytesMut,
    write_buf_cursor: &mut usize,
    conn_write: &Conn,
    obj: Option<&T>,
) -> Result<bool, SummersetError>
where
    T: Serialize + Encode,
    Conn: AsRef<TcpStream>,
{
    // if last write was not successful, cannot send a new object
    if obj.is_some() && !write_buf.is_empty() {
        return Err(SummersetError::msg(
            "attempting new object while should retry",
        ));
    } else if obj.is_none() && write_buf.is_empty() {
        return Err(SummersetError::msg(
            "attempting to retry while buffer is empty",
        ));
    } else if let Some(obj) = obj {
        // sending a new object, fill write_buf
        debug_assert_eq!(*write_buf_cursor, 0);
        let write_bytes =
            bincode::encode_to_vec(obj, bincode::config::standard())?;
        let write_len = write_bytes.len();
        write_buf.extend_from_slice(&write_len.to_be_bytes());
        debug_assert_eq!(write_buf.len(), 8);
        write_buf.extend_from_slice(write_bytes.as_slice());
    } else {
        // retrying last unsuccessful write
        debug_assert!(*write_buf_cursor < write_buf.len());
    }

    // try until the length + the object are all written
    while *write_buf_cursor < write_buf.len() {
        match conn_write
            .as_ref()
            .try_write(&write_buf[*write_buf_cursor..])
        {
            Ok(n) => {
                *write_buf_cursor += n;
            }
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                return Ok(false);
            }
            Err(err) => return Err(err.into()),
        }
    }

    // everything written, clear write_buf
    write_buf.clear();
    *write_buf_cursor = 0;

    Ok(true)
}

/// Wrapper over tokio `TcpListener::bind()` that provides a retrying logic.
pub(crate) async fn tcp_bind_with_retry(
    bind_addr: SocketAddr,
    mut retries: u8,
) -> Result<TcpListener, SummersetError> {
    loop {
        let socket = TcpSocket::new_v4()?;
        socket.set_linger(None)?;
        socket.set_reuseaddr(true)?;
        socket.set_reuseport(true)?;
        socket.set_nodelay(true)?;

        let bind_addr = (Ipv4Addr::UNSPECIFIED, bind_addr.port()).into();
        if let Err(e) = socket.bind(bind_addr) {
            eprintln!("Binding {} failed!", bind_addr);
            eprintln!("Output of `ss` command:");
            eprintln!("{}", get_ss_cmd_output()?);
            return Err(SummersetError::from(e));
        }

        match socket.listen(1024) {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                if retries == 0 {
                    return Err(err.into());
                }
                retries -= 1;
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Wrapper over tokio `TcpStream::connect()` that provides a retrying logic.
pub(crate) async fn tcp_connect_with_retry(
    conn_addr: SocketAddr,
    mut retries: u8,
) -> Result<TcpStream, SummersetError> {
    loop {
        let socket = TcpSocket::new_v4()?;
        socket.set_linger(None)?;
        socket.set_reuseaddr(true)?;
        socket.set_reuseport(true)?;
        socket.set_nodelay(true)?;

        match socket.connect(conn_addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                if retries == 0 {
                    return Err(err.into());
                }
                retries -= 1;
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn get_ss_cmd_output() -> Result<String, SummersetError> {
    Ok(String::from_utf8(
        Command::new("ss").arg("-tnap").output()?.stdout,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "ad-hoc test for special experiments only"]
    fn safetcp_try_ss() -> Result<(), SummersetError> {
        println!("Output of `ss` command:");
        println!("{}", get_ss_cmd_output()?);
        Ok(())
    }
}
