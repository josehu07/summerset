//! Safe TCP bind/connect/read/write helper functions.

use std::io::ErrorKind;
use std::marker::Unpin;
use std::net::{Ipv4Addr, SocketAddr};
use std::process::Command;

use bytes::{Buf, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::{self, Duration};

use crate::utils::{CODEC_CONFIG, Codec, DECODE_SIZE_LIMIT, SummersetError, raise_fd_limit};

/// Deadline for a new connection to finish its handshake, so that a stalled
/// connection cannot block accepting others for long.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
    T: DeserializeOwned + Codec,
    Conn: AsyncReadExt + Unpin,
{
    // read length of obj first
    if read_buf.capacity() < 8 {
        read_buf.reserve(8 - read_buf.len());
    }
    while read_buf.len() < 8 {
        // obj_len not wholesomely read from socket before last cancellation
        if conn_read.read_buf(read_buf).await? == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
        }
    }
    let obj_len = u64::from_be_bytes(read_buf[..8].try_into().unwrap());

    // sanity check that the object size is reasonable before allocating for
    // it; there are cases when unexpected signal bytes slip through the
    // connection when a benchmarking client exists abruptly, causing absurdly
    // large `obj_len`
    let Some(obj_end) = usize::try_from(obj_len)
        .ok()
        .filter(|&len| len <= DECODE_SIZE_LIMIT)
        .map(|len| 8 + len)
    else {
        read_buf.clear();
        return Err(SummersetError::msg(format!(
            "ignoring invalidly large obj_len: {}",
            obj_len
        )));
    };

    // then read the obj itself
    if read_buf.capacity() < obj_end {
        // capacity not big enough, reserve more space
        read_buf.reserve(obj_end - read_buf.len());
    }
    while read_buf.len() < obj_end {
        if conn_read.read_buf(read_buf).await? == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
        }
    }
    let obj = wincode::config::deserialize_exact(&read_buf[8..obj_end], CODEC_CONFIG);

    // if reached this point, no further cancellation to this call is
    // possible (because there are no more awaits ahead); discard bytes
    // used in this call, even if they failed to decode, so that the next
    // call starts at the next object
    if read_buf.len() > obj_end {
        read_buf.advance(obj_end);
    } else {
        read_buf.clear();
    }

    Ok(obj?)
}

/// Sends an object of type `T` to TCP writable connection `conn_write`, using
/// `write_buf` as buffer storage for partial writes. Returns:
///   - `Ok(true)` if successful (or if retrying with nothing left to send)
///   - `Ok(false)` if socket full and may block; in this case, bytes of the
///     input object is saved in the write buffer, and the next
///     calls to `send_req()` must give arg `obj == None` to
///     indicate retrying (typically after doing a few reads on the
///     same socket to free up some buffer space), until the
///     function returns success
///   - `Err(err)` if any unexpected error occurs; an object failing to be
///     written is then dropped, so that later calls can send new objects
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
    T: Serialize + Codec,
    Conn: AsRef<TcpStream>,
{
    // if last write was not successful, cannot send a new object
    if obj.is_some() && !write_buf.is_empty() {
        return Err(SummersetError::msg(
            "attempting new object while should retry",
        ));
    } else if obj.is_none() && write_buf.is_empty() {
        // nothing left to retry, e.g., as a failed write dropped its object
        return Ok(true);
    } else if let Some(obj) = obj {
        // sending a new object, encode it into write_buf after its length
        debug_assert_eq!(*write_buf_cursor, 0);
        let obj_len = wincode::config::serialized_size(obj, CODEC_CONFIG)?;
        write_buf.resize(8 + usize::try_from(obj_len)?, 0);
        write_buf[..8].copy_from_slice(&obj_len.to_be_bytes());
        if let Err(err) = wincode::config::serialize_into(&mut write_buf[8..], obj, CODEC_CONFIG) {
            write_buf.clear();
            return Err(err.into());
        }
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
            Err(err) => {
                // drop the object, which would otherwise stay stuck in the
                // buffer and fail all later sends
                write_buf.clear();
                *write_buf_cursor = 0;
                return Err(err.into());
            }
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
    raise_fd_limit();
    let bind_addr = (Ipv4Addr::UNSPECIFIED, bind_addr.port()).into();
    loop {
        let socket = TcpSocket::new_v4()?;
        #[allow(
            deprecated,
            reason = "setting linger period explicitly to None to turn lingering off"
        )]
        socket.set_linger(None)?;
        socket.set_reuseaddr(true)?;
        socket.set_reuseport(true)?;
        socket.set_nodelay(true)?;

        match socket.bind(bind_addr).and_then(|()| socket.listen(1024)) {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                if retries == 0 {
                    eprintln!("Binding {} failed!", bind_addr);
                    eprintln!("Output of `ss` command:");
                    eprintln!("{}", get_ss_cmd_output().unwrap_or_else(|e| e.to_string()));
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
    raise_fd_limit();
    loop {
        let socket = TcpSocket::new_v4()?;
        #[allow(
            deprecated,
            reason = "setting linger period explicitly to None to turn lingering off"
        )]
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
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Returns the local address at `offset` within a window of ports reserved for
/// this test process: below the OS ephemeral range, at a random start, and
/// probed to be unused, so that concurrent test runs and local clusters do not
/// collide.
#[cfg(test)]
pub(crate) fn test_addr_at_offset(offset: u16) -> SocketAddr {
    use std::sync::LazyLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    const WINDOW: u16 = 256;
    const NUM_WINDOWS: u16 = 12000 / WINDOW; // from port 20000 up
    static BASE: LazyLock<u16> = LazyLock::new(|| {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let start = now.subsec_nanos() ^ std::process::id();
        (0..u32::from(NUM_WINDOWS))
            .map(|i| {
                let idx = start.wrapping_add(i) % u32::from(NUM_WINDOWS);
                20000 + WINDOW * u16::try_from(idx).unwrap()
            })
            .find(|&base| {
                // without SO_REUSEADDR, binding fails if a port is in any use
                (base..base + WINDOW).all(|port| {
                    TcpSocket::new_v4()
                        .and_then(|socket| socket.bind(([0, 0, 0, 0], port).into()))
                        .is_ok()
                })
            })
            .expect("no free window of test ports")
    });
    assert!(offset < WINDOW, "test port offset {offset} out of window");
    ([127, 0, 0, 1], *BASE + offset).into()
}

fn get_ss_cmd_output() -> Result<String, SummersetError> {
    Ok(String::from_utf8(
        Command::new("ss").arg("-tnap").output()?.stdout,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::sync::oneshot;

    /// Frames `payload` as `safe_tcp_write()` sends it.
    fn frame_payload_bytes(payload: &[u8]) -> Vec<u8> {
        [&(payload.len() as u64).to_be_bytes(), payload].concat()
    }

    /// Returns both ends of a fresh TCP connection on the test port at `offset`.
    async fn connect_test_tcp_pair(offset: u16) -> Result<(TcpStream, TcpStream), SummersetError> {
        let addr = test_addr_at_offset(offset);
        let listener = tcp_bind_with_retry(addr, 0).await?;
        let (conn, accepted) = tokio::join!(tcp_connect_with_retry(addr, 0), listener.accept());
        Ok((conn?, accepted?.0))
    }

    #[tokio::test]
    async fn read_fragmented_and_cancelled() -> Result<(), SummersetError> {
        let objs = vec![vec![7u8; 100], vec![], vec![9; 300]];
        let mut bytes = vec![];
        for obj in &objs {
            bytes.extend(frame_payload_bytes(&wincode::config::serialize(
                obj,
                CODEC_CONFIG,
            )?));
        }
        let (mut conn_read, mut conn_write) = duplex(64);
        let (mut read_buf, mut got) = (BytesMut::new(), vec![]);
        for chunk in bytes.chunks(3) {
            conn_write.write_all(chunk).await?;
            // polls once to read what has arrived, cancelling the read if the
            // object is incomplete
            if let Some(obj) =
                safe_tcp_read::<Vec<u8>, _>(&mut read_buf, &mut conn_read).now_or_never()
            {
                got.push(obj?);
            }
        }
        assert_eq!(got, objs);
        assert!(read_buf.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn read_skips_bad_frames() -> Result<(), SummersetError> {
        let (mut conn_read, mut conn_write) = duplex(1024);
        let mut read_buf = BytesMut::new();
        // a truncated payload and a collection beyond the preallocation limit
        // fail to decode, but do not affect the next object
        let good = wincode::config::serialize(&vec![1u64, 2], CODEC_CONFIG)?;
        for payload in [&[1, 2][..], &(1u64 << 40).to_le_bytes(), &good] {
            conn_write.write_all(&frame_payload_bytes(payload)).await?;
        }
        for _ in 0..2 {
            assert!(
                safe_tcp_read::<Vec<u64>, _>(&mut read_buf, &mut conn_read)
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            safe_tcp_read::<Vec<u64>, _>(&mut read_buf, &mut conn_read).await?,
            vec![1, 2]
        );
        // an oversized length is rejected before allocating for it
        conn_write
            .write_all(&(DECODE_SIZE_LIMIT as u64 + 1).to_be_bytes())
            .await?;
        assert!(
            safe_tcp_read::<Vec<u64>, _>(&mut read_buf, &mut conn_read)
                .await
                .is_err()
        );
        assert!(read_buf.is_empty() && read_buf.capacity() < 1 << 20);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_blocked_by_slow_reader() -> Result<(), SummersetError> {
        let (conn, mut peer) = connect_test_tcp_pair(150).await?;
        let (_conn_read, conn_write) = conn.into_split();
        let objs: Vec<Vec<u8>> = (0..=255).map(|i| vec![i; 64 << 10]).collect();
        let (tx_blocked, rx_blocked) = oneshot::channel();
        let reader = tokio::spawn({
            let objs = objs.clone();
            async move {
                // starts reading only once the writer is blocked
                let _ = rx_blocked.await;
                let mut read_buf = BytesMut::new();
                for obj in objs {
                    assert_eq!(
                        safe_tcp_read::<Vec<u8>, _>(&mut read_buf, &mut peer).await?,
                        obj
                    );
                }
                Ok::<_, SummersetError>(())
            }
        });
        let (mut write_buf, mut cursor) = (BytesMut::new(), 0);
        let mut tx_blocked = Some(tx_blocked);
        for obj in &objs {
            if safe_tcp_write(&mut write_buf, &mut cursor, &conn_write, Some(obj))? {
                continue;
            }
            // a new object cannot be sent before the blocked one
            assert!(safe_tcp_write(&mut write_buf, &mut cursor, &conn_write, Some(obj)).is_err());
            if let Some(tx) = tx_blocked.take() {
                let _ = tx.send(());
            }
            while !safe_tcp_write::<Vec<u8>, _>(&mut write_buf, &mut cursor, &conn_write, None)? {
                conn_write.writable().await?;
            }
        }
        assert!(tx_blocked.is_none(), "writer never blocked");
        reader.await??;
        Ok(())
    }

    #[tokio::test]
    async fn write_error_drops_object() -> Result<(), SummersetError> {
        let (conn, _peer) = connect_test_tcp_pair(151).await?;
        let (_conn_read, mut conn_write) = conn.into_split();
        // fills up the connection until an object is blocked midway
        let obj = vec![0u8; 64 << 10];
        let (mut write_buf, mut cursor) = (BytesMut::new(), 0);
        while safe_tcp_write(&mut write_buf, &mut cursor, &conn_write, Some(&obj))? {}
        // then fails the retry, which drops the object
        conn_write.shutdown().await?;
        conn_write.writable().await?;
        assert!(
            safe_tcp_write::<Vec<u8>, _>(&mut write_buf, &mut cursor, &conn_write, None).is_err()
        );
        assert!(write_buf.is_empty() && cursor == 0);
        // leaving nothing to retry, and later objects fail on their own
        assert!(safe_tcp_write::<Vec<u8>, _>(
            &mut write_buf,
            &mut cursor,
            &conn_write,
            None
        )?);
        assert!(safe_tcp_write(&mut write_buf, &mut cursor, &conn_write, Some(&obj)).is_err());
        assert!(write_buf.is_empty() && cursor == 0);
        Ok(())
    }

    #[test]
    #[ignore = "ad-hoc test for special experiments only"]
    fn safetcp_try_ss() -> Result<(), SummersetError> {
        println!("Output of `ss` command:");
        println!("{}", get_ss_cmd_output()?);
        Ok(())
    }
}
