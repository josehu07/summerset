//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use std::net::SocketAddr;

use crate::core::utils::SummersetError;
use crate::core::external::{ApiRequest, ApiReply, ExternalApi};

use async_trait::async_trait;

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::TcpStream;

/// Client stub ID type.
pub type ClientId = u64;

/// Client trait to be implement by all protocol-specific client structs.
#[async_trait]
pub trait GenericClient {
    /// Get the ID of this client.
    fn id(&self) -> ClientId;

    /// Establish connection to the Summerset service; may contain protocol-
    /// specific logic.
    async fn connect(&mut self) -> Result<(), SummersetError>;

    /// Send a request to the Summerset service; may contain protocol-specific
    /// logic.
    async fn send_req(&mut self, req: ApiRequest)
        -> Result<(), SummersetError>;

    /// Receive a reply from the Summerset service; may contain protocol-
    /// specific logic.
    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError>;

    // Below are default implementation of helper functions, provided here
    // so that most client types can just use them out-of-the-box.

    /// Procedure to connect to a given server's ExternalApi module.
    async fn proc_connect(
        id: ClientId,
        addr: &str,
    ) -> Result<TcpStream, SummersetError> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u64(id).await?; // send my client ID
        Ok(stream)
    }

    /// Procedure to send a request to established server connection.
    async fn proc_send_req(
        req: ApiRequest,
        conn: &mut TcpStream,
    ) -> Result<(), SummersetError> {
        let req_bytes = encode_to_vec(req)?;
        conn.write_all(&req_bytes[..]).await?;
        Ok(())
    }

    /// Procedure to receive a reply from established server connection.
    async fn proc_recv_reply(
        conn: &mut TcpStream,
    ) -> Result<ApiReply, SummersetError> {
        let reply_buf: Vec<u8> = vec![0; ExternalApi::REPLY_SIZE];
        conn.read_exact(&mut reply_buf[..]).await?;
        let reply = decode_from_slice(&reply_buf)?;
        Ok(reply)
    }
}

/// Dummy client type, mainly for testing purposes.
pub struct DummyClient {
    id: ClientId,
    servers: Vec<String>,
    conn: Option<TcpStream>,
}

impl DummyClient {
    pub fn new(
        id: ClientId,
        servers: Vec<String>,
    ) -> Result<Self, SummersetError> {
        if servers.len() == 0 {
            return Err(SummersetError("servers list is empty".into()));
        }
        for &server in &servers {
            if let Err(_) = server.parse::<SocketAddr>() {
                return Err(SummersetError(format!(
                    "invalid server addr string '{}'",
                    server
                )));
            }
        }

        Ok(DummyClient {
            id,
            servers,
            conn: None,
        })
    }
}

impl Default for DummyClient {
    fn default() -> Self {
        DummyClient {
            id: 7123,
            servers: vec!["127.0.0.1:52700".into()],
            conn: None,
        }
    }
}

#[async_trait]
impl GenericClient for DummyClient {
    fn id(&self) -> ClientId {
        self.id
    }

    async fn connect(&mut self) -> Result<(), SummersetError> {
        // picks the first replica
        let stream = Self::proc_connect(self.id, &self.servers[0]).await?;
        self.conn = Some(stream);
        Ok(())
    }

    async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        if let None = self.conn {
            return Err(SummersetError(
                "connection not established yet".into(),
            ));
        }
        Self::proc_send_req(req, &mut self.conn.unwrap())?;
        Ok(())
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        if let None = self.conn {
            return Err(SummersetError(
                "connection not established yet".into(),
            ));
        }
        let reply = Self::proc_recv_reply(&mut self.conn.unwrap())?;
        Ok(reply)
    }
}

#[cfg(test)]
mod client_test {
    use super::*;
    use rand::Rng;

    #[test]
    fn dummy_client_new() {
        let id = rand::thread_rng().gen::<u64>();
        assert!(DummyClient::new(id, vec![]).is_err());
        assert!(DummyClient::new(
            id,
            vec!["127.0.0.1:52700".into(), "987abc123".into()]
        )
        .is_err());
        assert!(DummyClient::new(id, vec!["127.0.0.1:52700".into()]).is_ok());
        assert!(DummyClient::new(
            id,
            vec!["127.0.0.1:52700".into(), "127.0.0.1:52701".into()]
        )
        .is_ok());
    }

    #[test]
    fn dummy_client_id() {
        let id = rand::thread_rng().gen::<u64>();
        let dc = DummyClient::new(id, vec!["127.0.0.1:52700".into()]).unwrap();
        assert_eq!(dc.id(), id);
    }
}
