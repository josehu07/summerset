//! Core session client wrapper for ZooKeeper.

use std::net::SocketAddr;

use crate::zookeeper::ClientConfigZooKeeper;

use tokio::time::Duration;

use tokio_zookeeper::{
    error as zk_error, Acl, CreateMode, ZooKeeper, ZooKeeperBuilder,
};

use summerset::{parsed_config, SummersetError};

/// Path of parent Znode, under which all "keys" will be created as its
/// children Znodes.
static PARENT_ZNODE: &str = "/summerset";

/// ZooKeeper session client wrapper.
pub(crate) struct ZooKeeperSession {
    /// User-passed server address to connect to.
    server_addr: SocketAddr,

    /// Session client builder.
    builder: ZooKeeperBuilder,

    /// Current connected ZooKeeper session.
    session: Option<ZooKeeper>,
}

impl ZooKeeperSession {
    /// Create a new ZooKeeper session client.
    pub(crate) fn new(
        server: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigZooKeeper;
                                    expiry_ms)?;

        let mut builder = ZooKeeperBuilder::default();
        if config.expiry_ms > 0 {
            builder.set_timeout(Duration::from_millis(config.expiry_ms));
        }

        Ok(ZooKeeperSession {
            server_addr: server,
            builder,
            session: None,
        })
    }

    /// Connect to ZooKeeper server, get an active session.
    pub(crate) async fn connect(&mut self) -> Result<(), SummersetError> {
        let (zk, _) = self.builder.connect(&self.server_addr).await?;

        // create Summerset Znode if not present
        zk.create(
            PARENT_ZNODE,
            b"",
            Acl::open_unsafe(),
            CreateMode::Persistent,
        )
        .await?
        .or_else(|e| {
            if let zk_error::Create::NodeExists = e {
                Ok(PARENT_ZNODE.into())
            } else {
                Err(e)
            }
        })?;

        self.session = Some(zk);
        Ok(())
    }

    /// Leave the current ZooKeeper session.
    pub(crate) async fn leave(&mut self) -> Result<(), SummersetError> {
        self.session = None;
        Ok(())
    }

    /// Make a getData request to ZooKeeper.
    pub(crate) async fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>, SummersetError> {
        let path = format!("{}/{}", PARENT_ZNODE, key);
        let session = self
            .session
            .as_ref()
            .ok_or(SummersetError::msg("no active session"))?;
        let data = session.get_data(&path).await?;

        if let Some((data, _)) = data {
            // stat ignored
            Ok(Some(String::from_utf8(data)?))
        } else {
            Ok(None)
        }
    }

    /// Make a setData request to ZooKeeper.
    pub(crate) async fn set(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), SummersetError> {
        let path = format!("{}/{}", PARENT_ZNODE, key);
        let session = self
            .session
            .as_ref()
            .ok_or(SummersetError::msg("no active session"))?;
        let result = session
            .set_data(&path, None, value.as_bytes().to_owned()) // any version allowed
            .await?;

        if let Err(zk_error::SetData::NoNode) = result {
            // Znode does not exist yet, create with value as initial data
            session
                .create(
                    &path,
                    value.as_bytes().to_owned(),
                    Acl::open_unsafe(),
                    CreateMode::Persistent,
                )
                .await?
                .or_else(|e| {
                    if let zk_error::Create::NodeExists = e {
                        // it could be that a concurrent client has created
                        // the Znode before my call, so consider it a success
                        Ok(path)
                    } else {
                        Err(e)
                    }
                })?;
        } else {
            // all other errors are returned directly
            result?;
        }
        Ok(())
    }
}
