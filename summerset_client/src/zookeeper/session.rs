//! Core session client wrapper for ZooKeeper.

use std::net::SocketAddr;

use crate::zookeeper::ClientConfigZooKeeper;

use tokio::time::Duration;

use zookeeper_client as zkcli;

use summerset::{parsed_config, SummersetError};

/// Path of parent Znode, under which all "keys" will be created as its
/// children Znodes.
static PARENT_ZNODE: &str = "/summerset";

/// ZooKeeper session client wrapper.
pub(crate) struct ZooKeeperSession {
    /// User-passed server address to connect to.
    server_addr: SocketAddr,

    /// Session client builder.
    builder: zkcli::Connector,

    /// Current connected ZooKeeper session.
    session: Option<zkcli::Client>,

    /// Do a `sync` before every `get` for stricter consistency (though still
    /// not linearizability).
    sync_on_get: bool,
}

impl ZooKeeperSession {
    /// Create a new ZooKeeper session client.
    pub(crate) fn new(
        server: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigZooKeeper;
                                    expiry_ms, sync_on_get)?;

        let mut builder = zkcli::Client::connector();
        if config.expiry_ms > 0 {
            builder.session_timeout(Duration::from_millis(config.expiry_ms));
        }

        Ok(ZooKeeperSession {
            server_addr: server,
            builder,
            session: None,
            sync_on_get: config.sync_on_get,
        })
    }

    /// Connect to ZooKeeper server, get an active session.
    pub(crate) async fn connect(&mut self) -> Result<(), SummersetError> {
        let zk = self
            .builder
            .connect(&self.server_addr.to_string()[..])
            .await?;

        // create Summerset Znode if not present
        let creaete_result = zk
            .create(
                PARENT_ZNODE,
                b"",
                &zkcli::CreateMode::Persistent
                    .with_acls(zkcli::Acls::anyone_all()),
            )
            .await;
        if let Err(zkcli::Error::NodeExists) = creaete_result {
            // if it already exists, all good
        } else {
            creaete_result?;
        }

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

        if self.sync_on_get {
            // sync still does not guarantee linearizability:
            // https://zookeeper.apache.org/doc/r3.8.4/zookeeperInternals.html
            let sync_result = session.sync(&path).await;
            if let Err(zkcli::Error::NoNode) = sync_result {
                // node does not exist
                return Ok(None);
            } else {
                sync_result?;
            }
        }

        let get_result = session.get_data(&path).await;
        match get_result {
            // node exists and success
            Ok((data, _)) => Ok(Some(String::from_utf8(data)?)),
            // node does not exist
            Err(zkcli::Error::NoNode) => Ok(None),
            // error
            Err(err) => Err(err.into()),
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

        let set_result = session
            .set_data(&path, value.as_bytes(), None) // any version allowed
            .await;
        if let Err(zkcli::Error::NoNode) = set_result {
            // Znode does not exist yet, create with value as initial data
            let create_result = session
                .create(
                    &path,
                    value.as_bytes(),
                    &zkcli::CreateMode::Persistent
                        .with_acls(zkcli::Acls::anyone_all()),
                )
                .await;
            if let Err(zkcli::Error::NodeExists) = create_result {
                // it could be that a concurrent client has created the Znode
                // before my call, so consider it a success
            } else {
                create_result?;
            }
        } else {
            // other errors are returned directly
            set_result?;
        }
        Ok(())
    }
}
