//! Core KV client wrapper for etcd.

use std::net::SocketAddr;

use etcd_client as etcdcli;
use summerset::{SummersetError, parsed_config};
use tokio::time::Duration;

use crate::etcd::ClientConfigEtcd;

/// etcd KV client wrapper.
pub(crate) struct EtcdKvClient {
    /// User-passed server address to connect to.
    server_addr: SocketAddr,

    /// Connection options.
    connect_opts: etcdcli::ConnectOptions,

    /// Current connected etcd KV client struct.
    kv_client: Option<etcdcli::KvClient>,

    /// Use sequential consistency on reads (i.e., stale reads), instead of
    /// linearizable reads.
    stale_reads: bool,
}

impl EtcdKvClient {
    /// Create a new etcd KV client.
    pub(crate) fn new(
        server: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigEtcd;
                                    timeout_ms, stale_reads)?;

        let mut connect_opts = etcdcli::ConnectOptions::new();
        if config.timeout_ms > 0 {
            connect_opts = connect_opts
                .with_connect_timeout(Duration::from_millis(config.timeout_ms))
                .with_timeout(Duration::from_millis(config.timeout_ms));
        }

        Ok(EtcdKvClient {
            server_addr: server,
            connect_opts,
            kv_client: None,
            stale_reads: config.stale_reads,
        })
    }

    /// Connect to the given etcd server, keeping only the KV client struct.
    pub(crate) async fn connect(&mut self) -> Result<(), SummersetError> {
        let client = etcdcli::Client::connect(
            [&self.server_addr.to_string()],
            Some(self.connect_opts.clone()),
        )
        .await?;

        self.kv_client = Some(client.kv_client());
        Ok(())
    }

    /// Leave the current etcd client connection.
    pub(crate) fn leave(&mut self) {
        self.kv_client = None;
    }

    /// Make a get request to etcd.
    pub(crate) async fn get(
        &mut self,
        key: &str,
    ) -> Result<Option<String>, SummersetError> {
        let kv_client = self
            .kv_client
            .as_mut()
            .ok_or(SummersetError::msg("no active connection"))?;

        let get_opts = if self.stale_reads {
            // use sequential consistency for reads
            Some(etcdcli::GetOptions::new().with_serializable())
        } else {
            None
        };

        let mut get_resp = kv_client.get(key, get_opts).await?;
        let value = if get_resp.count() == 0 {
            None
        } else {
            Some(String::from_utf8(
                get_resp.take_kvs().remove(0).into_key_value().1,
            )?)
        };
        Ok(value)
    }

    /// Make a put request to etcd.
    pub(crate) async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<String>, SummersetError> {
        let kv_client = self
            .kv_client
            .as_mut()
            .ok_or(SummersetError::msg("no active connection"))?;

        // always return previous value (as this is the behavior of Summerset)
        let put_opts = Some(etcdcli::PutOptions::new().with_prev_key());

        let mut put_resp = kv_client.put(key, value, put_opts).await?;
        let old_value = if let Some(bytes) =
            put_resp.take_prev_key().map(|kv| kv.into_key_value().1)
        {
            Some(String::from_utf8(bytes)?)
        } else {
            None
        };
        Ok(old_value)
    }
}
