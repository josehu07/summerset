//! Summerset client side library.

use summerset::{SummersetClientStub, Command, CommandResult};
pub use summerset::{SMRProtocol, SummersetError, InitError};

use std::collections::HashSet;

/// Client side structure wrapper providing ergonomic key-value API.
/// This struct is NOT thread-safe.
#[derive(Debug)]
pub struct SummersetClient {
    /// Internal client stub struct.
    stub: SummersetClientStub,
}

impl SummersetClient {
    /// Create a new Summerset client structure.
    pub fn new(
        protocol: SMRProtocol,
        servers: &Vec<String>,
    ) -> Result<Self, InitError> {
        // server list must not be empty
        if servers.is_empty() {
            return Err(InitError("servers list is empty".into()));
        }

        // check for duplicate servers
        let mut server_set = HashSet::new();
        for s in servers.iter() {
            if server_set.contains(s) {
                return Err(InitError(format!(
                    "duplicate server address {} given",
                    s
                )));
            }
            server_set.insert(s.clone());
        }

        // pass servers list into client stub initializer for
        // protocol-specific errors
        SummersetClientStub::new(protocol, servers)
            .map(|c| SummersetClient { stub: c })
    }

    /// Do a Get request, looking up a key in the state machine. Returns
    /// `Ok(Option<String>)` on success, where the option is `Some(value)` if
    /// the key is found in the state machine, or `None` if the key does not
    /// exist. Returns `Err(SummersetError)` if any error occurs.
    pub fn get(
        &mut self,
        key: impl Into<String>,
    ) -> Result<Option<String>, SummersetError> {
        // compose get command struct
        let key_s: String = key.into();
        if key_s.is_empty() {
            return Err(SummersetError::CommandEmptyKey);
        }
        let cmd = Command::Get { key: key_s };

        // invoke client stub command interface
        match self.stub.complete(cmd) {
            Ok(CommandResult::GetResult { value }) => Ok(value),
            Err(e) => Err(e),
            _ => Err(SummersetError::WrongCommandType),
        }
    }

    /// Do a Put request, setting the value of key in the state machine to a
    /// new value. Returns `Ok(Option<String>)` on success, where the option is
    /// `Some(old_value)` if the key was already found in the state machine, or
    /// `None` if the key did not exist. Returns `Err(SummersetError)` if any
    /// error occurs.
    pub fn put(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, SummersetError> {
        // compose put command struct
        let key_s: String = key.into();
        if key_s.is_empty() {
            return Err(SummersetError::CommandEmptyKey);
        }
        let cmd = Command::Put {
            key: key_s,
            value: value.into(),
        };

        // invoke client stub command interface
        match self.stub.complete(cmd) {
            Ok(CommandResult::PutResult { old_value }) => Ok(old_value),
            Err(e) => Err(e),
            _ => Err(SummersetError::WrongCommandType),
        }
    }
}

#[cfg(test)]
mod client_tests {
    use super::{SummersetClient, SMRProtocol, InitError};

    #[test]
    fn sanitize_empty_servers() {
        let servers: Vec<String> = Vec::new();
        let client = SummersetClient::new(SMRProtocol::DoNothing, &servers);
        assert!(client.is_err());
        assert_eq!(
            client.unwrap_err(),
            InitError("servers list is empty".into())
        );
    }

    #[test]
    fn sanitize_duplicate_server() {
        let servers = vec!["somehost:50078".into(), "somehost:50078".into()];
        let client = SummersetClient::new(SMRProtocol::DoNothing, &servers);
        assert!(client.is_err());
        assert_eq!(
            client.unwrap_err(),
            InitError("duplicate server address somehost:50078 given".into())
        );
    }
}
