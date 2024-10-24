//! Summerset server control messages module implementation.

use std::net::SocketAddr;

use crate::manager::CtrlMsg;
use crate::server::ReplicaId;
use crate::utils::{
    safe_tcp_read, safe_tcp_write, tcp_connect_with_retry, SummersetError, ME,
};

use bytes::BytesMut;

use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// The manager control message handler module.
pub(crate) struct ControlHub {
    /// My replica ID.
    pub(crate) me: ReplicaId,

    /// Number of replicas in cluster.
    pub(crate) population: u8,

    /// Receiver side of the recv channel.
    rx_recv: mpsc::UnboundedReceiver<CtrlMsg>,

    /// Sender side of the send channel.
    tx_send: mpsc::UnboundedSender<CtrlMsg>,

    /// Control messengener task join handle.
    _control_messenger_handle: JoinHandle<()>,
}

// ControlHub public API implementation
impl ControlHub {
    /// Creates a new control message handler module. Connects to the cluster
    /// manager and getting assigned my server ID. Spawns the control messenger
    /// task. Creates a send channel for proactively sending control messages
    /// and a recv channel for buffering incoming control messages. Returns the
    /// assigned server ID on success.
    pub(crate) async fn new_and_setup(
        manager: SocketAddr,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and receive my assigned server ID
        pf_debug!("connecting to manager '{}'...", manager);
        let mut stream = tcp_connect_with_retry(manager, 10).await?;
        let id = stream.read_u8().await?; // first receive assigned server ID
        let population = stream.read_u8().await?; // then receive population
        pf_debug!("assigned server ID: {} of {}", id, population);

        ME.get_or_init(|| id.to_string());

        let (tx_recv, rx_recv) = mpsc::unbounded_channel();
        let (tx_send, rx_send) = mpsc::unbounded_channel();

        let mut messenger =
            ControlHubMessengerTask::new(stream, tx_recv, rx_send);
        let control_messenger_handle =
            tokio::spawn(async move { messenger.run().await });

        Ok(ControlHub {
            me: id,
            population,
            rx_recv,
            tx_send,
            _control_messenger_handle: control_messenger_handle,
        })
    }

    /// Waits for the next control event message from cluster manager.
    pub(crate) async fn recv_ctrl(
        &mut self,
    ) -> Result<CtrlMsg, SummersetError> {
        match self.rx_recv.recv().await {
            Some(msg) => Ok(msg),
            None => logged_err!("recv channel has been closed"),
        }
    }

    /// Sends a control message to the cluster manager.
    pub(crate) fn send_ctrl(
        &mut self,
        msg: CtrlMsg,
    ) -> Result<(), SummersetError> {
        self.tx_send.send(msg).map_err(SummersetError::msg)?;
        Ok(())
    }

    /// Sends a control message to the cluster manager and waits for an
    /// expected reply blockingly.
    pub(crate) async fn do_sync_ctrl(
        &mut self,
        msg: CtrlMsg,
        expect: fn(&CtrlMsg) -> bool,
    ) -> Result<CtrlMsg, SummersetError> {
        self.send_ctrl(msg)?;
        loop {
            let reply = self.recv_ctrl().await?;
            if expect(&reply) {
                return Ok(reply);
            }
            // else simply discard
        }
    }
}

/// ControlHub control messenger task.
struct ControlHubMessengerTask {
    conn_read: OwnedReadHalf,
    conn_write: OwnedWriteHalf,

    tx_recv: mpsc::UnboundedSender<CtrlMsg>,
    read_buf: BytesMut,

    rx_send: mpsc::UnboundedReceiver<CtrlMsg>,
    write_buf: BytesMut,
    write_buf_cursor: usize,
    retrying: bool,
}

impl ControlHubMessengerTask {
    /// Creates the control messenger task.
    fn new(
        conn: TcpStream,
        tx_recv: mpsc::UnboundedSender<CtrlMsg>,
        rx_send: mpsc::UnboundedReceiver<CtrlMsg>,
    ) -> Self {
        let (conn_read, conn_write) = conn.into_split();

        let read_buf = BytesMut::new();
        let write_buf = BytesMut::new();
        let write_buf_cursor = 0;
        let retrying = false;

        ControlHubMessengerTask {
            conn_read,
            conn_write,
            tx_recv,
            read_buf,
            rx_send,
            write_buf,
            write_buf_cursor,
            retrying,
        }
    }

    /// Reads a manager control message from given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    async fn read_ctrl(
        // first 8 btyes being the message length, and the rest bytes being the
        // message itself
        read_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<CtrlMsg, SummersetError> {
        safe_tcp_read(read_buf, conn_read).await
    }

    /// Writes a control message through given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    fn write_ctrl(
        write_buf: &mut BytesMut,
        write_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        msg: Option<&CtrlMsg>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(write_buf, write_buf_cursor, conn_write, msg)
    }

    /// Starts the control messenger task loop.
    async fn run(&mut self) {
        pf_debug!("control_messenger task spawned");

        loop {
            tokio::select! {
                // gets a message to send to manager
                msg = self.rx_send.recv(), if !self.retrying => {
                    match msg {
                        Some(msg) => {
                            match Self::write_ctrl(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(&msg)
                            ) {
                                Ok(true) => {
                                    // pf_trace!("sent ctrl {:?}", msg);
                                }
                                Ok(false) => {
                                    pf_debug!("should start retrying ctrl send");
                                    self.retrying = true;
                                }
                                Err(_e) => {
                                    // NOTE: commented out to prevent console lags
                                    // during benchmarking
                                    // pf_error!("error sending ctrl: {}", e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // retrying last unsuccessful send
                _ = self.conn_write.writable(), if self.retrying => {
                    match Self::write_ctrl(
                        &mut self.write_buf,
                        &mut self.write_buf_cursor,
                        &self.conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("finished retrying last ctrl send");
                            self.retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("still should retry last ctrl send");
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!("error retrying last ctrl send: {}", e);
                        }
                    }
                },

                // receives control message from manager
                msg = Self::read_ctrl(&mut self.read_buf, &mut self.conn_read) => {
                    match msg {
                        Ok(msg) => {
                            // pf_trace!("recv ctrl {:?}", msg);
                            if let Err(e) = self.tx_recv.send(msg) {
                                pf_error!("error sending to tx_recv: {}", e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!("error reading ctrl: {}", e);
                            break; // probably the manager exitted ungracefully
                        }
                    }
                }
            }
        }

        pf_debug!("control_messenger task exitted");
    }
}

// Unit tests are done together with `manager::reigner`.
