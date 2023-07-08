//! Replication protocol: HotStuff

use std::collections::{HashMap, BTreeMap, HashSet};
use std::path::Path;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::{RequestId, ReplicaMap, Command};
use crate::utils::SummersetError;
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, /*StorageHub, LogAction,*/ LogResult, LogActionId, GenericReplica,
    TransportHub,
};
use crate::client::{
    ClientId, ClientApiStub, ClientSendStub, ClientRecvStub, GenericClient,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

use threshold_crypto::{
    SecretKeySet, PublicKeyShare, SecretKeyShare, serde_impl, PublicKeySet,
    SignatureShare, Signature, PublicKey,
};

pub type ViewId = u64;
pub type Node = Vec<(ClientId, RequestId)>;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigHotStuff {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Path to backing file.
    pub backer_path: String,

    /// Base capacity for most channels.
    pub base_chan_cap: usize,

    /// Capacity for req/reply channels.
    pub api_chan_cap: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigHotStuff {
    fn default() -> Self {
        ReplicaConfigHotStuff {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.hotstuff.wal".into(),
            base_chan_cap: 1000,
            api_chan_cap: 10000,
        }
    }
}

/// Log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct LogEntry {
    reqs: Vec<(ClientId, ApiRequest)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuorumCert {
    hash: String,
    sign: Signature,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartQuorumCert {
    hash: String, 
    sign: SignatureShare,
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum QuorumMsg {
    Propose {
        view: ViewId,
        proposal: Node,
        justify: Option<QuorumCert>,
    },
    Vote {
        view: ViewId,
        part_qc: PartQuorumCert,
    },
    NewView {
        view: ViewId,
        generic_qc: Option<QuorumCert>,
    },
    // For distributing threshold key
    ThresholdKey {
        pk: PublicKeySet,
        sk: serde_impl::SerdeSecret<SecretKeyShare>,
    },
    ThresholdKeyReply,
    // If did not receive the request body from client, get it from peers
    FetchRequest {
        request: Vec<(ClientId, RequestId)>,
    },
    DeliverRequest {
        command: Vec<(ClientId, ApiRequest, String)>,
    },
}

/// In-memory instance containing a commands batch.
// TODO: add encryption
struct Instance {
    view: ViewId,
    reqs: Node,
    height: u64,
    votes: BTreeMap<ReplicaId, SignatureShare>,
    sign: Option<Signature>,
    parent: Option<String>,
    // durable: bool,
    execed: Vec<bool>,
}

/// HotStuff server replica module
pub struct HotStuffReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Maximum number of faulty nodes
    faulty: u8,

    /// Address string for peer-to-peer connections.
    smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Configuraiton parameters struct.
    config: ReplicaConfigHotStuff,

    /// Map from peer replica ID -> address.
    peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: Option<ExternalApi>,

    /// StateMachine module.
    state_machine: Option<StateMachine>,

    // /// StorageHub module.
    // storage_hub: Option<StorageHub<LogEntry>>,
    /// TransportHub module.
    transport_hub: Option<TransportHub<QuorumMsg>>,

    /// Current view number
    cur_view: ViewId,

    /// Requests to be processed
    pending_req: HashSet<(ClientId, RequestId)>,

    /// Keeping track of requst bodies
    req_pool: HashMap<(ClientId, RequestId), Command>,

    /// In-memory log of instances.
    insts: HashMap<String, Instance>,

    /// Log of insts
    log: Vec<String>,

    /// Current durable log file offset.
    log_offset: usize,

    /// Public key
    pub_key: Option<PublicKey>,

    /// Pub key share for verifying individual digests
    pub_key_shares: Option<HashMap<ReplicaId, PublicKeyShare>>,

    /// Secrete key
    sec_key_share: Option<SecretKeyShare>,

    /// Generic QC
    generic_qc: Option<String>,

    /// Locked QC
    locked_qc: Option<String>,
}

impl HotStuffReplica {
    fn make_command_id(inst_idx: usize, cmd_idx: usize) -> CommandId {
        assert!(inst_idx <= (u32::MAX as usize));
        assert!(cmd_idx <= (u32::MAX as usize));
        (inst_idx << 32 | cmd_idx) as CommandId
    }

    /// Decompose CommandId into instance index & command index within.
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let inst_idx = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (inst_idx, cmd_idx)
    }

    async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        for (client, req) in req_batch {
            match req {
                ApiRequest::Req { id, cmd } => {
                    self.pending_req.insert((client, id));
                    self.req_pool.insert((client, id), cmd);
                }
                ApiRequest::Leave => {
                    pf_error!(self.id; "unexpecte client request")
                }
            }
        }
        Ok(())
    }

    // async fn handle_log_result(
    //     &mut self,
    //     action_id: LogActionId,
    //     log_result: LogResult<LogEntry>,
    // ) -> Result<(), SummersetError> {
    //     todo!();
    // }

    async fn handle_propose_msg(
        &mut self,
        peer: ReplicaId,
        view: ViewId,
        proposal: Vec<(ClientId, RequestId)>,
        justify: Option<QuorumCert>,
    ) -> Result<(), SummersetError> {
        assert!(view % self.population as ViewId == peer as ViewId);

        if view < self.cur_view {
            return logged_err!(self.id; "got replica {}'s proposal from previous view {}; current view is {}", peer, view, self.cur_view);
        }

        let parent_blk_hash = justify.as_ref().map(|qc| &qc.hash);
        let parent_blk = parent_blk_hash.and_then(|hash| self.insts.get(hash));

        match self.locked_qc.as_ref() {
            Some(locked) => {
                if justify.is_none() {
                    return logged_err!(self.id; "no justify supplied for replica {}'s proposal; current view is {}. ", peer, view);
                }

                let qc = justify.as_ref().unwrap();
                if !self.pub_key.unwrap().verify(&qc.sign, &qc.hash) {
                    return logged_err!(self.id; "the quorum certificate does not match block {}. ", &qc.hash);
                }

                let parent_blk = match parent_blk {
                    Some(blk) => blk,
                    None => {
                        return logged_err!(self.id; "cannot verify qc since we do not have its parent block {}. ", qc.hash);
                    }
                };

                let locked_blk = self.insts.get(locked).unwrap();
                if parent_blk.view > locked_blk.view {
                    // liveness condition
                } else {
                    // safety condition
                    let mut hash = &qc.hash;
                    loop {
                        if hash == locked {
                            // this block extends from locked qc
                            break;
                        }
                        let parent = self.insts.get(hash).unwrap();
                        if parent.height > locked_blk.height
                            || parent.parent.is_none()
                        {
                            // this block does not extend from locked qc
                            return logged_err!(self.id; "replica {}'s proposed block is not safe to commit. ", peer);
                        }
                        hash = parent.parent.as_ref().unwrap();
                    }
                }
            }
            None => {
                if justify.as_ref().is_some_and(|qc| {
                    !self.pub_key.unwrap().verify(&qc.sign, &qc.hash)
                }) {
                    return logged_err!(self.id; "the quorum certificate does not match block {}. ", justify.as_ref().unwrap().hash);
                }
            }
        }

        // placed here to avoid extra look up for parent_blk after insert
        let grand_parent_blk_hash =
            parent_blk.and_then(|blk| blk.parent.as_ref()).cloned();

        // vote for this message
        let blk_hash =
            sha256::digest(serde_json::to_string(&proposal).unwrap());
        let sign = self.sec_key_share.as_ref().unwrap().sign(&blk_hash);
        let part_qc = PartQuorumCert { hash: blk_hash.clone(), sign };

        self.insts.insert(
            blk_hash,
            Instance {
                view,
                execed: vec![false; proposal.len()],
                reqs: proposal,
                height: parent_blk.map_or_else(|| 0, |blk| blk.height + 1),
                votes: BTreeMap::new(),
                sign: None,
                parent: parent_blk_hash.cloned(),
            },
        );

        let great_grand_parent_blk_hash = grand_parent_blk_hash
            .as_ref()
            .and_then(|hash| self.insts.get(hash))
            .and_then(|blk| blk.parent.as_ref());
        let great_grand_parent_blk =
            great_grand_parent_blk_hash.and_then(|hash| self.insts.get(hash));

        self.cur_view += 1;
        let mut target = ReplicaMap::new(self.population, false)?;
        target.set((self.cur_view % self.population as u64) as u8, true)?;
        self.transport_hub
            .as_mut()
            .unwrap()
            .send_msg(
                QuorumMsg::Vote {
                    view: self.cur_view,
                    part_qc,
                },
                target,
            )
            .await?;

        // update generic qc and lock qc
        self.generic_qc = parent_blk_hash.cloned();
        self.locked_qc = grand_parent_blk_hash;

        // execute great grand parent
        if let Some(exec_blk) = great_grand_parent_blk {
            self.log.insert(
                self.log_offset,
                great_grand_parent_blk_hash.unwrap().clone(),
            );
            for (idx, (client_id, req_id)) in exec_blk.reqs.iter().enumerate() {
                let id = (*client_id, *req_id);
                // dedup
                if self.pending_req.remove(&id) {
                    match self.req_pool.remove(&(*client_id, *req_id)) {
                        Some(cmd) => {
                            self.state_machine
                                .as_mut()
                                .unwrap()
                                .submit_cmd(
                                    Self::make_command_id(self.log_offset, idx),
                                    cmd.clone(),
                                )
                                .await?
                        }
                        // TODO: fetch request from others
                        None => {
                            pf_error!(self.id; "unrecognized request ({}, {}), skipping...", client_id, req_id)
                        }
                    }
                }
            }
        } else {
            if great_grand_parent_blk.is_some() {
                // TODO: fetch block from others
                pf_error!(self.id; "missing block {:?}", great_grand_parent_blk_hash);
            }
        }

        Ok(())
    }

    async fn handle_vote_msg(
        &mut self,
        peer: ReplicaId,
        view: ViewId,
        part_qc: PartQuorumCert,
    ) -> Result<(), SummersetError> {
        
        todo!();
    }

    async fn handle_fetch_block(&self) -> Result<(), SummersetError> {
        todo!();
    }

    async fn handle_deliver_block(&self) -> Result<(), SummersetError> {
        todo!();
    }

    async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (inst_idx, cmd_idx) = Self::split_command_id(cmd_id);
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }

        let inst_hash = &self.log[inst_idx];
        let inst = self.insts.get_mut(inst_hash).unwrap();
        if cmd_idx >= inst.reqs.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }
        if inst.execed[cmd_idx] {
            return logged_err!(self.id; "duplicate command index {}|{}", inst_idx, cmd_idx);
        }
        // if !inst.durable {
        //     return logged_err!(self.id; "instance {} is not durable yet", inst_idx);
        // }
        inst.execed[cmd_idx] = true;

        let req_key = &inst.reqs[cmd_idx];
        let (client, req_id) = req_key;
        self.external_api
            .as_mut()
            .unwrap()
            .send_reply(
                ApiReply::Reply {
                    id: *req_id,
                    result: cmd_result,
                },
                *client,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl GenericReplica for HotStuffReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if population == 0 {
            return Err(SummersetError(format!(
                "invalid population {}",
                population
            )));
        }
        if id >= population {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                id, population
            )));
        }
        if smr_addr == api_addr {
            return logged_err!(
                id;
                "smr_addr and api_addr are the same '{}'",
                smr_addr
            );
        }

        let config = parsed_config!(config_str => ReplicaConfigHotStuff;
            batch_interval_us, backer_path,
            base_chan_cap, api_chan_cap)?;

        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
            );
        }
        if config.base_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.base_chan_cap {}",
                config.base_chan_cap
            );
        }
        if config.api_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.api_chan_cap {}",
                config.api_chan_cap
            );
        }

        Ok(HotStuffReplica {
            id,
            population,
            faulty: (population - 1) / 3,
            smr_addr,
            api_addr,
            config,
            peer_addrs,
            external_api: None,
            state_machine: None,
            // storage_hub: None,
            transport_hub: None,
            cur_view: 0,
            pending_req: HashSet::new(),
            req_pool: HashMap::new(),
            insts: HashMap::new(),
            log: vec![],
            log_offset: 0,
            pub_key: None,
            pub_key_shares: None,
            sec_key_share: None,
            generic_qc: None,
            locked_qc: None,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        let mut state_machine = StateMachine::new(self.id);
        state_machine
            .setup(self.config.api_chan_cap, self.config.api_chan_cap)
            .await?;
        self.state_machine = Some(state_machine);

        // let mut storage_hub = StorageHub::new(self.id);
        // storage_hub
        //     .setup(
        //         Path::new(&self.config.backer_path),
        //         self.config.base_chan_cap,
        //         self.config.base_chan_cap,
        //     )
        //     .await?;
        // self.storage_hub = Some(storage_hub);

        let mut transport_hub = TransportHub::new(self.id, self.population);
        transport_hub
            .setup(
                self.smr_addr,
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        if !self.peer_addrs.is_empty() {
            transport_hub.group_connect(&self.peer_addrs).await?;
        }

        // Set up threshold encryption
        let (pk, sk) = if self.id == 0 {
            let sk_set = {
                let mut rng = rand::thread_rng();
                SecretKeySet::random(2 * self.faulty as usize, &mut rng)
            };
            let pk_set = sk_set.public_keys();

            let my_sk_share = sk_set.secret_key_share(0);

            for id in 1..self.population {
                let mut target = ReplicaMap::new(self.population, false)?;
                target.set(id, true)?;
                transport_hub
                    .send_msg(
                        QuorumMsg::ThresholdKey {
                            pk: pk_set.clone(),
                            sk: serde_impl::SerdeSecret(
                                sk_set.secret_key_share(id as usize),
                            ),
                        },
                        target,
                    )
                    .await?;
                let (peer, msg) = transport_hub.recv_msg().await?;
                assert!(
                    peer == id && matches!(msg, QuorumMsg::ThresholdKeyReply)
                );
            }

            (pk_set, my_sk_share)
        } else {
            let (peer, msg) = transport_hub.recv_msg().await?;
            assert!(peer == 0);

            match msg {
                QuorumMsg::ThresholdKey { pk, sk } => {
                    let mut target = ReplicaMap::new(self.population, false)?;
                    target.set(peer, true)?;
                    transport_hub
                        .send_msg(QuorumMsg::ThresholdKeyReply, target)
                        .await?;
                    (pk, sk.into_inner())
                }
                _ => unreachable!(),
            }
        };

        let pk_shares = (0..self.population)
            .map(|id| (id, pk.public_key_share(id as usize)))
            .collect::<HashMap<ReplicaId, PublicKeyShare>>();

        self.transport_hub = Some(transport_hub);
        self.pub_key = Some(pk.public_key());
        self.pub_key_shares = Some(pk_shares);
        self.sec_key_share = Some(sk);

        let mut external_api = ExternalApi::new(self.id);
        external_api
            .setup(
                self.api_addr,
                Duration::from_micros(self.config.batch_interval_us),
                self.config.api_chan_cap,
                self.config.api_chan_cap,
            )
            .await?;
        self.external_api = Some(external_api);

        pf_info!(self.id; "ready to run");
        Ok(())
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.as_mut().unwrap().get_req_batch() => {
                    if let Err(e) = req_batch {
                        pf_error!(self.id; "error getting req batch: {}", e);
                        continue;
                    }
                    let req_batch = req_batch.unwrap();
                    if let Err(e) = self.handle_req_batch(req_batch).await {
                        pf_error!(self.id; "error handling req batch: {}", e);
                    }
                },

                // durable logging result
                // log_result = self.storage_hub.as_mut().unwrap().get_result() => {
                //     if let Err(e) = log_result {
                //         pf_error!(self.id; "error getting log result: {}", e);
                //         continue;
                //     }
                //     let (action_id, log_result) = log_result.unwrap();
                //     if let Err(e) = self.handle_log_result(action_id, log_result).await {
                //         pf_error!(self.id; "error handling log result {}: {}", action_id, e);
                //     }
                // },

                // message from peer
                msg = self.transport_hub.as_mut().unwrap().recv_msg() => {
                    if let Err(e) = msg {
                        pf_error!(self.id; "error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    match msg {
                        QuorumMsg::Propose { view, proposal, justify } => {
                            if let Err(e) = self.handle_propose_msg(peer, view, proposal, justify).await {
                                pf_error!(self.id; "error handling peer proposal: {}", e);
                            }
                        },
                        QuorumMsg::Vote { view, part_qc } => {
                            if let Err(e) = self.handle_vote_msg(peer, view, part_qc).await {
                                pf_error!(self.id; "error handling peer vote: {}", e);
                            }
                        },
                        QuorumMsg::FetchRequest {..} => {
                            if let Err(e) = self.handle_fetch_block().await {
                                pf_error!(self.id; "error handling peer fetch block: {}", e);
                            }
                        },
                        QuorumMsg::DeliverRequest {..} => {
                            if let Err(e) = self.handle_deliver_block().await {
                                pf_error!(self.id; "error handling peer deliver block: {}", e);
                            }
                        }
                        _ => {
                            pf_error!(self.id; "got unexpected message: {:?}", msg);
                        }
                    }

                }

                // state machine execution result
                cmd_result = self.state_machine.as_mut().unwrap().get_result() => {
                    if let Err(e) = cmd_result {
                        pf_error!(self.id; "error getting cmd result: {}", e);
                        continue;
                    }
                    let (cmd_id, cmd_result) = cmd_result.unwrap();
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result).await {
                        pf_error!(self.id; "error handling cmd result {}: {}", cmd_id, e);
                    }
                },
            }
        }
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigHotStuff {}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigHotStuff {
    fn default() -> Self {
        ClientConfigHotStuff {}
    }
}

/// HotStuff client-side module.
pub struct HotStuffClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigHotStuff,

    /// Stubs for communicating with servers.
    stubs: HashMap<ReplicaId, (ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericClient for HotStuffClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        // let config = parsed_config!(config_str => ClientConfigHotStuff; )?;
        // TODO:
        let config = ClientConfigHotStuff {};

        Ok(HotStuffClient {
            id,
            servers,
            config,
            stubs: HashMap::new(),
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        // TODO: make this concurrent
        // for (id, addr) in self.servers.iter() {

        // }
        todo!();
        // let api_stub = ClientApiStub::new(self.id);
        // api_stub.connect(self.servers[&self.config.server_id]).await
    }

    async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        todo!();
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        todo!();
    }
}
