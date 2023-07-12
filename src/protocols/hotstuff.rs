//! Replication protocol: HotStuff
//! TODO: add pacemaker for liveness
//! TODO: add storage hub back for persistence

use std::collections::{HashMap, HashSet};
// use std::path::Path;
use std::net::SocketAddr;

use crate::{RequestId, ReplicaMap, Command};
use crate::utils::SummersetError;
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, /*StorageHub, LogAction, LogResult, LogActionId,*/
    GenericReplica, TransportHub,
};
use crate::client::{ClientId, ClientApiStub, GenericClient};

use async_trait::async_trait;
use async_recursion::async_recursion;

use serde::{Serialize, Deserialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
struct Instance {
    view: ViewId,
    reqs: Node,
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
    nfaulty: u8,

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

    /// Requests that have been executed
    executed_req: HashSet<(ClientId, RequestId)>,

    /// Keeping track of requst bodies
    req_pool: HashMap<(ClientId, RequestId), Command>,

    /// In-memory log of instances.
    insts: HashMap<String, Instance>,

    /// List of pending proposals for future views
    pending_prop: HashMap<
        ViewId,
        (ReplicaId, Vec<(ClientId, RequestId)>, Option<QuorumCert>),
    >,

    /// List of pending votes if the votes arrived before the proposal
    pending_votes: HashMap<(ViewId, String), Vec<(ReplicaId, SignatureShare)>>,

    /// Log of insts
    log: Vec<String>,

    /// The next request to be executed
    exec_offset: (usize, usize),

    /// Public key
    pub_key: Option<PublicKey>,

    /// Public key set for combining signatures
    pub_key_set: Option<PublicKeySet>,

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

    /// Tries to continue executing commands in the log
    async fn continue_exec(&mut self) -> Result<(), SummersetError> {
        let (log_offset, mut req_offset) = self.exec_offset;
        for log_idx in log_offset..self.log.len() {
            let blk = match self.insts.get(&self.log[log_idx]) {
                Some(blk) => blk,
                None => {
                    self.exec_offset = (log_idx, 0);
                    return Ok(());
                }
            };
            for req_idx in req_offset..blk.reqs.len() {
                let (client, req) = blk.reqs[req_idx];
                if !self.executed_req.contains(&(client, req)) {
                    match self.req_pool.get(&(client, req)) {
                        Some(cmd) => {
                            self.state_machine
                                .as_mut()
                                .unwrap()
                                .submit_cmd(
                                    Self::make_command_id(log_idx, req_idx),
                                    cmd.clone(),
                                )
                                .await?;
                            self.executed_req.insert((client, req));
                        }
                        None => {
                            self.exec_offset = (log_idx, req_idx);
                            return Ok(());
                        }
                    }
                }
            }
            req_offset = 0;
        }
        self.exec_offset = (self.log.len(), 0);
        Ok(())
    }

    /// Check if it is safe to vote for the proposal
    async fn verify_proposal(
        &mut self,
        peer: ReplicaId,
        view: ViewId,
        justify: &Option<QuorumCert>,
    ) -> Result<(), SummersetError> {
        match self.locked_qc.as_ref() {
            Some(locked) => {
                if justify.is_none() {
                    return logged_err!(self.id; "no justify supplied for replica {}'s proposal; current view is {}. ", peer, view);
                }

                let qc = justify.as_ref().unwrap();
                if !self.pub_key.unwrap().verify(&qc.sign, &qc.hash) {
                    return logged_err!(self.id; "the quorum certificate does not match block {}. ", &qc.hash);
                }

                let parent_blk = match justify
                    .as_ref()
                    .map(|qc| &qc.hash)
                    .and_then(|hash| self.insts.get(hash))
                {
                    Some(blk) => blk,
                    None => {
                        // TODO: fetch the parent block from other replicas
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
                        if parent.view <= locked_blk.view
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

        Ok(())
    }

    /// Stores the proposal message, including its justify and request batch
    /// blk_hash is passed here to avoid expensive repeated computation
    async fn store_proposal(
        &mut self,
        view: ViewId,
        blk_hash: &String,
        proposal: Vec<(ClientId, RequestId)>,
        justify: &Option<QuorumCert>,
    ) -> Result<(), SummersetError> {
        // store the signature for new view
        let parent_blk_hash = justify.as_ref().map(|qc| &qc.hash);
        let parent_blk =
            parent_blk_hash.and_then(|hash| self.insts.get_mut(hash));
        if let Some(parent) = parent_blk {
            assert!(parent.sign.is_none());
            parent.sign = justify.as_ref().map(|qc| qc.sign.clone());
        }

        // store the proposal
        self.insts.insert(
            blk_hash.clone(),
            Instance {
                view,
                execed: vec![false; proposal.len()],
                reqs: proposal,
                sign: None,
                parent: parent_blk_hash.cloned(),
            },
        );

        Ok(())
    }

    /// Record locally the given vote
    /// If we have received enough votes, do the next round of proposal
    async fn store_vote(
        &mut self,
        peer: ReplicaId,
        view: ViewId,
        part_qc: PartQuorumCert,
    ) -> Result<(), SummersetError> {
        let votes = self
            .pending_votes
            .entry((view, part_qc.hash.clone()))
            .or_insert(vec![]);
        votes.push((peer, part_qc.sign));

        if view > self.cur_view {
            pf_debug!(self.id; "got replica {}'s vote for future view {}; current view is {}", peer, view, self.cur_view);
            return Ok(());
        }

        if self.insts.get(&part_qc.hash).is_none() {
            pf_debug!(self.id; "got vote before proposal for block {}", part_qc.hash);
            return Ok(());
        }

        if votes.len() > 2 * self.nfaulty as usize {
            let combined_sign = self
                .pub_key_set
                .as_ref()
                .unwrap()
                .combine_signatures(
                    votes.iter().map(|(peer, sign)| (*peer as u64, sign)),
                )
                .unwrap();
            let qc = QuorumCert {
                hash: part_qc.hash.clone(),
                sign: combined_sign,
            };
            self.pending_votes.remove(&(view, part_qc.hash.clone()));
            let blk_hash = self.do_propose(Some(qc)).await?;
            self.do_vote(view, &blk_hash, Some(part_qc.hash)).await?;
        }

        Ok(())
    }

    /// Send vote message AND update generic and locked qc, as well as commit
    /// the great grand parent block
    #[async_recursion]
    async fn do_vote(
        &mut self,
        view: ViewId,
        blk_hash: &String,
        parent_blk_hash: Option<String>,
    ) -> Result<(), SummersetError> {
        assert!(view == self.cur_view);

        let grand_parent_blk_hash = parent_blk_hash
            .as_ref()
            .and_then(|hash| self.insts.get(hash))
            .and_then(|blk| blk.parent.clone());
        let great_grand_parent_blk_hash = grand_parent_blk_hash
            .as_ref()
            .and_then(|hash| self.insts.get(hash))
            .and_then(|blk| blk.parent.clone());

        // Send vote message to the next leader
        let sign = self.sec_key_share.as_ref().unwrap().sign(&blk_hash);
        let part_qc = PartQuorumCert {
            hash: blk_hash.clone(),
            sign,
        };

        self.cur_view += 1;
        let next_leader = (self.cur_view % self.population as u64) as u8;
        if next_leader == self.id {
            self.store_vote(self.id, self.cur_view, part_qc).await?;
        } else {
            let mut target = ReplicaMap::new(self.population, false)?;
            target.set(next_leader, true)?;
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
        }

        // generic_qc <- parent block
        // locked_qc <- grand parent block
        // commit great grand parent block

        // this could happen e.g. if this proposal is from a new view
        if parent_blk_hash == self.generic_qc {
            return Ok(());
        }

        // update generic qc and lock qc
        self.generic_qc = parent_blk_hash;
        self.locked_qc = grand_parent_blk_hash;

        // execute great grand parent
        if let Some(hash) = great_grand_parent_blk_hash {
            self.log.push(hash.clone());
            // requests will be committed, remove them from pending queue
            if let Some(blk) = self.insts.get(&hash) {
                for req in blk.reqs.iter() {
                    self.pending_req.remove(req);
                }
            }
            self.continue_exec().await?;
        }

        // We are in the new view. If the proposal for the current view has been received, vote for it.
        if let Some((peer, prop, justify)) =
            self.pending_prop.remove(&self.cur_view)
        {
            self.handle_propose_msg(peer, self.cur_view, prop, justify)
                .await?
        }

        Ok(())
    }

    async fn do_propose(
        &mut self,
        justify: Option<QuorumCert>,
    ) -> Result<String, SummersetError> {
        let proposal: Vec<(ClientId, RequestId)> =
            self.pending_req.drain().collect();
        let blk_hash = sha256::digest(
            serde_json::to_string(&(self.cur_view, &proposal)).unwrap(),
        );

        pf_info!(self.id; "proposing new block {}", blk_hash);

        self.store_proposal(
            self.cur_view,
            &blk_hash,
            proposal.clone(),
            &justify,
        )
        .await?;

        // broadcast to others
        let mut target = ReplicaMap::new(self.population, true)?;
        target.set(self.id, false)?;
        self.transport_hub
            .as_mut()
            .unwrap()
            .send_msg(
                QuorumMsg::Propose {
                    view: self.cur_view,
                    proposal,
                    justify,
                },
                target,
            )
            .await?;

        Ok(blk_hash)
    }

    async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        for (client, req) in req_batch {
            match req {
                ApiRequest::Req { id, cmd } => {
                    if !self.executed_req.contains(&(client, id)) {
                        self.pending_req.insert((client, id));
                    }
                    self.req_pool.insert((client, id), cmd);
                }
                ApiRequest::Leave => {
                    pf_error!(self.id; "unexpecte client request");
                }
            }
        }
        self.continue_exec().await?;
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
        if view % self.population as ViewId != peer as ViewId {
            return logged_err!(self.id; "got proposal from non leader {}, current view is {}", peer, view);
        }

        if view < self.cur_view {
            return logged_err!(self.id; "got replica {}'s proposal from previous view {}; current view is {}", peer, view, self.cur_view);
        } else if view > self.cur_view {
            pf_debug!(self.id; "got replica {}'s proposal for future view {}; current view is {}", peer, view, self.cur_view);
            self.pending_prop.insert(view, (peer, proposal, justify));
            return Ok(());
        }

        let validity = self.verify_proposal(peer, view, &justify).await;
        if validity.is_err() {
            return validity;
        }

        let blk_hash =
            sha256::digest(serde_json::to_string(&(view, &proposal)).unwrap());

        let parent_blk_hash = justify.as_ref().map(|qc| qc.hash.clone());

        self.store_proposal(view, &blk_hash, proposal, &justify)
            .await?;

        self.do_vote(view, &blk_hash, parent_blk_hash).await?;

        Ok(())
    }

    async fn handle_vote_msg(
        &mut self,
        peer: ReplicaId,
        view: ViewId,
        part_qc: PartQuorumCert,
    ) -> Result<(), SummersetError> {
        if view % self.population as ViewId != self.id as ViewId {
            return logged_err!(self.id; "got vote for another leader from replica {}, the view is {}", peer, view);
        }

        if view < self.cur_view {
            pf_debug!(self.id; "got replica {}'s vote from previous view {}; current view is {}", peer, view, self.cur_view);
            return Ok(());
        }

        if let Some(blk) = self.insts.get(&part_qc.hash) {
            if blk.sign.is_some() {
                // we already have enough signatures, skipping
                return Ok(());
            }
        }

        if !self
            .pub_key_shares
            .as_ref()
            .unwrap()
            .get(&peer)
            .unwrap()
            .verify(&part_qc.sign, &part_qc.hash)
        {
            return logged_err!(self.id; "got invalid vote for block {} from replica {}", part_qc.hash, peer);
        }

        self.store_vote(peer, view, part_qc).await?;

        Ok(())
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
        let (log_idx, cmd_idx) = Self::split_command_id(cmd_id);
        if log_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, log_idx, cmd_idx);
        }

        let inst_hash = &self.log[log_idx];
        let inst = self.insts.get_mut(inst_hash).unwrap();
        if cmd_idx >= inst.reqs.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, log_idx, cmd_idx);
        }
        if inst.execed[cmd_idx] {
            return logged_err!(self.id; "duplicate command index {}|{}", log_idx, cmd_idx);
        }
        // if !inst.durable {
        //     return logged_err!(self.id; "instance {} is not durable yet", inst_idx);
        // }
        inst.execed[cmd_idx] = true;

        let (client, req_id) = &inst.reqs[cmd_idx]; 
        pf_info!(self.id; "Sending reply for {}-{}: {:?}", client, req_id, cmd_result);
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
            nfaulty: (population - 1) / 3,
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
            executed_req: HashSet::new(),
            req_pool: HashMap::new(),
            insts: HashMap::new(),
            pending_prop: HashMap::new(),
            pending_votes: HashMap::new(),
            log: vec![],
            exec_offset: (0, 0),
            pub_key: None,
            pub_key_set: None,
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
                SecretKeySet::random(2 * self.nfaulty as usize, &mut rng)
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
        self.pub_key_set = Some(pk);
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
        if self.id == 0 {
            match self.do_propose(None).await {
                Ok(blk_hash) => {
                    if let Err(e) =
                        self.do_vote(self.cur_view, &blk_hash, None).await
                    {
                        pf_error!(self.id; "error voting: {}", e);
                    }
                }
                Err(e) => {
                    pf_error!(self.id; "error proposing: {}", e);
                }
            }
        }

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
pub struct ClientConfigHotStuff {
    pub chan_cap: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigHotStuff {
    fn default() -> Self {
        ClientConfigHotStuff { chan_cap: 10000 }
    }
}

/// HotStuff client-side module.
pub struct HotStuffClient {
    /// Client ID.
    id: ClientId,

    /// Number of faulty nodes the cluster it can tolerate
    nfaulty: usize,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigHotStuff,

    /// Send stubs for communicating with servers
    tx_sends: HashMap<ReplicaId, mpsc::Sender<ApiRequest>>,

    /// Receiver side of the receive channel
    rx_recv: Option<mpsc::Receiver<(ReplicaId, ApiReply)>>,

    /// Server listening threads
    server_messager_handles: HashMap<ReplicaId, JoinHandle<()>>,

    /// Reply pool
    reply_pool: HashMap<RequestId, HashMap<String, usize>>,
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

        // TODO: add configurations as needed
        let config = parsed_config!(config_str => ClientConfigHotStuff;
            chan_cap)?;

        Ok(HotStuffClient {
            id,
            nfaulty: (servers.len() - 1) / 3,
            servers,
            config,
            tx_sends: HashMap::new(),
            rx_recv: None,
            server_messager_handles: HashMap::new(),
            reply_pool: HashMap::new(),
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        let (tx_recv, rx_recv) = mpsc::channel(self.config.chan_cap);
        self.rx_recv = Some(rx_recv);

        for (server, addr) in self.servers.iter() {
            let (tx_send, rx_send) = mpsc::channel(self.config.chan_cap);
            let messager_handle = tokio::spawn(Self::server_messager(
                self.id,
                *server,
                *addr,
                rx_send,
                tx_recv.clone(),
            ));
            self.tx_sends.insert(*server, tx_send);
            self.server_messager_handles
                .insert(*server, messager_handle);
        }

        Ok(())
    }

    async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        if let ApiRequest::Req { id, cmd: _cmd } = &req {
            if self.reply_pool.contains_key(&id) {
                return logged_err!(self.id; "another request with the id {} already exists", id);
            }
            self.reply_pool.insert(*id, HashMap::new());
        }

        for (server, tx_send) in self.tx_sends.iter() {
            if let Err(e) = tx_send.send(req.clone()).await {
                return logged_err!(self.id; "failed to send request to sever message thread {}: {}", server, e);
            }
        }
        Ok(())
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        loop {
            match self.rx_recv.as_mut().unwrap().recv().await {
                Some((server, reply)) => match &reply {
                    ApiReply::Reply { id, result } => {
                        match self.reply_pool.get_mut(&id) {
                            Some(replies) => {
                                let hash = sha256::digest(
                                    serde_json::to_string(result).unwrap(),
                                );
                                let votes = replies.entry(hash).or_insert(0);
                                *votes += 1;
                                if *votes > 2 * self.nfaulty {
                                    // We have got enough votes
                                    self.reply_pool.remove(id);
                                    return Ok(reply);
                                }
                            }
                            None => {
                                pf_debug!(self.id; "got reply for request that has returned");
                            }
                        }
                    }
                    ApiReply::Leave => {
                        match self.tx_sends.remove(&server).and_then(|_| {
                            self.server_messager_handles.remove(&server)
                        }) {
                            Some(join_handle) => {
                                if let Err(e) = join_handle.await {
                                    return logged_err!(self.id; "failed to wait for server messager thread {} to complete: {}", server, e);
                                }
                                if self.tx_sends.is_empty() {
                                    // We have disconnected from all servers
                                    return Ok(reply);
                                }
                            }
                            None => {
                                return logged_err!(self.id; "unexpected leave message from disconnected server {}", server);
                            }
                        }
                    }
                },
                None => {
                    return logged_err!(self.id; "receive channel closed unexpectedly");
                }
            }
        }
    }
}

/// Server communication thread
impl HotStuffClient {
    async fn server_messager(
        me: ClientId,
        server: ReplicaId,
        addr: SocketAddr,
        mut rx_send: mpsc::Receiver<ApiRequest>,
        tx_recv: mpsc::Sender<(ReplicaId, ApiReply)>,
    ) {
        let api_stub = ClientApiStub::new(me);
        let (mut send_stub, mut recv_stub) = match api_stub.connect(addr).await
        {
            Ok(stubs) => stubs,
            Err(e) => {
                pf_error!(me; "failed to connec to server {} at {}: {}", server, addr, e);
                return;
            }
        };

        loop {
            tokio::select! {
                msg = rx_send.recv() => {
                    match msg {
                        Some(req) => {
                            if let Err(e) = send_stub.send_req(req).await {
                                pf_error!(me; "error sending request to server {}: {}", server, e);
                            }
                        },
                        None => break,
                    }
                },

                msg = recv_stub.recv_reply() => {
                    match msg {
                        Ok(reply) => {
                            if let Err(e) = tx_recv.send((server, reply)).await {
                                pf_error!(me; "error sending reply back to main thread: {}", e);
                            }
                        },
                        Err(e) => {
                            pf_error!(me; "error receiving reply from server {}: {}", server, e);
                            break;
                        }
                    }
                }
            }
        }
    }
}
