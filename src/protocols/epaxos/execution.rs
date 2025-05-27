//! EPaxos -- command execution.
//!
//! NOTE: since Summerset Put commands always return the old value, they are
//!       effectively RMW commands; thus the commit vs. execute distinction in
//!       original EPaxos paper does not make a difference here. Put commands
//!       get replied to client only after execution. This does not make any
//!       difference to the performance metrics that we are interested in.

use super::*;

use std::collections::VecDeque;

use crate::server::{ApiReply, ApiRequest};
use crate::utils::SummersetError;

use petgraph::algo::tarjan_scc;
use petgraph::graphmap::DiGraphMap;

// EPaxosReplica state machine execution
impl EPaxosReplica {
    /// Attempt the dependency graph-based execution algorithm on the given
    /// instance, submitting all commands in its dependency graph in the
    /// correct order if eligible. If `sync_exec` is true, executes those
    /// commands synchronously in place. Returns whether execution was carried
    /// out or not.
    pub(super) async fn attempt_execution(
        &mut self,
        tail_slot: SlotIdx,
        sync_exec: bool,
    ) -> Result<bool, SummersetError> {
        // build dependency graph, doing pruning and checking along the way
        let mut dep_graph = DiGraphMap::<SlotIdx, ()>::new();
        let mut dep_queue = VecDeque::new();
        dep_queue.push_back(tail_slot);
        let mut last_slot = None;

        while let Some(slot) = dep_queue.pop_front() {
            let (row, col) = slot.unpack();
            debug_assert!(row < self.population as usize);

            if col >= self.commit_bars[row] {
                // dependency not committed; can't proceed to execution
                pf_trace!("execution attempt aborted due to dep slot {}", slot);
                return Ok(false);
            }
            if col < self.start_col
                || self.insts[row][col - self.start_col].status
                    >= Status::Executing
            {
                // already submitted for execution, can prune it and all its
                // transitive dependencies
            } else if dep_graph.contains_node(slot) {
                // already added to dependency graph (dependencies likely have
                // cyclic loops and it's normal)
            } else {
                // add to dependency graph
                dep_graph.add_node(slot);
                if let Some(last_slot) = last_slot {
                    dep_graph.add_edge(last_slot, slot, ());
                }

                // push its dependencies to queue
                for (r, c) in self.insts[row][col - self.start_col]
                    .deps
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.is_some())
                {
                    dep_queue.push_back(SlotIdx(r as ReplicaId, c.unwrap()));
                }
                if col > self.start_col {
                    // don't forget that R.c implicitly depends on R.(c-1)
                    dep_queue.push_back(SlotIdx(row as ReplicaId, col - 1));
                }
            }
            last_slot = Some(slot);
        }
        let dep_graph = dep_graph.into_graph::<usize>();

        // use Tarjan's SCC algorithm to find the condensed graph where each
        // super-node is an SCC in original graph; the library's implementation
        // nicely returns the condensed graph in reverse topological order
        // which is exactly what we need
        let scc_list = tarjan_scc(&dep_graph);
        pf_trace!(
            "execution attempt passed: |dep_graph| {} |scc_list| {}",
            dep_graph.node_count(),
            scc_list.len()
        );
        for mut scc in scc_list {
            // sort instances in each SCC in seq order
            scc.sort_by_key(|&n| {
                let (row, col) = dep_graph[n].unpack();
                self.insts[row][col - self.start_col].seq
            });

            // then execute one-by-one
            for n in scc {
                let (row, col) = dep_graph[n].unpack();
                for (cmd_idx, (_, req)) in self.insts[row][col - self.start_col]
                    .reqs
                    .iter()
                    .enumerate()
                {
                    if let ApiRequest::Req { cmd, .. } = req {
                        let cmd_id =
                            Self::make_command_id(dep_graph[n], cmd_idx);
                        if !sync_exec {
                            self.state_machine
                                .submit_cmd(cmd_id, cmd.clone())?;
                        } else {
                            self.state_machine
                                .do_sync_cmd(cmd_id, cmd.clone())
                                .await?;
                        }
                    }
                }

                if !sync_exec {
                    self.insts[row][col - self.start_col].status =
                        Status::Executing;
                } else {
                    self.insts[row][col - self.start_col].status =
                        Status::Executed;

                    // update index of the first non-executed instance
                    if col == self.exec_bars[row] {
                        while self.exec_bars[row]
                            < self.start_col + self.insts[row].len()
                        {
                            if self.insts[row]
                                [self.exec_bars[row] - self.start_col]
                                .status
                                < Status::Executed
                            {
                                break;
                            }
                            self.exec_bars[row] += 1;
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    /// Handler of state machine exec result chan recv.
    pub(super) async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(col < self.start_col + self.insts[row].len());
        pf_trace!("executed cmd in instance at slot {} idx {}", slot, cmd_idx);

        let inst = &mut self.insts[row][col - self.start_col];
        debug_assert!(cmd_idx < inst.reqs.len());
        let (client, ref req) = inst.reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if inst.external && self.external_api.has_client(client) {
                self.external_api.send_reply(
                    ApiReply::normal(*req_id, Some(cmd_result)),
                    client,
                )?;
                pf_trace!(
                    "replied -> client {} for slot {} idx {}",
                    client,
                    slot,
                    cmd_idx
                );
            }
        } else {
            return logged_err!("unexpected API request type");
        }

        // if all commands in this instance have been executed, set status to
        // Executed and update `exec_bar`
        if cmd_idx == inst.reqs.len() - 1 {
            inst.status = Status::Executed;
            pf_debug!("executed all cmds in instance at slot {}", slot);

            // update index of the first non-executed instance
            if col == self.exec_bars[row] {
                while self.exec_bars[row]
                    < self.start_col + self.insts[row].len()
                {
                    if self.insts[row][self.exec_bars[row] - self.start_col]
                        .status
                        < Status::Executed
                    {
                        break;
                    }
                    self.exec_bars[row] += 1;
                }
            }
        }

        Ok(())
    }
}
