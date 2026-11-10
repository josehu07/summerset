//! `EPaxos` -- command execution.
//!
//! NOTE: since Summerset Put commands always return the old value, they are
//!       effectively RMW commands; thus the commit vs. execute distinction in
//!       original `EPaxos` paper does not make a difference here. Put commands
//!       get replied to client only after execution. This does not make any
//!       difference to the performance metrics that we are interested in.

use std::collections::{BTreeMap, VecDeque};

use petgraph::algo::tarjan_scc;
use petgraph::graphmap::DiGraphMap;

use super::*;
use crate::server::{ApiReply, ApiRequest};
use crate::utils::SummersetError;

/// What the executor knows about an instance.
enum DepInfo {
    /// Not committed yet, so neither it nor what depends on it can execute.
    Uncommitted,
    /// Already submitted for execution (or compacted), along with all its
    /// transitive dependencies.
    Pruned,
    /// Committed, with its dependencies (explicit ones plus the previous
    /// instance in its row) and sequence number.
    Ready(Vec<SlotIdx>, SeqNum),
}

/// Order in which to execute instance `tail` and its transitive dependencies
/// not pruned, or `None` if any of them is uncommitted. Strongly connected
/// components come dependencies first, each ordered by `(seq, row, col)`, so
/// that the order does not depend on the traversal.
fn execution_order(
    tail: SlotIdx,
    info: impl Fn(SlotIdx) -> Result<DepInfo, SummersetError>,
) -> Result<Option<Vec<SlotIdx>>, SummersetError> {
    // collect the instances to execute, with their dependencies and seqs
    let mut ready = BTreeMap::new();
    let mut queue = VecDeque::from([tail]);
    while let Some(slot) = queue.pop_front() {
        if ready.contains_key(&slot) {
            continue;
        }
        match info(slot)? {
            DepInfo::Uncommitted => return Ok(None),
            DepInfo::Pruned => {}
            DepInfo::Ready(deps, seq) => {
                queue.extend(deps.iter().copied());
                ready.insert(slot, (deps, seq));
            }
        }
    }

    // link each instance to its dependencies executing along with it
    let mut dep_graph = DiGraphMap::<SlotIdx, ()>::new();
    for (&slot, (deps, _)) in &ready {
        dep_graph.add_node(slot);
        for &dep in deps.iter().filter(|dep| ready.contains_key(dep)) {
            dep_graph.add_edge(slot, dep, ());
        }
    }

    // Tarjan's algorithm gives SCCs in reverse topological order, i.e., with
    // dependencies first
    Ok(Some(
        tarjan_scc(&dep_graph)
            .into_iter()
            .flat_map(|mut scc| {
                scc.sort_by_key(|slot| (ready[slot].1, *slot));
                scc
            })
            .collect(),
    ))
}

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
        let order = execution_order(tail_slot, |slot| {
            let (row, col) = slot.unpack();
            if col >= self.commit_bars[row] {
                return Ok(DepInfo::Uncommitted);
            }
            if col < self.start_col
                || self.insts[row][col - self.start_col].status >= Status::Executing
            {
                return Ok(DepInfo::Pruned);
            }
            let inst = &self.insts[row][col - self.start_col];
            let mut deps = vec![];
            for (r, c) in inst.deps.0.iter().enumerate() {
                if let Some(c) = c {
                    deps.push(SlotIdx(ReplicaId::try_from(r)?, *c));
                }
            }
            if col > self.start_col {
                // R.c implicitly depends on R.(c-1)
                deps.push(SlotIdx(slot.0, col - 1));
            }
            Ok(DepInfo::Ready(deps, inst.seq))
        })?;
        let Some(order) = order else {
            pf_trace!("execution attempt aborted for slot {}", tail_slot);
            return Ok(false);
        };
        pf_trace!("execution attempt passed: |order| {}", order.len());

        // then execute one-by-one
        for slot in order {
            let (row, col) = slot.unpack();
            for (cmd_idx, (_, req)) in self.insts[row][col - self.start_col]
                .reqs
                .iter()
                .enumerate()
            {
                if let ApiRequest::Req { cmd, .. } = req {
                    // no client sessions, as replicas may execute a client's
                    // non-interfering commands in different orders
                    let cmd_id = Self::make_command_id(slot, cmd_idx);
                    if sync_exec {
                        self.state_machine
                            .do_sync_cmd(cmd_id, cmd.clone(), None)
                            .await?;
                    } else {
                        self.state_machine.submit_cmd(cmd_id, cmd.clone(), None)?;
                    }
                }
            }

            if sync_exec {
                self.insts[row][col - self.start_col].status = Status::Executed;
                // update index of the first non-executed instance
                self.advance_exec_bar(row, col);
            } else {
                self.insts[row][col - self.start_col].status = Status::Executing;
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
                self.external_api
                    .send_reply(ApiReply::normal(*req_id, Some(cmd_result)), client)?;
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
            self.advance_exec_bar(row, col);
        }

        Ok(())
    }

    /// If `exec_bars[row]` is at `col`, advances it past the instances
    /// executed, which may come out of order.
    pub(super) fn advance_exec_bar(&mut self, row: usize, col: usize) {
        if col == self.exec_bars[row] {
            while self.exec_bars[row] < self.start_col + self.insts[row].len()
                && self.insts[row][self.exec_bars[row] - self.start_col].status >= Status::Executed
            {
                self.exec_bars[row] += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executes_dependencies_first_and_once() -> Result<(), SummersetError> {
        // tail depends on the executed done, on base, and on mid, which
        // depends on base
        let (tail, base, mid) = (SlotIdx(0, 1), SlotIdx(1, 2), SlotIdx(2, 1));
        let done = SlotIdx(1, 1);
        let info = |slot| {
            Ok(if slot == tail {
                DepInfo::Ready(vec![done, base, mid], 3)
            } else if slot == base {
                DepInfo::Ready(vec![], 1)
            } else if slot == mid {
                DepInfo::Ready(vec![base], 2)
            } else {
                DepInfo::Pruned
            })
        };
        assert_eq!(execution_order(tail, info)?, Some(vec![base, mid, tail]));
        Ok(())
    }

    #[test]
    fn orders_cycles_by_seq() -> Result<(), SummersetError> {
        // two instances depending on each other: the lower seq goes first,
        // whichever the traversal starts from
        let (first, second) = (SlotIdx(1, 1), SlotIdx(0, 1));
        let info = |slot| {
            Ok(if slot == first {
                DepInfo::Ready(vec![second], 1)
            } else {
                DepInfo::Ready(vec![first], 2)
            })
        };
        assert_eq!(execution_order(first, info)?, Some(vec![first, second]));
        assert_eq!(execution_order(second, info)?, Some(vec![first, second]));
        Ok(())
    }

    #[test]
    fn waits_for_uncommitted() -> Result<(), SummersetError> {
        let (tail, dep) = (SlotIdx(0, 1), SlotIdx(1, 1));
        let info = |slot| {
            Ok(if slot == tail {
                DepInfo::Ready(vec![dep], 1)
            } else {
                DepInfo::Uncommitted
            })
        };
        assert_eq!(execution_order(tail, info)?, None);
        Ok(())
    }
}
