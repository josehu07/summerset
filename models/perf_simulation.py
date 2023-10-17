from enum import Enum
import random
import statistics
import math
import argparse
import pickle

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore
import simpy  # type: ignore


##############
# Data types #
##############


class Data:
    def __init__(self, mark, size):
        self.mark = mark
        self.size = size

    def __str__(self):
        return f"<{self.mark};{self.size}>"


class Req(Data):
    def __init__(self, cid, mark, size):
        self.cid = cid
        super().__init__(mark, size)


class Ack(Data):
    def __init__(self, cid, mark):
        self.cid = cid
        super().__init__(mark, 8)


class Codeword(Data):
    def __init__(self, req, n, m, flags):
        assert len(flags) > 0
        assert len(flags) <= n
        self.req = req
        self.m = m
        self.n = n
        self.flags = flags
        shard_size = req.size / m
        super().__init__(req.mark, shard_size * len(flags))


###############
# Event types #
###############


class EType(Enum):
    NetRecved = 1
    DiskSaved = 2
    ApiGotReq = 3
    SendNewReq = 4


class Event:
    def __init__(self, enum, info, value):
        self.enum = enum
        self.info = info
        self.value = value

    def __str__(self):
        return f"{{{self.enum}|{self.info}|{self.value}}}"


class NetRecved(Event):
    def __init__(self, src, msg):
        super().__init__(EType.NetRecved, src, msg)


class DiskSaved(Event):
    def __init__(self, mark):
        super().__init__(EType.DiskSaved, None, mark)


class ApiGotReq(Event):
    def __init__(self, cid, req):
        super().__init__(EType.ApiGotReq, cid, req)


class SendNewReq(Event):
    def __init__(self, mark):
        super().__init__(EType.SendNewReq, None, mark)


###################
# Component types #
###################


class Device:
    def __init__(self, env, l, t, lv, tv):
        self.env = env
        self.l = l  # latency factor in ms
        self.t = t  # ms to transfer 1 MB
        self.lv = lv  # max variation multiplier for l
        self.tv = tv  # max variation multiplier for t
        self.pipe = simpy.Store(env)

        if self.lv < 1:
            raise RuntimeError(f"invalid variation ratio {self.lv}")
        if self.tv < 1:
            raise RuntimeError(f"invalid variation ratio {self.tv}")

    def delay(self, data):
        l = self.l * random.uniform(1, self.lv)
        t = self.t * random.uniform(1, self.tv)
        delay = l + t * (data.size / 1000000.0)
        yield self.env.timeout(delay)
        self.pipe.put(data)


class NetLink(Device):
    def __init__(self, env, l, t, lv, tv, src, dst):
        self.src = src
        self.dst = dst
        super().__init__(env, l, t, lv, tv)

    def send(self, msg):
        self.env.process(self.delay(msg))

    def recv(self):
        msg = yield self.pipe.get()
        return NetRecved(self.src, msg)


class DiskDev(Device):
    def __init__(self, env, l, t, lv, tv, rid):
        self.rid = rid
        super().__init__(env, l, t, lv, tv)

    def write(self, ent):
        self.env.process(self.delay(ent))

    def saved(self):
        ent = yield self.pipe.get()
        return DiskSaved(ent.mark)


class ExtlApi:
    def __init__(self, env, l, t, lv, tv, rid):
        self.env = env
        self.rid = rid
        self.l = l
        self.t = t
        self.lv = lv
        self.tv = tv
        self.req_links = dict()
        self.ack_links = dict()

    def connect(self, client):
        req_link = NetLink(
            self.env, self.l, self.t, self.lv, self.tv, client.cid, self.rid
        )
        ack_link = NetLink(
            self.env, self.l, self.t, self.lv, self.tv, self.rid, client.cid
        )
        self.req_links[client.cid] = req_link
        self.ack_links[client.cid] = ack_link
        return (req_link, ack_link)

    def req(self):
        # NOTE: hardcode assuming only one client connected
        event = yield self.env.process(self.req_links[2957].recv())
        req = event.value
        return ApiGotReq(2957, req)

    def ack(self, cid, mark):
        if cid not in self.ack_links:
            raise RuntimeError(f"cid {cid} not in connected")
        self.ack_links[cid].send(Ack(cid, mark))


#####################
# Replica & Cluster #
#####################


class Replica:
    def __init__(self, env, rid, api_ltv, disk_ltv, protocol, **protocol_args):
        self.env = env
        self.rid = rid
        self.extl_api = ExtlApi(
            env, api_ltv[0], api_ltv[1], api_ltv[2], api_ltv[3], rid
        )
        self.disk_dev = DiskDev(
            env, disk_ltv[0], disk_ltv[1], disk_ltv[2], disk_ltv[3], rid
        )
        self.send_links = dict()
        self.recv_links = dict()

        # protocol-specific fields & event handlers
        self.protocol = protocol(self, **protocol_args)

    def add_peer(self, peer, net_ltv):
        s2p_link = NetLink(
            self.env, net_ltv[0], net_ltv[1], net_ltv[2], net_ltv[3], self.rid, peer.rid
        )
        p2s_link = NetLink(
            self.env, net_ltv[0], net_ltv[1], net_ltv[2], net_ltv[3], peer.rid, self.rid
        )
        self.send_links[peer.rid] = s2p_link
        self.recv_links[peer.rid] = p2s_link
        peer.send_links[self.rid] = p2s_link
        peer.recv_links[self.rid] = s2p_link

    def run(self):
        events = {
            "disk_saved": self.env.process(self.disk_dev.saved()),
        }
        for peer, link in self.recv_links.items():
            events[("net_recved", peer)] = self.env.process(link.recv())

        # NOTE: hardcoding to have non-leader not do api_got_req
        if self.rid == 0:
            events["api_got_req"] = self.env.process(self.extl_api.req())

        while True:
            # could get multiple completed triggers at this yield
            conds = yield self.env.any_of(events.values())
            for event in conds.values():
                # print(f"{self.env.now}:  R{self.rid}  {event}")

                if event.enum == EType.ApiGotReq:
                    req = event.value
                    yield self.env.process(self.protocol.handle_api_got_req(req))
                    events["api_got_req"] = self.env.process(self.extl_api.req())

                elif event.enum == EType.DiskSaved:
                    mark = event.value
                    yield self.env.process(self.protocol.handle_disk_saved(mark))
                    events["disk_saved"] = self.env.process(self.disk_dev.saved())

                elif event.enum == EType.NetRecved:
                    peer, msg = event.info, event.value
                    yield self.env.process(self.protocol.handle_net_recved(peer, msg))
                    events[("net_recved", peer)] = self.env.process(
                        self.recv_links[peer].recv()
                    )

                else:
                    raise RuntimeError(f"unrecognized event type: {event}")

    def connect(self, client):
        return self.extl_api.connect(client)


class Cluster:
    def __init__(
        self,
        env,
        num_replicas,
        api_ltv,
        disk_ltv_map,
        net_ltv_map,
        protocol,
        **protocol_args,
    ):
        self.env = env
        self.replicas = [
            Replica(
                env,
                rid,
                api_ltv,
                disk_ltv_map[rid],
                protocol,
                **protocol_args,
            )
            for rid in range(num_replicas)
        ]
        self.leader = self.replicas[0]

        for rid, replica in enumerate(self.replicas):
            for peerid in range(rid + 1, num_replicas):
                peer = self.replicas[peerid]
                replica.add_peer(peer, net_ltv_map[(rid, peerid)])

    def launch(self):
        for replica in self.replicas:
            self.env.process(replica.run())

    def connect(self, client):
        return self.leader.connect(client)


#############
# Protocols #
#############


class Protocol:
    def __init__(self, replica):
        self.replica = replica


class MultiPaxos(Protocol):
    def __init__(self, replica, cluster_size):
        super().__init__(replica)

        self.q = cluster_size // 2 + 1

        self.insts = []

    @classmethod
    def name_str(cls, cluster_size):
        return "MultiPaxos/Raft"

    class Instance:
        def __init__(self):
            self.req = None
            self.num_replies = 0
            self.from_peer = -1
            self.client_acked = False

    class AcceptMsg(Data):
        def __init__(self, slot, req):
            super().__init__(f"a-{slot}", req.size + 8)
            self.req = req

    class AcceptReply(Data):
        def __init__(self, slot):
            super().__init__(f"r-{slot}", 8)

    def handle_api_got_req(self, req):
        self.insts.append(self.Instance())
        slot = len(self.insts) - 1
        self.insts[slot].req = req

        for link in self.replica.send_links.values():
            link.send(self.AcceptMsg(slot, req))

        self.replica.disk_dev.write(self.AcceptMsg(slot, req))

        yield from []

    def handle_disk_saved(self, mark):
        if not mark.startswith("a-"):
            raise RuntimeError(f"unrecognized ent mark: {mark}")
        slot = int(mark[2:])
        assert slot < len(self.insts)

        if self.insts[slot].from_peer < 0:
            # disk save on leader
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            # disk save on follower
            self.replica.send_links[self.insts[slot].from_peer].send(
                self.AcceptReply(slot)
            )

        yield from []

    def handle_net_recved(self, peer, msg):
        if msg.mark.startswith("a-"):
            # net recv on follower
            slot = int(msg.mark[2:])
            while slot >= len(self.insts):
                self.insts.append(self.Instance())
            self.insts[slot].from_peer = peer
            self.insts[slot].req = msg.req

            self.replica.disk_dev.write(self.AcceptMsg(slot, msg.req))

        elif msg.mark.startswith("r-"):
            # net recv on leader
            slot = int(msg.mark[2:])
            assert slot < len(self.insts)
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            raise RuntimeError(f"unrecognized msg mark: {msg.mark}")

        yield from []

    def ack_client_reqs(self, slot):
        assert not self.insts[slot].client_acked
        req = self.insts[slot].req
        self.replica.extl_api.ack(req.cid, req.mark)
        self.insts[slot].client_acked = True


class RSPaxos(Protocol):
    def __init__(
        self,
        replica,
        cluster_size,
        same_liveness,
        comp_delay=0,
    ):
        super().__init__(replica)

        self.cluster_size = cluster_size
        self.comp_delay = comp_delay

        self.m = cluster_size // 2 + 1
        if same_liveness:
            self.q = cluster_size
            self.f = cluster_size - self.m
        else:
            self.q = math.ceil((cluster_size + self.m) // 2)
            self.f = self.q - self.m

        self.insts = []

    @classmethod
    def name_str(cls, cluster_size, same_liveness, comp_delay=0):
        if same_liveness:
            return f"RS-Paxos/CRaft (f-forced)"
        else:
            return f"RS-Paxos/CRaft (original)"

    class Instance:
        def __init__(self):
            self.req = None
            self.num_replies = 0
            self.from_peer = -1
            self.client_acked = False

    class AcceptMsg(Data):
        def __init__(self, slot, shard):
            super().__init__(f"a-{slot}", shard.size + 8)
            self.shard = shard

    class AcceptReply(Data):
        def __init__(self, slot):
            super().__init__(f"r-{slot}", 8)

    def handle_api_got_req(self, req):
        self.insts.append(self.Instance())
        slot = len(self.insts) - 1
        self.insts[slot].req = req

        # add EC computation delay
        comp_time = self.comp_delay * (float(req.size) / 1000000.0)
        yield self.replica.env.timeout(comp_time)

        for peer, link in self.replica.send_links.items():
            codeword = Codeword(req, self.cluster_size, self.m, {peer})
            link.send(self.AcceptMsg(slot, codeword))

        codeword = Codeword(req, self.cluster_size, self.m, {self.replica.rid})
        self.replica.disk_dev.write(self.AcceptMsg(slot, codeword))

        yield from []

    def handle_disk_saved(self, mark):
        if not mark.startswith("a-"):
            raise RuntimeError(f"unrecognized ent mark: {mark}")
        slot = int(mark[2:])
        assert slot < len(self.insts)

        if self.insts[slot].from_peer < 0:
            # disk save on leader
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            # disk save on follower
            self.replica.send_links[self.insts[slot].from_peer].send(
                self.AcceptReply(slot)
            )

        yield from []

    def handle_net_recved(self, peer, msg):
        if msg.mark.startswith("a-"):
            # net recv on follower
            slot = int(msg.mark[2:])
            while slot >= len(self.insts):
                self.insts.append(self.Instance())
            self.insts[slot].from_peer = peer
            self.insts[slot].req = msg.shard

            self.replica.disk_dev.write(self.AcceptMsg(slot, msg.shard))

        elif msg.mark.startswith("r-"):
            # net recv on leader
            slot = int(msg.mark[2:])
            assert slot < len(self.insts)
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            raise RuntimeError(f"unrecognized msg mark: {msg.mark}")

        yield from []

    def ack_client_reqs(self, slot):
        assert not self.insts[slot].client_acked
        req = self.insts[slot].req
        self.replica.extl_api.ack(req.cid, req.mark)
        self.insts[slot].client_acked = True


class Crossword(Protocol):
    def __init__(
        self,
        replica,
        cluster_size,
        comp_delay=0,
        shards_per_replica=1,  # NOTE: a "cheating" approach to adaptiveness
    ):
        super().__init__(replica)

        self.cluster_size = cluster_size
        self.comp_delay = comp_delay

        self.m = cluster_size // 2 + 1
        f = cluster_size - self.m
        assert shards_per_replica >= 1
        assert shards_per_replica <= self.m
        self.l = shards_per_replica
        self.q = self.m + f + 1 - self.l

        self.insts = []

    @classmethod
    def name_str(cls, cluster_size, comp_delay=0, shards_per_replica=1):
        return f"Crossword"

    # def update_perf_number(self, lat):
    #     self.perf_tries[self.l].append(lat)
    #     if len(self.perf_tries[self.l]) > 100:
    #         del self.perf_tries[self.l][0]

    #     if not self.all_tried:
    #         if len(self.perf_tries[self.l]) >= 100:
    #             if self.l == 1:
    #                 self.all_tried = True
    #             else:
    #                 self.l -= 1

    # def choose_best_config(self):
    #     if self.all_tried and not self.ql_picked:
    #         m = self.cluster_size // 2 + 1
    #         f = self.cluster_size - m

    #         avg_lats = dict()
    #         for l, lats in self.perf_tries.items():
    #             sorted_lats = sorted(lats)[:-10]
    #             avg_lats[l] = sum(sorted_lats) / len(sorted_lats)
    #         self.l = min(avg_lats, key=avg_lats.get)
    #         self.q = m + f - self.l + 1

    #         print(" picked", self.l)
    #         self.ql_picked = True

    class Instance:
        def __init__(self):
            self.req = None
            self.num_replies = 0
            self.from_peer = -1
            self.client_acked = False

    class AcceptMsg(Data):
        def __init__(self, slot, shard):
            super().__init__(f"a-{slot}", shard.size + 8)
            self.shard = shard

    class AcceptReply(Data):
        def __init__(self, slot):
            super().__init__(f"r-{slot}", 8)

    def handle_api_got_req(self, req):
        self.insts.append(self.Instance())
        slot = len(self.insts) - 1
        self.insts[slot].req = req

        # add EC computation delay
        comp_time = self.comp_delay * (float(req.size) / 1000000.0)
        yield self.replica.env.timeout(comp_time)

        # pick the best config if haven't yet
        # self.choose_best_config()

        # record this req's starting time
        # self.curr_reqs[req.mark] = self.replica.env.now

        for peer, link in self.replica.send_links.items():
            codeword = Codeword(
                req,
                self.cluster_size,
                self.m,
                {(p % self.cluster_size) for p in range(peer, peer + self.l)},
            )
            link.send(self.AcceptMsg(slot, codeword))

        me = self.replica.rid
        codeword = Codeword(
            req,
            self.cluster_size,
            self.m,
            {(p % self.cluster_size) for p in range(me, me + self.l)},
        )
        self.replica.disk_dev.write(self.AcceptMsg(slot, codeword))

        yield from []

    def handle_disk_saved(self, mark):
        if not mark.startswith("a-"):
            raise RuntimeError(f"unrecognized ent mark: {mark}")
        slot = int(mark[2:])
        assert slot < len(self.insts)

        if self.insts[slot].from_peer < 0:
            # disk save on leader
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            # disk save on follower
            self.replica.send_links[self.insts[slot].from_peer].send(
                self.AcceptReply(slot)
            )

        yield from []

    def handle_net_recved(self, peer, msg):
        if msg.mark.startswith("a-"):
            # net recv on follower
            slot = int(msg.mark[2:])
            while slot >= len(self.insts):
                self.insts.append(self.Instance())
            self.insts[slot].from_peer = peer
            self.insts[slot].req = msg.shard

            self.replica.disk_dev.write(self.AcceptMsg(slot, msg.shard))

        elif msg.mark.startswith("r-"):
            # net recv on leader
            slot = int(msg.mark[2:])
            assert slot < len(self.insts)
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.q
            ):
                self.ack_client_reqs(slot)

        else:
            raise RuntimeError(f"unrecognized msg mark: {msg.mark}")

        yield from []

    def ack_client_reqs(self, slot):
        assert not self.insts[slot].client_acked
        req = self.insts[slot].req

        # update perf records
        # assert req.mark in self.curr_reqs
        # lat = self.replica.env.now - self.curr_reqs[req.mark]
        # self.update_perf_number(lat)
        # del self.curr_reqs[req.mark]

        self.replica.extl_api.ack(req.cid, req.mark)
        self.insts[slot].client_acked = True


##########
# Client #
##########


class Stats:
    def __init__(self, env):
        self.env = env
        self.total_sent = 0
        self.total_acks = 0
        self.req_times = dict()
        self.ack_times = dict()

    def add_req(self, mark):
        assert mark not in self.req_times
        self.total_sent += 1
        self.req_times[mark] = self.env.now

    def add_ack(self, mark):
        assert mark in self.req_times
        assert mark not in self.ack_times
        self.total_acks += 1
        self.ack_times[mark] = self.env.now

    def summary(self):
        lats = [self.ack_times[m] - self.req_times[m] for m in self.ack_times]
        lats.sort()
        assert len(lats) > 100

        chunk_cnt = len(lats)
        med_lat = lats[len(lats) // 2]

        lats = lats[:-100]
        avg_lat = sum(lats) / len(lats) if len(lats) > 0 else 0.0
        std_lat = statistics.stdev(lats)

        return (med_lat, avg_lat, std_lat, chunk_cnt, self.total_acks, self.total_sent)

    def clear(self):
        for mark in self.ack_times:
            del self.req_times[mark]
        self.ack_times = dict()


class Client:
    def __init__(self, env, cluster, cid, freq, vsize):
        self.env = env
        self.cid = cid
        self.service = cluster
        self.req_link, self.ack_link = self.service.connect(self)

        self.gap = 1.0 / freq
        self.vsize = vsize
        self.stats = Stats(env)

        self.mark = 0
        self.tick = simpy.Container(env, capacity=1)

        self.env.process(self.ticker())

    def ticker(self):
        while True:
            yield self.env.timeout(self.gap)
            if self.tick.level == 0:
                self.tick.put(1)

    def new_req(self):
        yield self.tick.get(1)
        self.mark += 1
        return SendNewReq(self.mark)

    def loop(self, num_reqs=None):
        events = {
            "req": self.env.process(self.new_req()),
            "ack": self.env.process(self.ack_link.recv()),
        }

        while True:
            # could get multiple completed triggers at this yield
            conds = yield self.env.any_of(events.values())
            for event in conds.values():
                # print(f"{self.env.now}:  C{self.cid}  {event}")

                if event.enum == EType.SendNewReq:
                    mark = event.value
                    self.req_link.send(Req(self.cid, mark, self.vsize))
                    self.stats.add_req(mark)
                    # if num_reqs given, only issue this many reqs
                    if num_reqs is None or self.stats.total_sent < num_reqs:
                        events["req"] = self.env.process(self.new_req())
                    else:
                        del events["req"]

                elif event.enum == EType.NetRecved:
                    mark = event.value.mark
                    self.stats.add_ack(mark)
                    events["ack"] = self.env.process(self.ack_link.recv())

                else:
                    raise RuntimeError(f"unrecognized event type: {event}")

            # if num_reqs given, only issue this many reqs
            if num_reqs is not None and self.stats.total_acks == num_reqs:
                break

        return self.stats

    def start(self, num_reqs=None):
        return self.env.process(self.loop(num_reqs=num_reqs))


#################
# Main entrance #
#################


class HomoParams:
    def __init__(self, num_replicas, api_ltv, disk_ltv, net_ltv, vsize):
        self.num_replicas = num_replicas

        self.api_ltv = api_ltv
        self.disk_ltv_map = {rid: disk_ltv for rid in range(num_replicas)}
        self.net_ltv_map = dict()
        for rid in range(num_replicas):
            for peerid in range(rid + 1, num_replicas):
                self.net_ltv_map[(rid, peerid)] = net_ltv

        # NOTE: a "cheating" approach to adaptiveness
        shards_per_replica = 1
        if net_ltv[0] >= 10:
            shards_per_replica = 3
        elif net_ltv[0] >= 5:
            if vsize <= 1900 * 1000:
                shards_per_replica = 3
            elif vsize <= 2300 * 1000:
                shards_per_replica = 2

        self.protocol_configs = [
            (MultiPaxos, {"cluster_size": num_replicas}),
            (RSPaxos, {"cluster_size": num_replicas, "same_liveness": True}),
            (RSPaxos, {"cluster_size": num_replicas, "same_liveness": False}),
            (
                Crossword,
                {
                    "cluster_size": num_replicas,
                    "shards_per_replica": shards_per_replica,
                },
            ),
        ]

        self.vsize = vsize


class ParamsLatBounded(HomoParams):
    def __init__(self, num_replicas, vsize):
        api_ltv = (1, 1, 1, 1)
        disk_ltv = (2, 0.5, 20, 1.5)
        net_ltv = (10, 2.5, 20, 1.5)
        super().__init__(num_replicas, api_ltv, disk_ltv, net_ltv, vsize)


class ParamsTputBounded(HomoParams):
    def __init__(self, num_replicas, vsize):
        api_ltv = (1, 1, 1, 1)
        disk_ltv = (0.1, 10, 20, 1.5)
        net_ltv = (0.5, 50, 20, 1.5)
        super().__init__(num_replicas, api_ltv, disk_ltv, net_ltv, vsize)


class ParamsLatTputMix(HomoParams):
    def __init__(self, num_replicas, vsize):
        api_ltv = (1, 1, 1, 1)
        disk_ltv = (1, 5, 20, 1.5)
        net_ltv = (5, 25, 20, 1.5)
        super().__init__(num_replicas, api_ltv, disk_ltv, net_ltv, vsize)


def simulate(params):
    results = dict()
    for protocol, protocol_args in params.protocol_configs:
        env = simpy.Environment()
        cluster = Cluster(
            env,
            params.num_replicas,
            params.api_ltv,
            params.disk_ltv_map,
            params.net_ltv_map,
            protocol,
            **protocol_args,
        )
        client = Client(env, cluster, 2957, freq=0.002, vsize=params.vsize)

        cluster.launch()
        done = client.start(num_reqs=1000)
        stats = env.run(until=done)

        med_lat, avg_lat, std_lat, _, _, _ = stats.summary()
        name_str = protocol.name_str(**protocol_args)
        results[name_str] = (med_lat, avg_lat, std_lat)

    return results


def protocol_style(protocol, cluster_size):
    m = cluster_size // 2 + 1
    f = cluster_size - m
    if "MultiPaxos" in protocol:
        return ("-", "dimgray", "s", f"MultiPaxos/Raft\nf={f}  |Q|={m}  l={m}")
    elif "RS-Paxos" in protocol:
        if "forced" in protocol:
            return (
                "-",
                "red",
                "x",
                f"RS-Paxos/CRaft (f-forced)\nf={f}  |Q|={cluster_size}  l=1",
            )
        else:
            q = math.ceil((cluster_size + m) // 2)
            lower_f = q - m
            return (
                ":",
                "orange",
                "x",
                f"RS-Paxos/CRaft (original)\nf={lower_f}  |Q|={q}  l=1",
            )
    elif "Crossword" in protocol:
        return ("-", "steelblue", "o", f"Crossword\nf={f}  |Q|,l=adaptive")
    else:
        raise RuntimeError(f"unrecognized protocol {protocol}")


def params_display(params):
    if params == "lat_bounded":
        return "Latency bounded"
    elif params == "tput_bounded":
        return "Throughput bounded"
    elif params == "lat_tput_mix":
        return "Both moderate"
    else:
        raise RuntimeError(f"unrecognized params {params}")


def plot_x_vsize(num_replicas, results, output_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (11, 3),
            "font.size": 10,
        }
    )

    plt.figure()

    xs = list(map(lambda s: s / 1000, results["vsizes"]))
    protocols = results["lat_bounded"][0].keys()

    for idx, params in enumerate(("lat_bounded", "lat_tput_mix", "tput_bounded")):
        plt.subplot(131 + idx)

        for protocol in protocols:
            ys = [r[protocol][0] for r in results[params]]
            yerrs = [r[protocol][2] for r in results[params]]
            linestyle, color, marker, label = protocol_style(protocol, num_replicas)

            plt.errorbar(
                xs,
                ys,
                # yerr=yerrs,
                label=label,
                linestyle=linestyle,
                linewidth=2,
                color=color,
                # marker=marker,
                # markersize=3,
                ecolor="darkgray",
                elinewidth=1,
                capsize=2,
            )

        plt.ylim(0, 420)

        plt.xlabel("Instance size (kB)")
        plt.ylabel("Response time (ms)")

        title = params_display(params)
        plt.title(title)

    plt.legend(loc="center left", bbox_to_anchor=(1.1, 0.5), labelspacing=1.2)

    plt.tight_layout()

    plt.savefig(f"{output_dir}/sim.x_vsize.r_{num_replicas}.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o", "--output_dir", type=str, default="./results", help="output folder"
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not args.plot:
        random.seed()

        print("NOTE: adaptiveness hardcoded for 5!")

        # for num_replicas in (3, 5, 7, 9):
        for num_replicas in (5,):
            results = {
                "vsizes": [],
                "lat_bounded": [],
                "tput_bounded": [],
                "lat_tput_mix": [],
            }

            vsizes = [v * 1000 for v in (2**p for p in range(3, 11))]
            vsizes += [v * 1000 for v in (100 * i for i in range(1, 51))]
            vsizes.sort()

            for vsize in vsizes:
                results["vsizes"].append(vsize)
                results["lat_bounded"].append(
                    simulate(ParamsLatBounded(num_replicas, vsize))
                )
                results["tput_bounded"].append(
                    simulate(ParamsTputBounded(num_replicas, vsize))
                )
                results["lat_tput_mix"].append(
                    simulate(ParamsLatTputMix(num_replicas, vsize))
                )
                print(f"Ran: {num_replicas} {vsize // 1000}")

            with open(
                f"{args.output_dir}/sim.x_vsize.r_{num_replicas}.pkl", "wb"
            ) as fpkl:
                pickle.dump(results, fpkl)
                print(f"Dumped: {num_replicas}")

    else:
        for num_replicas in (5,):
            with open(
                f"{args.output_dir}/sim.x_vsize.r_{num_replicas}.pkl", "rb"
            ) as fpkl:
                results = pickle.load(fpkl)
                plot_x_vsize(num_replicas, results, args.output_dir)
