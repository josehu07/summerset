import simpy  # type: ignore
from enum import Enum  # type: ignore


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


class Batch(Data):
    def __init__(self, mark, vec):
        self.vec = vec
        size = sum((data.size for data in vec))
        super().__init__(mark, size)


###############
# Event types #
###############


class EType(Enum):
    NetRecved = 1
    DiskSaved = 2
    ApiBatch = 3
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


class ApiBatch(Event):
    def __init__(self, batch):
        super().__init__(EType.ApiBatch, None, batch)


class SendNewReq(Event):
    def __init__(self, mark):
        super().__init__(EType.SendNewReq, None, mark)


###################
# Component types #
###################


class Device:
    def __init__(self, env, l, t, v):
        self.env = env
        self.l = l
        self.t = t
        self.v = v  # TODO: use this
        self.pipe = simpy.Store(env)

    def delay(self, data):
        delay = self.l + self.t * data.size
        yield self.env.timeout(delay)
        self.pipe.put(data)


class NetLink(Device):
    def __init__(self, env, l, t, v, src, dst):
        self.src = src
        self.dst = dst
        super().__init__(env, l, t, v)

    def send(self, msg):
        self.env.process(self.delay(msg))

    def recv(self):
        msg = yield self.pipe.get()
        return NetRecved(self.src, msg)


class DiskDev(Device):
    def __init__(self, env, l, t, v, rid):
        self.rid = rid
        super().__init__(env, l, t, v)

    def write(self, ent):
        self.env.process(self.delay(ent))

    def saved(self):
        ent = yield self.pipe.get()
        return DiskSaved(ent.mark)


class ExtlApi:
    def __init__(self, env, l, t, v, b, rid):
        self.env = env
        self.l = l
        self.t = t
        self.v = v
        self.b = b

        self.rid = rid
        self.req_links = dict()
        self.ack_links = dict()

        self.mark = 0
        self.tick = simpy.Container(env, capacity=1)

        self.env.process(self.ticker())

    def connect(self, client):
        req_link = NetLink(self.env, self.l, self.t, self.v, client.cid, self.rid)
        ack_link = NetLink(self.env, self.l, self.t, self.v, self.rid, client.cid)
        self.req_links[client.cid] = req_link
        self.ack_links[client.cid] = ack_link
        return (req_link, ack_link)

    def ticker(self):
        while True:
            yield self.env.timeout(self.b)
            if self.tick.level == 0:
                self.tick.put(1)

    def batch(self):
        while True:
            yield self.tick.get(1)

            reqs = []
            for link in self.req_links.values():
                if len(link.pipe.items) > 0:
                    reqs += link.pipe.items
                    link.pipe.items = []

            # do not return if no reqs available at this tick
            if len(reqs) == 0:
                continue

            batch = Batch(self.mark, reqs)
            self.mark += 1
            return ApiBatch(batch)

    def ack(self, cid, mark):
        if cid not in self.ack_links:
            raise RuntimeError(f"cid {cid} not in connected")
        self.ack_links[cid].send(Ack(cid, mark))


#####################
# Replica & Cluster #
#####################


class Replica:
    def __init__(self, env, rid, api_ltvb, disk_ltv, protocol, **protocol_args):
        self.env = env
        self.rid = rid
        self.extl_api = ExtlApi(
            env, api_ltvb[0], api_ltvb[1], api_ltvb[2], api_ltvb[3], rid
        )
        self.disk_dev = DiskDev(env, disk_ltv[0], disk_ltv[1], disk_ltv[2], rid)
        self.send_links = dict()
        self.recv_links = dict()

        # protocol-specific fields & event handlers
        self.protocol = protocol(self, **protocol_args)

    def add_peer(self, peer, net_ltv):
        s2p_link = NetLink(
            self.env, net_ltv[0], net_ltv[1], net_ltv[2], self.rid, peer.rid
        )
        p2s_link = NetLink(
            self.env, net_ltv[0], net_ltv[1], net_ltv[2], peer.rid, self.rid
        )
        self.send_links[peer.rid] = s2p_link
        self.recv_links[peer.rid] = p2s_link
        peer.send_links[self.rid] = p2s_link
        peer.recv_links[self.rid] = s2p_link

    def run(self):
        events = {
            "api_batch": self.env.process(self.extl_api.batch()),
            "disk_saved": self.env.process(self.disk_dev.saved()),
        }
        for peer, link in self.recv_links.items():
            events[("net_recved", peer)] = self.env.process(link.recv())

        while True:
            # could get multiple completed triggers at this yield
            conds = yield self.env.any_of(events.values())
            for event in conds.values():
                # print(f"{self.env.now}:  R{self.rid}  {event}")

                if event.enum == EType.ApiBatch:
                    batch = event.value
                    self.protocol.handle_api_batch(batch)
                    events["api_batch"] = self.env.process(self.extl_api.batch())

                elif event.enum == EType.DiskSaved:
                    mark = event.value
                    self.protocol.handle_disk_saved(mark)
                    events["disk_saved"] = self.env.process(self.disk_dev.saved())

                elif event.enum == EType.NetRecved:
                    peer, msg = event.info, event.value
                    self.protocol.handle_net_recved(peer, msg)
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
        api_ltvb,
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
                api_ltvb,
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
    def __init__(self, replica, quorum_size):
        super().__init__(replica)

        self.quorum_size = quorum_size
        self.insts = []

    class Instance:
        def __init__(self):
            self.batch = None
            self.num_replies = 0
            self.from_peer = -1
            self.client_acked = False

    class AcceptMsg(Data):
        def __init__(self, slot, batch):
            super().__init__(f"a-{slot}", batch.size + 8)
            self.batch = batch

    class AcceptReply(Data):
        def __init__(self, slot):
            super().__init__(f"r-{slot}", 8)

    def handle_api_batch(self, batch):
        self.insts.append(self.Instance())
        slot = len(self.insts) - 1
        self.insts[slot].batch = batch

        for link in self.replica.send_links.values():
            link.send(self.AcceptMsg(slot, batch))

        self.replica.disk_dev.write(self.AcceptMsg(slot, batch))

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
                and self.insts[slot].num_replies >= self.quorum_size
            ):
                self.ack_client_reqs(slot)

        else:
            # disk save on follower
            self.replica.send_links[self.insts[slot].from_peer].send(
                self.AcceptReply(slot)
            )

    def handle_net_recved(self, peer, msg):
        if msg.mark.startswith("a-"):
            # net recv on follower
            slot = int(msg.mark[2:])
            while slot >= len(self.insts):
                self.insts.append(self.Instance())
            self.insts[slot].from_peer = peer
            self.insts[slot].batch = msg.batch

            self.replica.disk_dev.write(self.AcceptMsg(slot, msg.batch))

        elif msg.mark.startswith("r-"):
            # net recv on leader
            slot = int(msg.mark[2:])
            assert slot < len(self.insts)
            self.insts[slot].num_replies += 1

            if (
                not self.insts[slot].client_acked
                and self.insts[slot].num_replies >= self.quorum_size
            ):
                self.ack_client_reqs(slot)

        else:
            raise RuntimeError(f"unrecognized msg mark: {msg.mark}")

    def ack_client_reqs(self, slot):
        assert not self.insts[slot].client_acked

        for req in self.insts[slot].batch.vec:
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

    def display(self, chunk_time):
        lats = [self.ack_times[m] - self.req_times[m] for m in self.ack_times]
        avg_tput = len(lats) / chunk_time
        avg_lat = sum(lats) / len(lats) if len(lats) > 0 else 0.0
        return f"{avg_tput:>9.2f}  {avg_lat:>9.2f}  {len(lats):>7d}  {self.total_acks:>8d} / {self.total_sent:<8d}"

    def clear(self):
        for mark in self.ack_times:
            del self.req_times[mark]
        self.ack_times = dict()


class Client:
    def __init__(self, env, cluster, cid, freq, vsize, chunk_time):
        self.env = env
        self.cid = cid
        self.service = cluster
        self.req_link, self.ack_link = self.service.connect(self)

        self.gap = 1.0 / freq
        self.vsize = vsize

        self.mark = 0
        self.tick = simpy.Container(env, capacity=1)

        self.stats = Stats(env)
        self.last_print = 0
        self.chunk_time = chunk_time

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

    def loop(self):
        events = {
            "req": self.env.process(self.new_req()),
            "ack": self.env.process(self.ack_link.recv()),
        }

        print(
            f"{'Time':>5s}:  {'Tput':>9s}  {'Lat':>9s}  {'Chunk':>7s}  {'Reply':>8s} / {'Total':<8s}"
        )
        while True:
            # could get multiple completed triggers at this yield
            conds = yield self.env.any_of(events.values())
            for event in conds.values():
                # print(f"{self.env.now}:  C{self.cid}  {event}")

                if event.enum == EType.SendNewReq:
                    mark = event.value
                    self.req_link.send(Req(self.cid, mark, self.vsize))
                    self.stats.add_req(mark)
                    events["req"] = self.env.process(self.new_req())

                elif event.enum == EType.NetRecved:
                    mark = event.value.mark
                    self.stats.add_ack(mark)
                    events["ack"] = self.env.process(self.ack_link.recv())

                else:
                    raise RuntimeError(f"unrecognized event type: {event}")

            # print chunk-average stats
            if self.env.now - self.last_print > self.chunk_time:
                print(f"{self.env.now:>5.1f}:  {self.stats.display(self.chunk_time)}")
                self.stats.clear()
                self.last_print = self.env.now

    def start(self):
        self.env.process(self.loop())


#################
# Main entrance #
#################


if __name__ == "__main__":
    num_replicas = 5
    api_ltvb = (1, 1, 0, 3)
    disk_ltv_map = {rid: (1, 1, 0) for rid in range(num_replicas)}
    net_ltv_map = dict()
    for rid in range(num_replicas):
        for peerid in range(rid + 1, num_replicas):
            net_ltv_map[(rid, peerid)] = (1, 1, 0)
    freq = 1
    vsize = 1

    env = simpy.Environment()

    cluster = Cluster(
        env,
        5,
        api_ltvb,
        disk_ltv_map,
        net_ltv_map,
        MultiPaxos,
        quorum_size=3,
    )
    cluster.launch()

    client = Client(env, cluster, 2957, freq, vsize, 10)
    client.start()

    env.run(until=60)
