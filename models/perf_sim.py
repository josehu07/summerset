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
    def __init__(self, mark, size):
        super().__init__(mark, size)


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
    def __init__(self, env, b, rid):
        self.env = env
        self.b = b
        self.rid = rid
        self.mark = 0
        self.ibuf = []
        self.tick = simpy.Container()

        self.env.process(self.ticker())

    def ticker(self):
        while True:
            yield self.env.timeout(self.b)
            self.tick.put(1)

    def req(self, req):
        self.ibuf.append(req)

    def batch(self):
        yield self.tick.get(1)
        self.tick.level = 0

        batch = Batch(self.mark, self.ibuf)
        self.mark += 1
        self.ibuf = []
        return ApiBatch(batch)


#####################
# Replica & Cluster #
#####################


class Replica:
    def __init__(self, env, rid, api_b, disk_ltv):
        self.env = env
        self.rid = rid
        self.extl_api = ExtlApi(env, api_b)
        self.disk_dev = DiskDev(env, disk_ltv[0], disk_ltv[1], disk_ltv[2])
        self.send_links = dict()
        self.recv_links = dict()

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
        while True:
            events = [
                self.env.process(self.extl_api.batch()),
                self.env.process(self.disk_dev.saved()),
            ]
            for link in self.recv_links:
                events.append(self.env.process(link.recv()))
            event = yield self.env.any_of(events)

            print(event)

    def req(self, data):
        self.extl_api.req(data)


class Cluster:
    def __init__(self, env, num_replicas, api_b, disk_perf_map, net_perf_map):
        self.env = env
        self.replicas = [
            Replica(
                env,
                rid,
                api_b,
                disk_perf_map[rid],
            )
            for rid in range(num_replicas)
        ]
        self.leader = self.replicas[0]

        for rid, replica in enumerate(self.replicas):
            for peerid in range(rid + 1, num_replicas):
                peer = self.replicas[peerid]
                replica.add_peer(peer, net_perf_map[{rid, peerid}])

    def launch(self):
        for replica in self.replicas:
            self.env.process(replica.run())

    def req(self, data):
        self.leader.req(data)


##########
# Client #
##########


class Client:
    def __init__(self, env, cluster, freq):
        self.env = env
        self.service = cluster
        self.gap = 1.0 / freq

    def driver(self):
        while True:
            yield self.env.timeout(self.gap)
            self.service.req(Req("TODO", 8))

    def start(self):
        self.env.process(self.driver())


if __name__ == "__main__":
    env = simpy.Environment()
    env.run(until=15)
