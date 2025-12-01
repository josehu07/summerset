import math


class RingWorld:
    def __init__(self):
        self.ticks = 24
        self.servers = [3, 0, 18, 14, 12]
        self.clients = list(range(4)) + list(range(11, 20))
        self.leader_id = 4
        self.leader = self.servers[self.leader_id]

    def distance(self, a, b):
        assert a >= 0 and a < self.ticks
        assert b >= 0 and b < self.ticks
        d = abs(a - b)
        return min(d, self.ticks - d)

    def nearest_server_among(self, origin, servers):
        nearest, min_dist = None, self.ticks + 1
        for s in servers:
            dist = self.distance(origin, s)
            if dist < min_dist:
                min_dist = dist
                nearest = s
        return nearest

    def nearest_server(self, origin):
        return self.nearest_server_among(origin, self.servers)

    def farthest_server_among(self, origin, servers):
        farthest, max_dist = None, 0
        for s in servers:
            dist = self.distance(origin, s)
            if dist > max_dist:
                max_dist = dist
                farthest = s
        return farthest

    def farthest_server(self, origin):
        return self.farthest_server_among(origin, self.servers)

    def quorum_max_from(self, origin, size):
        ds = [self.distance(origin, s) for s in self.servers]
        ds.sort()
        return ds[size - 1]

    def quorum_incl_max_from(self, origin, size, includes):
        max_noni = self.quorum_max_from(origin, size)
        max_incl = 0
        for s in includes:
            dist = self.distance(origin, s)
            if dist > max_incl:
                max_incl = dist
        return max(max_noni, max_incl)


class Protocol:
    def __init__(self, world, name):
        self.name = name
        self.world = world
        assert len(self.world.servers) % 2 == 1

    def write_from(self, client):
        raise RuntimeError("base method called")

    def read_idle_from(self, client):
        raise RuntimeError("base method called")

    def read_busy_from(self, client):
        raise RuntimeError("base method called")

    def window_guess(self, client):
        """Only consider one farthest in-flight conflicting write here."""
        raise RuntimeError("base method called")

    def write_avg(self):
        delays = [self.write_from(c) for c in self.world.clients]
        return sum(delays) / len(delays)

    def read_idle_avg(self):
        delays = [self.read_idle_from(c) for c in self.world.clients]
        return sum(delays) / len(delays)

    def read_busy_avg(self):
        delays = [self.read_busy_from(c) for c in self.world.clients]
        return sum(delays) / len(delays)

    def window_avg(self):
        windows = [self.window_guess(c) for c in self.world.clients]
        return sum(windows) / len(windows)


class MultiPaxos(Protocol):
    def __init__(self, world):
        super().__init__(world, "MP")
        self.majority = (len(world.servers) + 1) // 2

    def write_from(self, client):
        return 2 * (
            self.world.distance(client, self.world.leader)
            + self.world.quorum_max_from(self.world.leader, self.majority)
        )

    def read_idle_from(self, client):
        return 2 * self.world.distance(client, self.world.leader)

    def read_busy_from(self, client):
        return 2 * self.world.distance(client, self.world.leader)

    def window_guess(self, client):
        return 0


class EPaxos(Protocol):
    def __init__(self, world):
        super().__init__(world, "EP")
        self.majority = (len(world.servers) + 1) // 2
        self.supermajority = math.floor((3 * len(world.servers) - 1) / 4)

    def write_from(self, client):
        nearest = self.world.nearest_server(client)
        return 2 * (
            self.world.distance(client, nearest)
            + self.world.quorum_max_from(nearest, self.supermajority)
        )

    def read_idle_from(self, client):
        nearest = self.world.nearest_server(client)
        return 2 * (
            self.world.distance(client, nearest)
            + self.world.quorum_max_from(nearest, self.supermajority)
        )

    def read_busy_from(self, client):
        nearest = self.world.nearest_server(client)
        return 2 * (
            self.world.distance(client, nearest)
            + self.world.quorum_max_from(nearest, self.supermajority)
            + self.world.quorum_max_from(nearest, self.majority)
        )

    def window_guess(self, client):
        farthest = self.world.farthest_server(client)
        return 2 * self.world.quorum_max_from(farthest, self.supermajority)


class QuorumLease(Protocol):
    def __init__(self, world, lessees):
        super().__init__(world, f"QL-{len(lessees)}")
        self.majority = (len(world.servers) + 1) // 2
        self.lessees = [self.world.servers[l] for l in lessees]

    def write_from(self, client):
        return 2 * (
            self.world.distance(client, self.world.leader)
            + self.world.quorum_incl_max_from(
                self.world.leader, self.majority, self.lessees
            )
        )

    def read_idle_from(self, client):
        nearest = self.world.nearest_server_among(client, self.lessees)
        return 2 * min(
            self.world.distance(client, nearest),
            self.world.distance(client, self.world.leader),
        )

    def read_busy_from(self, client):
        nearest = self.world.nearest_server_among(client, self.lessees)
        return (
            self.world.distance(client, nearest)
            + self.world.distance(nearest, self.world.leader)
            + self.world.distance(self.world.leader, client)
        )

    def window_guess(self, client):
        return 4 * self.world.quorum_incl_max_from(
            self.world.leader, self.majority, self.lessees
        )


class NearbyRead(Protocol):
    def __init__(self, world, read_qsize):
        super().__init__(world, f"NR-{read_qsize}")
        self.majority = (len(world.servers) + 1) // 2
        self.read_qsize = read_qsize
        self.write_qsize = max(
            len(world.servers) + 1 - read_qsize, self.majority
        )

    def write_from(self, client):
        return 2 * (
            self.world.distance(client, self.world.leader)
            + self.world.quorum_max_from(self.world.leader, self.write_qsize)
        )

    def read_idle_from(self, client):
        return 2 * min(
            self.world.quorum_max_from(client, self.read_qsize),
            self.world.distance(client, self.world.leader),
        )

    def read_busy_from(self, client):
        return 2 * self.world.distance(client, self.world.leader)

    def window_guess(self, client):
        return 2 * self.world.quorum_max_from(
            self.world.leader, self.write_qsize
        )


def ring_world_model():
    world = RingWorld()
    protocols = [
        MultiPaxos(world),
        EPaxos(world),
        QuorumLease(world, list(range(5))),
        QuorumLease(world, [0, 2, 4]),
        NearbyRead(world, 1),
        NearbyRead(world, 2),
        NearbyRead(world, 3),
    ]

    results = {
        "write": [],
        "read-idle": [],
        "read-busy": [],
        "window": [],
    }
    for p in protocols:
        results["write"].append(p.write_avg())
        results["read-idle"].append(p.read_idle_avg())
        results["read-busy"].append(p.read_busy_avg())
        results["window"].append(p.window_avg())

    print(f"{' ':9s}", end="")
    for p in protocols:
        print(f" {p.name:>4s}", end="")
    print()
    for w in results:
        print(f"{w:>9s}", end="")
        for i in range(len(protocols)):
            print(f" {results[w][i]:4.1f}", end="")
        print()


def main():
    ring_world_model()
