import simpy  # type: ignore


class Data:
    def __init__(self, mark, size):
        self.mark = mark
        self.size = size


class NetLink:
    def __init__(self, env, a, b):
        self.env = env
        self.a = a
        self.b = b
        self.store = simpy.Store(env)

    def delay(self, data):
        delay = self.a + self.b * data.size
        yield self.env.timeout(delay)
        self.store.put(data)

    def send(self, data):
        self.env.process(self.delay(data))

    def recv(self):
        return self.store.get()


class DiskDev:
    def __init__(self, env, a, b):
        self.env = env
        self.a = a
        self.b = b

    def delay(self, data):
        delay = self.a + self.b * data.size
        yield self.env.timeout(delay)

    def save(self, data):
        self.env.process(self.delay(data))


class Replica:
    def __init__(self, env, rid, disk_ab):
        self.env = env
        self.rid = rid
        self.api = NetLink(env, 0, 0)
        self.disk_dev = DiskDev(env, disk_ab[0], disk_ab[1])
        self.net_links = dict()

    def add_peer(self, rid, net_ab):
        self.net_links[rid] = NetLink(self.env, net_ab[0], net_ab[1])

    def run(self):
        while True:
            yield self.env.timeout(3)
            print(f"req")
            yield self.disk_dev.save(Data("d", 2))
            print(f"saved")

    def req(self, data):
        self.api.send(data)


class Cluster:
    def __init__(self, env, num_replicas, disk_perf_map, net_perf_map):
        self.env = env
        self.replicas = [
            Replica(
                env,
                rid,
                disk_perf_map[rid],
            )
            for rid in range(num_replicas)
        ]
        self.leader = self.replicas[0]

        for replica in self.replicas:
            for rid in range(num_replicas):
                if rid != replica.rid:
                    replica.add_peer(
                        rid,
                        net_perf_map[(replica.rid, rid)],
                    )


if __name__ == "__main__":
    env = simpy.Environment()
    env.run(until=15)
