import simpy


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
    def __init__(self, env, is_leader, disk_a, disk_b):
        self.env = env
        self.is_leader = is_leader
        self.disk_dev = DiskDev(env, disk_a, disk_b)
        self.peers = dict()
        self.net_links = dict()

    def add_peer(self, name, peer, net_a, net_b):
        self.peers[name] = peer
        self.net_links[name] = NetLink(self.env, net_a, net_b)

    def run(self):
        while True:
            yield self.env.timeout(3)
            print(f"req")
            yield self.disk_dev.save(Data("d", 2))
            print(f"saved")


class Cluster:
    def __init__(self, num_replicas):
        pass


if __name__ == "__main__":
    env = simpy.Environment()
    env.run(until=15)
