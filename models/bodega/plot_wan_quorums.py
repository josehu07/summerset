import math

# fmt: off
import matplotlib  # type: ignore
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore
# fmt: on


class World:
    def plot_quorum_delays(self):
        sorted_dists = []
        for c in self.clients:
            c_dists = sorted([self.distance_func(c, s) for s in self.servers])
            sorted_dists.append(c_dists)

        quorum_sizes = list(range(1, len(self.servers) + 1))
        quorum_delays = dict()
        for qs in quorum_sizes:
            delays = [sorted_dists[ci][qs - 1] for ci in range(len(self.clients))]
            quorum_delays[qs] = delays

        for qs in quorum_sizes:
            # print(f" {qs}: {quorum_delays[qs]}")
            plt.plot(list(range(len(self.clients))), quorum_delays[qs], label=str(qs))
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"models/bodega/{self.name}.png")
        plt.close()


class World1DArray(World):
    def __init__(self, width, num_servers):
        assert num_servers > 1
        assert width >= num_servers

        self.name = f"1DArray.{width}.{num_servers}"
        self.width = width
        self.clients = list(range(self.width))

        self.servers = [0]
        for i in range(1, num_servers - 1):
            self.servers.append((self.width * i) // (num_servers - 1))
        self.servers.append(self.width - 1)

    def distance_func(self, p1, p2):
        assert p1 >= 0 and p1 < self.width
        assert p2 >= 0 and p2 < self.width
        return abs(p1 - p2)


class World1DRing(World):
    def __init__(self, length, num_servers):
        assert num_servers > 1
        assert length >= num_servers

        self.name = f"1DRing.{length}.{num_servers}"
        self.length = length
        self.clients = list(range(self.length))

        self.servers = []
        for i in range(num_servers):
            self.servers.append((self.length * i) // num_servers)

    def distance_func(self, p1, p2):
        assert p1 >= 0 and p1 < self.length
        assert p2 >= 0 and p2 < self.length
        return min(abs(p1 - p2), p1 + self.length - p2, p2 + self.length - p1)


class World2DRect(World):
    def __init__(self, width, height, num_servers):
        assert num_servers == 5  # TODO: complete this

        self.name = f"2DRect.{width}x{height}.{num_servers}"
        self.width = width
        self.height = height
        self.clients = [(w, h) for w in range(width) for h in range(height)]

        self.servers = [
            (0, 0),
            (0, height - 1),
            (width // 2, height // 2),
            (width - 1, 0),
            (width - 1, height - 1),
        ]

    def distance_func(self, p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


if __name__ == "__main__":
    world_1d_array = World1DArray(100, 5)
    world_1d_array.plot_quorum_delays()

    world_1d_ring = World1DRing(100, 5)
    world_1d_ring.plot_quorum_delays()

    world_2d_rect = World2DRect(100, 50, 5)
    world_2d_rect.plot_quorum_delays()
