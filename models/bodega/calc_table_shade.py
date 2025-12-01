import sys
import os
import pprint

# keeping this hardcoded import for now
sys.path.append(
    f"{os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))}/scripts"
)
import utils


RTTS = utils.config.PairsMap(
    # Ref: https://www.usenix.org/system/files/nsdi21-tollman.pdf#page=7
    {
        (0, 1): 77,
        (0, 2): 129,
        (0, 3): 137,
        (0, 4): 221,
        (1, 2): 59,
        (1, 3): 64,
        (1, 4): 146,
        (2, 3): 26,
        (2, 4): 91,
        (3, 4): 98,
    },
    default=0,
)
LEADER = "host1"
CLIENT = "host3"

PROTOCOLS = [
    "LeaderLs",
    "EPaxos",
    "Hermes",
    "PQR",
    "PQRLeaderLs",
    "Pando",
    "Megastore",
    "QuorumLs",
    "QuorumLsCtn",
    "Bodega",
]


class Metrics:
    def __init__(self):
        self.c = 2  # hardcoded 1 ms for client-nearest RTT
        self.l = RTTS.get(LEADER, CLIENT)

        leader_rtts = sorted([RTTS.get(LEADER, f"host{i}") for i in range(5)])
        self.N = leader_rtts[-1]
        self.M = leader_rtts[-2]
        self.m = leader_rtts[-3]

        print("Metrics:")
        print(f"  c = {self.c}")
        print(f"  l = {self.l}")
        print(f"  N = {self.N}")
        print(f"  M = {self.M}")
        print(f"  m = {self.m}")

    def shading(self):
        c = self.c
        l = self.l
        m = self.m
        N = self.N
        M = self.M
        # fmt: off
        cells = {
            "LeaderLs":     (l + m    , l      , l          , 0     ),
            "EPaxos":       (c + M    , c + M  , c + M + m  , M     ),
            "Hermes":       (c + N    , c      , c + N / 2  , N / 2 ),
            "PQR":          (l + m    , c + m  , c + m * 2  , m     ),
            "PQRLeaderLs":  (l + m    , c + m  , c + m + l  , m     ),
            "Pando":        (c + m    , c + m  , c + N      , N     ),
            "Megastore":    (l + N * 2, c      , c + l      , N * 2 ),
            "QuorumLs":     (l + N    , c      , c + l      , N * 2 ),
            "QuorumLsCtn":  (l + N    , c      , c + l      , N     ),
            "Bodega":       (l + N    , c      , c + m / 2  , m / 2 ),
        }
        # fmt: on
        print("\nCells:")
        pprint.pprint(cells)

        normalized_cells = {}
        columns = [
            [values[i] for values in cells.values() if values[i] > 0]
            for i in range(4)
        ]
        col_mins = [min(column) for column in columns]
        col_maxs = [max(column) for column in columns]

        for protocol, values in cells.items():
            normalized_values = [
                (
                    3 + 24 * (value - col_mins[i]) / (col_maxs[i] - col_mins[i])
                    if col_maxs[i] != col_mins[i] and value >= col_mins[i]
                    else -1
                )
                for i, value in enumerate(values)
            ]
            normalized_cells[protocol] = tuple(normalized_values)

        print("\nNormalized Cells:")
        pprint.pprint(normalized_cells)

        return normalized_cells


def main():
    metrics = Metrics()
    cells = metrics.shading()

    print("\nCell colors:")
    for protocol in PROTOCOLS:
        print("\n", protocol)
        for cell in cells[protocol]:
            if cell > 0:
                print(f"  \\cellcolor{{black!{int(cell)}}}")
            else:
                print("  -")
