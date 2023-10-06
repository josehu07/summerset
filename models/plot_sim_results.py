import matplotlib

matplotlib.use("Agg")

import pickle
import math
import matplotlib.pyplot as plt


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


def plot_x_vsize(num_replicas, results):
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

    plt.savefig(f"results/sim.x_vsize.r_{num_replicas}.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    for num_replicas in (5,):
        with open(f"results/sim.x_vsize.r_{num_replicas}.pkl", "rb") as fpkl:
            results = pickle.load(fpkl)
            plot_x_vsize(num_replicas, results)
