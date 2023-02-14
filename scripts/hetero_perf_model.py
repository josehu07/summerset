import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
from enum import Enum

def calc_time_net(B, L_net, T_net):
    # print("net", L_net + (B / T_net))
    return L_net + (B / T_net)

def calc_time_dur(B, T_dur):
    # print("dur", B / T_dur)
    return B / T_dur

def calc_time_comp(B, T_comp):
    # print("comp", B / T_comp)
    return B / T_comp

class HControl(Enum):
    L_NET = 1
    T_NET = 2
    T_DUR = 3
    T_COMP = 4

def calc_T_raft(T_formula, h_control, h_range):
    if h_control == HControl.L_NET:
        return [T_formula(1. / h, 1, 1, 1) for h in h_range]
    elif h_control == HControl.T_NET:
        return [T_formula(1, h, 1, 1) for h in h_range]
    elif h_control == HControl.T_DUR:
        return [T_formula(1, 1, h, 1) for h in h_range]
    elif h_control == HControl.T_COMP:
        return [T_formula(1, 1, 1, h) for h in h_range]
    else:
        raise Exception(f"unrecognized h_control enum {h_control}")

def plot_T_series(h_range, results_list, label_list):
    assert len(results_list) == len(label_list)
    assert len(results_list) > 0
    assert len(h_range) == len(results_list[0])

    for (results, label) in zip(results_list, label_list):
        plt.plot(h_range, results, label=f"varying {label}", marker='o')

    plt.legend()

    plt.xlabel("heterogeneity ratio h")
    plt.ylabel("overall throughput of Raft (#cmds/s)")

    plt.savefig("T_raft.png", dpi=120)
    plt.close()

def perf_raft_model():
    L_net_l_fp1 = 1 * 10**(-4)      # s
    T_net_l_fp1 = 10 * 10**9 / 8    # Bytes/s
    L_net_c_l = 1 * 10**(-4)
    T_net_c_l = 10 * 10**9 / 8
    T_dur_fp1 = 200 * 10**6         # Bytes/s
    T_comp_l = 5 * 10**9            # Bytes/s

    batch_size = 8
    cmd_bytes = 10240
    B = batch_size * cmd_bytes

    T_raft = lambda h_L_net, h_T_net, h_T_dur, h_T_comp: batch_size / (
        2 * calc_time_net(B, h_L_net * L_net_l_fp1, h_T_net * T_net_l_fp1)
        + calc_time_dur(B, h_T_dur * T_dur_fp1)
        + calc_time_comp(B, h_T_comp * T_comp_l)
        + 2 * calc_time_net(B, L_net_c_l, T_net_c_l)
    )

    h_range = [0.1 * hi for hi in range(2, 11)]

    T_raft_L_net_results = calc_T_raft(T_raft, HControl.L_NET, h_range)
    T_raft_T_net_results = calc_T_raft(T_raft, HControl.T_NET, h_range)
    T_raft_T_dur_results = calc_T_raft(T_raft, HControl.T_DUR, h_range)

    plot_T_series(h_range,
                  [T_raft_L_net_results,
                   T_raft_T_net_results,
                   T_raft_T_dur_results],
                  ["network latency",
                   "network bandwidth",
                   "durability bandwidth"])

if __name__ == "__main__":
    perf_raft_model()
