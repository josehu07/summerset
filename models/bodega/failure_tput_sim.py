# Monte Carlo simulation by Claude Sonnet 4

import argparse
import numpy as np
import time
import re
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


def get_coverage_tputs() -> List[float]:
    put_ratio = 0.1
    wlats = [40.96, 43.26, 45.31, 52.06, 57.69]
    rlats = [16.42, 12.03, 8.02, 5.37, 4.23]
    anchor = 2
    anchor_tput = 1.29

    coverage_tputs = [0.2]  # no-lease tput
    for i in range(len(wlats)):
        tput = (wlats[anchor] / wlats[i]) * anchor_tput * put_ratio
        tput += (rlats[anchor] / rlats[i]) * anchor_tput * (1 - put_ratio)
        coverage_tputs.append(tput)

    return coverage_tputs


@dataclass
class SimulationParams:
    """Parameters for the server availability simulation."""

    bodega: bool
    max_coverage: int

    num_servers: int = 5  # Number of servers in the system
    coverage_tputs: List[float] = field(default_factory=get_coverage_tputs)

    failure_rate: float = 0.005  # Probability of failure per time unit
    recovery_rate: float = 0.01  # Probability of recovery per time unit
    expire_duration: float = (
        3.0  # Expiration downtime after each failure (time units)
    )
    change_duration: float = 0.1  # Regular roster change duration (time units)

    dt: float = 0.01  # Time step for simulation
    simulation_time: float = 1_000_000.0  # Total simulation time


@dataclass
class SimulationResult:
    """Result of the simulation run."""

    unavailability_percent: float
    estimated_throughput: float

    failure_count: int
    recovery_count: int


def simulate(params: SimulationParams) -> SimulationResult:
    """
    Monte Carlo simulation for system availability and throughput.

    Args:
        params: SimulationParams object containing all simulation parameters

    Returns:
        SimulationResult object
    """

    # Initialize system state
    servers = [True] * params.num_servers  # True = healthy, False = failed
    majority_count = (params.num_servers // 2) + 1
    max_coverage = params.max_coverage if params.bodega else 1

    current_time = 0.0
    last_expire_start = -params.expire_duration
    last_change_start = -params.change_duration
    total_downtime = 0.0
    total_requests = 0.0
    failure_count = 0
    recovery_count = 0

    # Run simulation
    while current_time < params.simulation_time:
        # Check if system is currently unavailable
        if (
            current_time - last_expire_start < params.expire_duration
            or current_time - last_change_start < params.change_duration
        ):
            # unavailability period
            total_downtime += params.dt
        else:
            # system not in lease expiration now, so accumulate requests
            num_responders = sum(servers[:max_coverage])
            total_requests += params.dt * params.coverage_tputs[num_responders]

        # Process each server's health status
        for i in range(params.num_servers):
            if servers[i]:  # Healthy server
                if (
                    sum(servers) > majority_count
                    and np.random.random() < params.failure_rate * params.dt
                ):
                    servers[i] = False  # Server fails
                    if i < max_coverage:
                        last_expire_start = current_time
                    failure_count += 1
            else:  # Failed server
                if np.random.random() < params.recovery_rate * params.dt:
                    servers[i] = True  # Server recovers
                    if i < max_coverage:
                        last_change_start = current_time
                    recovery_count += 1

        current_time += params.dt

    # Populate simulation result
    result = SimulationResult(
        unavailability_percent=(total_downtime / current_time) * 100,
        estimated_throughput=total_requests / current_time,
        failure_count=failure_count,
        recovery_count=recovery_count,
    )

    return result


def run_simulations(odir: str):
    """Run the simulation and display results"""
    log_path = f"{odir}/ftsim/result.log"

    # Set random seed for reproducibility
    np.random.seed(42)

    # Compute coverage-based throughputs
    coverage_tputs = get_coverage_tputs()

    def log_print(msg):
        """Print to both stdout and file"""
        print(msg)
        with open(log_path, "a") as flog:
            flog.write(msg + "\n")

    with open(log_path, "w") as _flog:
        pass  # Just create/clear the file

    log_print("Throughput if covered x nodes:")
    log_print(f" {[f'{tput:.3f}' for tput in coverage_tputs]}")

    # Run simulation for all settings
    for max_coverage in range(1, 6):
        for bodega in (True, False):
            params = SimulationParams(max_coverage=max_coverage, bodega=bodega)

            log_print(
                f"\nRunning simulation for coverage = {max_coverage}, bodega = {bodega}..."
            )

            start_time = time.time()
            result = simulate(params)
            runtime = time.time() - start_time

            log_print(" =>")
            log_print(
                f"  Unavailability:      {result.unavailability_percent:.3f}%"
            )
            log_print(
                f"  Throughput est.:     {result.estimated_throughput:.3f} kreqs/sec"
            )
            log_print(f"  Failure count:       {result.failure_count}")
            log_print(f"  Recovery count:      {result.recovery_count}")
            log_print(f"  Simulation runtime:  {runtime:.3f} seconds")


def parse_result_log(
    log_path: str,
) -> Tuple[List[float], Dict[Tuple[int, bool], float]]:
    """
    Parse the result.log file to extract:
    1. Original throughput list
    2. Dictionary mapping (coverage, bodega) -> throughput estimation

    Returns:
        Tuple of (throughput_list, simulation_results)
    """

    with open(log_path, "r") as flog:
        content = flog.read()

    # Parse the original throughput list
    throughput_pattern = r"Throughput if covered x nodes:\s*\['(.+?)'\]"
    throughput_match = re.search(throughput_pattern, content)

    if throughput_match:
        throughput_strings = throughput_match.group(1).split("', '")
        throughput_list = [float(t) for t in throughput_strings]
    else:
        throughput_list = []

    # Parse simulation results
    simulation_results = {}

    # Pattern to match simulation blocks
    simulation_pattern = r"Running simulation for coverage = (\d+), bodega = (True|False)\.\.\.[\s\S]*?Throughput est\.\s*:\s*([\d.]+) kreqs/sec"

    matches = re.findall(simulation_pattern, content)

    for match in matches:
        coverage = int(match[0])
        bodega = match[1] == "True"
        throughput = float(match[2])

        simulation_results[(coverage, bodega)] = throughput

    return throughput_list, simulation_results


def plot_results(odir: str):
    """
    Plot the results from the result log file.
    """
    log_path = f"{odir}/ftsim/result.log"
    throughput_list, simulation_results = parse_result_log(log_path)

    num_servers = len(throughput_list) - 1
    results = {
        "LeaderLeases-no-failure": [
            throughput_list[1] for _ in range(num_servers)
        ],
        "LeaderLeases-with-failure": [
            simulation_results[(i, False)] for i in range(1, num_servers + 1)
        ],
        "Bodega-no-failure": throughput_list[1:],
        "Bodega-with-failure": [
            simulation_results[(i, True)] for i in range(1, num_servers + 1)
        ],
    }
    print("Parsed results:")
    for case in results:
        print(f"  {case:>25s}:  ", end="")
        for tput in results[case]:
            print(f"{tput:.3f}  ", end="")
        print()

    matplotlib.rcParams.update(
        {
            "figure.figsize": (6.8, 2.3),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    _fig = plt.figure("Model-ftsim")

    CASES_ORDER = [
        "LeaderLeases-no-failure",
        "LeaderLeases-with-failure",
        "Bodega-no-failure",
        "Bodega-with-failure",
    ]
    CASES_LABEL_COLOR = {
        "LeaderLeases-no-failure": ("Leader Leases (no failures)", "pink"),
        "LeaderLeases-with-failure": (
            "Leader Leases (with failures)",
            "lightcoral",
        ),
        "Bodega-no-failure": ("Bodega (no failures)", "lightsteelblue"),
        "Bodega-with-failure": ("Bodega (with failures)", "cornflowerblue"),
    }

    x = np.arange(1, num_servers + 1)  # X positions for groups
    width = 0.2  # Width of each bar
    multiplier = 0

    for case in CASES_ORDER:
        label, color = CASES_LABEL_COLOR[case]
        offset = width * multiplier
        hatch = "xx" if "with-failure" in case else None
        _bars = plt.bar(
            x + offset,
            results[case],
            width,
            label=label,
            color=color,
            hatch=hatch,
            edgecolor="gray",
            linewidth=0,
        )
        multiplier += 1

    plt.xlabel("Number of Roster-Covered Replicas")
    plt.ylabel("Throughput (k reqs/s)")
    plt.xticks(
        x + width * 1.5, [str(i) for i in range(1, num_servers + 1)]
    )  # Center the x-tick labels
    plt.legend(bbox_to_anchor=(1.04, 0.5), loc="center left", handlelength=1.0)

    # Add vertical separation lines between groups
    for i in range(2, num_servers + 1):
        plt.axvline(x=i - 0.2, color="gray", linestyle="--", linewidth=0.5)

    # Remove top and right spines
    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    pdf_name = f"{odir}/ftsim/result.pdf"
    plt.savefig(pdf_name, bbox_inches="tight")
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default="./results",
        help="directory that holds logs and outputs",
    )
    parser.add_argument(
        "-p",
        "--plot",
        action="store_true",
        help="if set, do the plotting phase",
    )
    args = parser.parse_args()

    if not args.plot:
        run_simulations(args.odir)
    else:
        plot_results(args.odir)
