import atexit
import multiprocessing
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import FrameType
from typing import IO, Any

# every process launched through `run_process()` or `run_process_over_ssh()`,
# for `terminate_spawned()`
SPAWNED: list[subprocess.Popen[bytes]] = []

# color codes in Summerset logs, ignored when matching outputs
ANSI_ESCAPES = re.compile(r"\x1b\[[0-9;]*m")

# log line of a Summerset server that has fully set up
SERVER_READY = re.compile(r"\((\d+)\) accepting clients")


class DrainedPopen(subprocess.Popen[bytes]):
    """A process whose captured outputs are drained in the background as they
    come (and also written to `forward` if given), so that it never blocks on a
    full pipe; `communicate()` returns them, and `wait_output()` waits on them."""

    def __init__(self, *args: Any, forward: IO[bytes] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.outputs: dict[str, bytearray] = {}
        self.outputs_cond = threading.Condition()
        self.drainers: list[threading.Thread] = []
        for name, stream in (("stdout", self.stdout), ("stderr", self.stderr)):
            if stream is not None:
                self.outputs[name] = bytearray()
                drainer = threading.Thread(
                    target=self.drain, args=(name, stream, forward), daemon=True
                )
                drainer.start()
                self.drainers.append(drainer)

    def drain(self, name: str, stream: IO[bytes], forward: IO[bytes] | None) -> None:
        for line in iter(stream.readline, b""):
            if forward is not None:
                forward.write(line)
                forward.flush()
            with self.outputs_cond:
                self.outputs[name] += line
                self.outputs_cond.notify_all()
        stream.close()
        with self.outputs_cond:
            self.outputs_cond.notify_all()

    def communicate(
        self, input: bytes | None = None, timeout: float | None = None
    ) -> tuple[bytes | None, bytes | None]:
        if input is not None:
            raise ValueError("input not supported on drained processes")
        self.wait(timeout=timeout)
        for drainer in self.drainers:
            drainer.join()
        out, err = self.outputs.get("stdout"), self.outputs.get("stderr")
        return (None if out is None else bytes(out), None if err is None else bytes(err))

    def wait_output(
        self, ready: Callable[[str], bool], timeout: float, name: str = "stderr"
    ) -> None:
        """Waits until `ready` holds for the output (decoded, without colors)
        so far, failing if the process exits or `timeout` secs pass first."""
        deadline = time.monotonic() + timeout
        with self.outputs_cond:
            while not ready(ANSI_ESCAPES.sub("", self.outputs[name].decode(errors="replace"))):
                if self.poll() is not None and not any(d.is_alive() for d in self.drainers):
                    raise RuntimeError(f"process exited with {self.returncode} before ready")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f"process not ready within {timeout} secs")
                self.outputs_cond.wait(min(remaining, 0.5))


def cleanup_on_exit() -> None:
    """Makes sure processes launched through `run_process()` get terminated
    whenever this script exits: normally, upon errors, upon termination
    signals, or when its parent process (e.g., a `uv run` wrapper) dies."""
    atexit.register(terminate_spawned)

    def handler(signum: int, _frame: FrameType | None) -> None:
        sys.exit(128 + signum)

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, handler)

    ppid = os.getppid()

    def watch_parent() -> None:
        while os.getppid() == ppid:
            time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=watch_parent, daemon=True).start()


def terminate_spawned(timeout: float = 5) -> None:
    """Terminates all processes launched through `run_process()`, killing any
    that has not exited within `timeout` secs."""
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal.SIG_IGN)  # do not get interrupted midway
    for proc in SPAWNED:
        if proc.poll() is None:
            proc.terminate()
    deadline = time.monotonic() + timeout
    for proc in SPAWNED:
        try:
            proc.wait(timeout=max(deadline - time.monotonic(), 0))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def kill_all_local_procs():
    # print("Killing all local procs...")
    cmd = "./scripts/kill_all_procs.sh"
    os.system(cmd)


def kill_all_distr_procs(
    group: str,
    targets: str = "all",
    chain: bool = False,
    cockroach: bool = False,
    etcd: bool = False,
    zookeeper: bool = False,
) -> None:
    # print(f"Killing all procs on {group} {targets}...")
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.remote_killall",
        "-g",
        group,
        "-t",
        targets,
    ]
    if chain:
        cmd.append("--chain")
    if cockroach:
        cmd.append("--cockroach")
    if etcd:
        cmd.append("--etcd")
    if zookeeper:
        cmd.append("--zookeeper")
    rc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    if rc != 0:
        raise RuntimeError(f"failed to kill procs on {group} {targets}")


def env_assigns(env: dict[str, str]) -> list[str]:
    """Composes an `env` command prefix that sets the given variables, which
    survives wrappers like `sudo` that reset the environment."""
    return ["env"] + [f"{k}={v}" for k, v in env.items()]


def run_process(
    cmd: list[str],
    cd_dir: str | None = None,
    capture_stdout: bool = False,
    capture_stderr: bool = False,
    print_cmd: bool = True,
    cpu_list: str | None = None,
    in_netns: str | None = None,
    extra_env: dict[str, str] | None = None,
    shell: bool = False,
    forward: IO[bytes] | None = None,
) -> DrainedPopen:
    stdout, stderr = None, None
    if capture_stdout:
        stdout = subprocess.PIPE
    if capture_stderr:
        stderr = subprocess.PIPE

    # summerset executables exit by themselves should this script die
    env = {"SUMMERSET_EXIT_WITH_PARENT": str(os.getpid())}
    if extra_env is not None:
        env.update(extra_env)

    pinned = cpu_list is not None and "-" in cpu_list
    in_netns = in_netns if in_netns is not None and len(in_netns) > 0 else None
    if pinned or in_netns is not None:
        cmd = env_assigns(env) + cmd

    if pinned:
        cmd = ["sudo", "taskset", "-c", cpu_list] + cmd

    if in_netns is not None:
        cmd = [s for s in cmd if s != "sudo"]
        cmd = ["sudo", "ip", "netns", "exec", in_netns] + cmd

    env_vars = os.environ.copy()
    env_vars.update(env)

    if print_cmd:
        print("Run:", " ".join(cmd))

    proc = DrainedPopen(
        cmd,
        cwd=cd_dir,
        stdout=stdout,
        stderr=stderr,
        env=env_vars,
        shell=shell,
        forward=forward,
    )
    SPAWNED.append(proc)
    return proc


def run_process_over_ssh(
    remote: str,
    cmd: list[str],
    cd_dir: str | None = None,
    capture_stdout: bool = False,
    capture_stderr: bool = False,
    print_cmd: bool = True,
    cpu_list: str | None = None,
    extra_env: dict[str, str] | None = None,
    forward: IO[bytes] | None = None,
) -> DrainedPopen:
    stdout, stderr = None, None
    if capture_stdout:
        stdout = subprocess.PIPE
    if capture_stderr:
        stderr = subprocess.PIPE

    if extra_env is not None and len(extra_env) > 0:
        cmd = env_assigns(extra_env) + cmd

    if cpu_list is not None and "-" in cpu_list:
        cmd = ["sudo", "taskset", "-c", cpu_list] + cmd

    if print_cmd:
        print(f"Run on {remote}: {shlex.join(cmd)}")

    # quote every argument, so that it reaches the remote program as is
    wrapped_cmd = shlex.join(cmd)
    if cd_dir is not None and len(cd_dir) > 0:
        wrapped_cmd = f"cd {shlex.quote(cd_dir)} && {wrapped_cmd}"
    wrapped_cmd = f". ~/.profile; {wrapped_cmd}"

    ssh_exec_cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        remote,
        wrapped_cmd,
    ]
    proc = DrainedPopen(ssh_exec_cmd, stdout=stdout, stderr=stderr, forward=forward)
    SPAWNED.append(proc)
    return proc


def wait_parallel_procs(
    procs: list[subprocess.Popen[bytes]],
    names: list[str] | None = None,
    check_rc: bool = True,
) -> None:
    """Waits for all given processes. If `check_rc`, prints the outputs of
    those that failed and raises an error."""
    failed = []
    for i, proc in enumerate(procs):
        name = f"proc {i}" if names is None else names[i]
        outputs = proc.communicate()
        if check_rc:
            if proc.returncode == 0:
                print(f"  {name}: OK")
            else:
                print(f"  {name}: ERROR")
                for output in outputs:
                    if output is not None:
                        print(output.decode())
                failed.append(name)
    if len(failed) > 0:
        raise RuntimeError(f"failed on {', '.join(failed)}")


def wait_cluster_ready(
    procs: list[DrainedPopen],
    num_replicas: int,
    settle_secs: float = 5,
    timeout: float = 120,
) -> None:
    """Waits until all `num_replicas` servers of each Summerset cluster launched
    by `procs` (e.g., `scripts.distr_cluster` over SSH with stderr captured)
    accept clients, then `settle_secs` more for leader step-up and leases."""
    for proc in procs:
        proc.wait_output(lambda err: len(set(SERVER_READY.findall(err))) >= num_replicas, timeout)
    time.sleep(settle_secs)


def wait_concurrent_clients(client_procs, timeout_secs=None, ts_start=None):
    rcs = []
    secs = None if ts_start is None else []

    def wait_and_time(p):
        rc = p.wait(timeout=timeout_secs)
        elapsed = None if ts_start is None else time.perf_counter() - ts_start
        return rc, elapsed

    with ThreadPoolExecutor(max_workers=len(client_procs)) as executor:
        futures = [executor.submit(wait_and_time, p) for p in client_procs]
        for future in as_completed(futures):
            rc, elapsed = future.result()
            rcs.append(rc)
            if secs is not None:
                secs.append(elapsed)

    return rcs, secs


def get_cpu_count(remote=None):
    cpus = multiprocessing.cpu_count()
    if remote is not None:
        cpus = int(
            run_process_over_ssh(
                remote,
                ["sudo", "nproc"],
                capture_stdout=True,
                capture_stderr=True,
                print_cmd=False,
            )
            .communicate()[0]
            .decode()
            .strip()
        )
    return cpus


def check_enough_cpus(expected, remote=None):
    cpus = get_cpu_count(remote=remote)
    if cpus < expected:
        print(f"WARN: expect >= {expected} CPUs, found {cpus} on {remote}")
