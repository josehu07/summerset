#! /bin/env python3

import sys
import signal
import argparse

from subprocess import Popen

_CLUSTER = None

def _signal_handler(sig, _from):
    if _CLUSTER is None:
        print("Got signal {sig}, but no cluster is running...")
    else:
        print("Got signal. Stopping...")
        _CLUSTER.stop()
    sys.exit(0)

PROTOCOL_CONFIGS = {
    "RepNothing": lambda r, _: f"backer_path='/tmp/summerset.rep_nothing.{r}.wal'",
    "SimplePush": lambda r, n: f"backer_path='/tmp/summerset.simple_push.{r}.wal'+rep_degree={n-1}",
}

class Cluster:
    ''' For setting up a test server cluster '''

    def __init__(self, release = False, num_replicas = 3, 
                 protocol = "RepNothing", num_threads = 2) -> None:
        global _CLUSTER
        if _CLUSTER is not None:
            raise RuntimeError("Another cluster is running")
        _CLUSTER = self

        # kill replicas when the current process gets killed
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGQUIT, _signal_handler)

        if protocol not in PROTOCOL_CONFIGS:
            raise ValueError(f"unknown protocol name '{protocol}'")
        if num_replicas <= 0:
            raise ValueError(f"invalid number of replicas {num_replicas}")

        self.replicas: list[Popen] = []
        for id in range(num_replicas):
            cmd = ["cargo", "run", "-p", "summerset_server"]

            if release:
                cmd += ["-r"]

            cmd += [
                "--",
                f"--protocol={protocol}",
                f"--api-port={52700+id}",
                f"--smr-port={52800+id}",
                f"--id={id}",
                f"--config={PROTOCOL_CONFIGS[protocol](id, num_replicas)}",
                f"--threads={num_threads}",
            ]

            for pos in range(num_replicas):
                cmd += [f"--replicas=127.0.0.1:{52800+pos}"]

            replica = Popen(cmd)
            self.replicas.append(replica)


    def wait(self) -> None:
        signal.sigwait({signal.SIGINT, signal.SIGTERM, signal.SIGQUIT})


    def stop(self) -> None:
        global _CLUSTER

        for (pos, node) in enumerate(self.replicas):
            print(f"Shutting down node #{pos+1}...")
            node.terminate()
            node.wait()
            
        _CLUSTER = None
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--release", action="store_true")
    parser.add_argument("-n", "--replicas", type=int, default=3)
    parser.add_argument("-p", "--protocol", type=str, default="RepNothing")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()

    cluster = Cluster(release=args.release, num_replicas=args.replicas, 
                      protocol=args.protocol, num_threads=args.threads)
    cluster.wait()

