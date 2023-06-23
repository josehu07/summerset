import sys
import signal

from subprocess import Popen

_CLUSTER = None

def _signal_handler(sig, _from):
    if _CLUSTER is None:
        print("Got signal {sig}, but no cluster is running...")
    else:
        print("Got signal. Stopping...")
        _CLUSTER.stop()
    sys.exit(0)


class Cluster:
    
    def __init__(self, buildtype, num_replicas = 3, protocol = "RepNothing", 
                 config = "", num_threads = 2) -> None:
        global _CLUSTER
        if _CLUSTER is not None:
            raise RuntimeError("Another cluster is running")
        _CLUSTER = self

        # kill replicas when the current process gets killed
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGQUIT, _signal_handler)

        self.replicas: list[Popen] = []
        for id in range(num_replicas):
            cmd = [
                f"./target/{buildtype}/summerset_server",
                f"--protocol={protocol}",
                f"--api-port={52700+id}",
                f"--smr-port={52800+id}",
                f"--id={id}",
                f"--config={config}",
                f"--threads={num_threads}",
            ]

            for pos in range(num_replicas):
                cmd += [f"--replicas=127.0.0.1:{52800+pos}"]

            print(cmd)
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
    cluster = Cluster("debug")
    cluster.wait()

