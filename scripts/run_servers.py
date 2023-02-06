import sys
import argparse
import subprocess
import atexit

SPAWNED_PROCS = []

def kill_spawned_procs():
    for proc in SPAWNED_PROCS:
        proc.kill()

def run_process(cmd):
    print("Run:", ' '.join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return proc

def launch_servers(protocol, api_ports, comm_ports):
    assert len(comm_ports) == len(api_ports)
    for (api_port, comm_port) in zip(api_ports, comm_ports):
        peers = []
        for peer_port in comm_ports:
            if peer_port != comm_port:
                peers += ['-n', f"localhost:{peer_port}"]
        
        cmd = ['cargo', 'run', '-p', 'summerset_server', '--', '-p', protocol, '-a', str(api_port), '-s', str(comm_port)]
        cmd += peers
        SPAWNED_PROCS.append(run_process(cmd))

    atexit.register(kill_spawned_procs)

def parse_ports_list(s):
    s = s.strip()
    if len(s) == 0:
        return []

    ports = []
    l = s.split(',')

    for port_str in l:
        port = None
        try:
            port = int(port_str)
        except:
            raise Exception(f"{port_str} is not a valid integer")
        
        if port <= 1024 or port >= 65536:
            raise Exception(f"{port} is not in the range of valid ports")
        
        ports.append(port)
    
    return ports

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--protocol', type=str, required=True, help="protocol name")
    parser.add_argument('-a', '--api_ports', type=str, required=True, help="list of server key-value API ports")
    parser.add_argument('-c', '--comm_ports', type=str, required=True, help="list of internal communication ports")
    args = parser.parse_args()

    api_ports = parse_ports_list(args.api_ports)
    comm_ports = parse_ports_list(args.comm_ports)
    if len(comm_ports) != len(api_ports):
        raise Exception("length of `comm_ports` does not match `api_ports`")
    
    print("NOTE: Type 'exit' to terminate all servers...")
    launch_servers(args.protocol, api_ports, comm_ports)
    
    while True:
        word = input()
        if word == 'exit':
            break

    sys.exit()      # triggers atexit handler
