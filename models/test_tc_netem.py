import multiprocessing
import threading
import time
import socket
import random
import argparse
import os

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


LOCALHOST = "192.168.0.1"
LOCALPORT = 42907

HEADER_LEN = 8
MSG_ID_LEN = 8
MSG_LEN = 1024
REPLY_LEN = 32

GAP_MS = 10
NUM_MSGS = 20000

DELAY_BASE = 10
DELAY_JITTERS = [1, 3, 5]
DISTRIBUTIONS = ["normal", "pareto", "paretonormal", "experimental"]
RATE_GBIT = 10


def set_tc_qdisc_netem(mean, jitter, distribution, rate):
    os.system(
        f"sudo tc qdisc replace dev dummy1 root netem limit 100000 "
        + f"delay {mean}ms {jitter}ms distribution {distribution} "
        + f"rate {rate}gbit 10"
    )


def clear_tc_qdisc_netem():
    os.system("sudo tc qdisc delete dev dummy1 root")


class Host:
    def __init__(self, conn_send, conn_recv):
        self.conn_send = conn_send
        self.conn_send.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.conn_recv = conn_recv
        self.conn_recv.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def gen_bytes(self, l):
        return bytearray((random.getrandbits(8) for _ in range(l)))

    def recv_bytes(self, l):
        data = b""
        while len(data) < l:
            chunk = self.conn_recv.recv(min(l - len(data), 65536))
            if chunk == b"":
                raise RuntimeError("socket connection broken")
            data += chunk
        return data

    def send_bytes(self, b):
        sent = 0
        while sent < len(b):
            chunk_len = self.conn_send.send(b[sent:])
            if chunk_len == 0:
                raise RuntimeError("socket connection broken")
            sent += chunk_len

    def recv_msg(self):
        header = self.recv_bytes(HEADER_LEN)
        msg_len = int.from_bytes(header, byteorder="big", signed=False)
        msg_id = self.recv_bytes(MSG_ID_LEN)
        msg_id = int.from_bytes(msg_id, byteorder="big", signed=False)
        msg = self.recv_bytes(msg_len)
        return msg_id, msg

    def send_msg(self, msg_id, msg):
        header = len(msg).to_bytes(HEADER_LEN, byteorder="big", signed=False)
        self.send_bytes(header)
        msg_id = msg_id.to_bytes(MSG_ID_LEN, byteorder="big", signed=False)
        self.send_bytes(msg_id)
        self.send_bytes(msg)


class Responder(Host):
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((LOCALHOST, LOCALPORT))
        self.sock.listen()

        conn_req, _ = self.sock.accept()
        conn_reply, _ = self.sock.accept()
        super().__init__(conn_reply, conn_req)

    def loop(self):
        while True:
            msg_id, msg = self.recv_msg()
            if msg == b"TERMINATE":
                break
            self.send_msg(msg_id, self.gen_bytes(REPLY_LEN))

    @classmethod
    def responder_func(cls):
        responder = Responder()
        responder.loop()


class Requester(Host):
    def __init__(self, msg_len, num_msgs, gap_ms):
        conn_req = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_req.connect((LOCALHOST, LOCALPORT))
        conn_reply = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_reply.connect((LOCALHOST, LOCALPORT))
        super().__init__(conn_req, conn_reply)

        self.msg_len = msg_len
        self.num_msgs = num_msgs
        self.gap_ms = gap_ms

        self.send_ts = [0 for _ in range(num_msgs)]
        self.recv_ts = [0 for _ in range(num_msgs)]

    def req_sender_func(self):
        for i in range(self.num_msgs):
            self.send_ts[i] = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            self.send_msg(i, self.gen_bytes(self.msg_len))

            time.sleep(self.gap_ms / 1000)

    def run(self, do_async=False):
        if do_async:
            t = threading.Thread(target=Requester.req_sender_func, args=[self])
            t.start()

        for i in range(self.num_msgs):
            if not do_async:
                self.send_ts[i] = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
                self.send_msg(i, self.gen_bytes(self.msg_len))

            msg_id, _ = self.recv_msg()
            assert msg_id == i
            self.recv_ts[msg_id] = time.clock_gettime_ns(time.CLOCK_MONOTONIC)

        if do_async:
            t.join()
        self.send_msg(0, b"TERMINATE")

        nanosecs = [self.recv_ts[i] - self.send_ts[i] for i in range(self.num_msgs)]
        return nanosecs


def plot_histogram(nanosecs, distribution, jitter, output_dir):
    millisecs = [s / 1000000 for s in nanosecs]
    # print(millisecs)

    plt.hist(millisecs, bins=100)

    plt.xlabel("ms")
    plt.xlim(0, max(millisecs) * 1.1)

    plt.savefig(f"{output_dir}/netem.{distribution}.{jitter}.png")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o", "--output_dir", type=str, default="./results", help="output folder"
    )
    parser.add_argument(
        "--do_async",
        action="store_true",
        help="if set, use 2-threads async mode requester",
    )
    args = parser.parse_args()

    for distribution in DISTRIBUTIONS:
        for jitter in DELAY_JITTERS:
            set_tc_qdisc_netem(DELAY_BASE, jitter, distribution, RATE_GBIT)

            responder_proc = multiprocessing.Process(target=Responder.responder_func)
            responder_proc.start()
            time.sleep(1)

            requester = None
            while requester is None:
                try:
                    requester = Requester(MSG_LEN, NUM_MSGS, GAP_MS)
                except ConnectionRefusedError:
                    requester = None
            nanosecs = requester.run(do_async=args.do_async)

            responder_proc.join()

            plot_histogram(nanosecs, distribution, jitter, args.output_dir)
            print(f"plotted {distribution} {jitter}")

    clear_tc_qdisc_netem()
