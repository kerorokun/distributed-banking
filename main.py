from threading import Thread, Lock
from queue import Queue
from node import Node, ConnectionType
from typing import List
import random
import socket
import sys
import time
import enum


class RAFTState:
    LEADER = 0,
    CANDIDATE = 1,
    FOLLOWER = 2

    def __init__(self, past_state = None):
        self.election_term = 0
        pass


class RAFTFollower(RAFTState):

    def __init__(self, initial_timeout=0):
        RAFTState.__init__(self)
        self.__target_time = time.time() + initial_timeout

    def step(self) -> RAFTState:
        if time.time() >= self.__target_time:
            # Timed out
            print("Timed out")
            pass            
        return self

    def handle_msg(self, node: Node, sock: socket.socket, msg: str) -> RAFTState:
        return self


class RAFTLeader(RAFTState):

    def __init__(self):
        pass


class RAFTCandidate(RAFTState):

    def __init__(self):
        pass


class RAFTNode:

    def __init__(self, port: int, neighbors: List = []):
        self.node = Node(port, self.handle_conn, blocking=False)
        self.neighbors = neighbors
        self.connected_neighbors = set()
        self.leader_sock = None
        self.timeout = 0
        self.election_term = 0
        self.state_lock = Lock()
        self.state = RAFTFollower(10)

        self.__start()

    def __start(self):
        self.node.start()

        # Connect to the other nodes in the group
        print(f"Connecting to: {self.neighbors}")
        for neighbor in self.neighbors:
            self.node.connect_to(neighbor[0], neighbor[1])

        while True:
            self.state = self.state.step()

    def __get_next_timeout(self) -> float:
        return random.uniform(150, 300)

    def handle_conn(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        leftover = ''
        self.node.send(sock, f"Hello from {self.node.ip}:{self.node.port}")
        while True:
            try:
                msgs, leftover = self.node.read(sock, leftover)
                for msg in msgs:
                    print(msg)
                    self.state = self.state.handle_msg(self.node, sock, msg)
                    print(self.state)
            except IOError:
                break


if __name__ == "__main__":
    neighbors = []
    for i in range(2, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(int(sys.argv[1]), neighbors)
