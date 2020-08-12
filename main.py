from threading import Thread, Lock
from queue import Queue
from server.node import Node, ConnectionType
from typing import List
import typing
from states.follower import RAFTFollower
import random
import socket
import sys
import time
import enum


class RAFTNode:

    def __init__(self, port: int, neighbors: List = []):
        self.node = Node("localhost", port, self.handle_conn, blocking=False)
        self.neighbors = neighbors
        self.connected_neighbors = set()
        self.leader_sock = None
        self.state_lock = Lock()
        self.state = RAFTFollower(initial_timeout=10, node=self.node)

        self.__start()

    def __start(self):
        self.node.start()

        # Connect to the other nodes in the group
        print(f"Connecting to: {self.neighbors}")
        for neighbor in self.neighbors:
            self.node.connect_to(neighbor[0], neighbor[1])

        while True:
            self.state = self.state.step()

    def handle_conn(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        leftover = ''
        self.node.send(sock, f"Hello from {self.node.ip}:{self.node.port}")
        while True:
            try:
                msgs, leftover = self.node.read(sock, leftover)
                for msg in msgs:
                    self.state = self.state.handle_msg(sock, msg)
            except IOError:
                break


if __name__ == "__main__":
    neighbors = []
    for i in range(2, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(int(sys.argv[1]), neighbors)
