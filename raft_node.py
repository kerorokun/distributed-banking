from threading import Thread, Lock
from server.node import Node, Connection
from typing import List
from states.state import RAFTStateMachine
from states.common import RAFTStates
import typing
import random
import socket
import sys
import time
import enum
import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)

handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)

class RAFTNode:

    def __init__(self, id: str, ip: str, port: int):
        self.node = Node(ip, port, on_connect=self.on_connect, on_message=self.on_message)
        self.id = id
        self.connected_ids = set()
        self.id_to_socket = {}
        self.connected_lock = Lock()
        self.state_lock = Lock()
        self.state_machine = None

    def start(self, initial_conns: List = []):
        initial_conns = set(initial_conns)
        self.node.start(blocking=False)

        self.state_machine = RAFTStateMachine(self.node)
        self.state_machine.state_info.id = self.id
        self.state_machine.state_info.cluster_size = len(initial_conns) + 1
        self.state_machine.change_to(RAFTStates.FOLLOWER)

        # Connect to the other nodes in the group
        print(f"Connecting to: {initial_conns}")
        for conn in initial_conns:
            self.node.connect_to(conn[0], conn[1])
        time.sleep(3)

        while True:
            pass

    def on_message(self, conn: Connection, msg: str) -> None:
        # self.node.send(sock, f"ID: {self.id}")
        self.state_machine.on_msg(conn, msg)

    def on_connect(self, conn: Connection) -> None:
        pass

    def __handle_new_conn(self, sock: socket.socket, greeting: str) -> str:
        _, id = greeting.split()
        with self.connected_lock:
            if ((id == self.id) or
                (id in self.connected_ids)):
                print(f"{id} already connected. Dropping...")
                id = None
            else:
                print(f"Added new connection {id}")
                self.connected_ids.add(id)
                self.id_to_socket[id] = sock
        return id


if __name__ == "__main__":
    # Usage: main.py <id> <ip> <port> <others...>
    neighbors = []
    for i in range(4, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    raft_node.start(neighbors)
