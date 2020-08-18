from threading import Thread, Lock
from queue import Queue
from server.node import Node, ConnectionType
from typing import List
from states.state import RAFTStateMachine
from states.common import RAFTStates
import typing
import random
import socket
import sys
import time
import enum


class RAFTNode:

    def __init__(self, id: str, port: int):
        # TODO: Add the ability to specify the address of the node
        self.node = Node("localhost", port, self.handle_conn, blocking=False)
        self.id = id
        self.connected_ids = set()
        self.id_to_socket = {}
        self.connected_lock = Lock()
        self.state_lock = Lock()
        self.state_machine = None

    def start(self, initial_conns: List = []):
        initial_conns = set(initial_conns)
        self.node.start()

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

    def handle_conn(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        self.node.send(sock, f"ID: {self.id}")

        leftover = ''
        id = ""
        processed_greeting = False
        should_loop = True
        try:
            while should_loop:
                msgs, leftover = self.node.read(sock, leftover)
                for msg in msgs:
                    # Handle the first connection
                    if not processed_greeting:
                        id = self.__handle_new_conn(sock, msg)
                        processed_greeting = True
                        if not id:
                            should_loop = False
                    else:
                        self.state_machine.on_msg(sock, msg)
        except Exception as e:
            print(e)
            pass
        if id and id in self.connected_ids:
            print(f"Lost connection with {id}")
            with self.connected_lock:
                self.connected_ids.remove(id)
                self.id_to_socket.pop(id, None)

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
    # Usage: main.py <id> <port> <others...>
    neighbors = []
    for i in range(3, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(sys.argv[1], int(sys.argv[2]))
    raft_node.start(neighbors)
