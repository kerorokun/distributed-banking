from threading import Thread, Lock
from queue import Queue
from server.node import Node, ConnectionType
from typing import List
import typing
from states.follower import RAFTFollower
from states.leader import RAFTLeader
from states.state import RAFTState
import random
import socket
import sys
import time
import enum


class RAFTNode:

    def __init__(self, id: str, port: int, initial_conns: List = []):
        # TODO: Add the ability to specify the address of the node
        self.node = Node("localhost", port, self.handle_conn, blocking=False)
        self.id = id
        self.connected_id_to_socket = {}
        self.connected_lock = Lock()
        self.leader_id = ""

        initial_conns = set(initial_conns)
        self.state_info = RAFTState(id=self.id, num_neighbors=len(initial_conns)+1)
        self.state = RAFTFollower(state=self.state_info, node=self.node)

        self.state_lock = Lock()

        self.__start(initial_conns)

    def __start(self, initial_conns: List = []):
        self.node.start()

        # Connect to the other nodes in the group
        print(f"Connecting to: {initial_conns}")
        for conn in initial_conns:
            if not conn in self.connected_id_to_socket:
                self.node.connect_to(conn[0], conn[1])

        # TODO: Figure out a better way to wait for all the connections to be made
        time.sleep(3)

        while True:
            with self.state_lock:
                self.state = self.state.step()

    def handle_conn(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        self.node.send(sock, f"ID: {self.id}")

        leftover = ''
        id = ""
        processed_greeting = False
        should_continue = True
        while should_continue:
            try:
                msgs, leftover = self.node.read(sock, leftover)
                for msg in msgs:
                    # Handle the first connection
                    if not processed_greeting:
                        id = self.__handle_new_conn(sock, msg)
                        processed_greeting = True
                        if not id:
                            should_continue = False
                    
                    with self.state_lock:
                        self.state = self.state.handle_msg(sock, msg)
            except IOError:
                break
        if id:
            with self.connected_lock:
                self.connected_id_to_socket.pop(id, None)

    def __handle_new_conn(self, sock: socket.socket, greeting: str) -> str:
        _, id = greeting.split()
        with self.connected_lock:
            if id == self.id or id in self.connected_id_to_socket and self.connected_id_to_socket[id]:
                print(f"{id} already connected. Dropping...")
                id = None
            else:
                print(f"Added new connection {id}")
                self.connected_id_to_socket[id] = sock
        return id


if __name__ == "__main__":
    # Usage: main.py <id> <type> <port> <others...>
    neighbors = []
    for i in range(3, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(sys.argv[1], int(sys.argv[2]), neighbors)