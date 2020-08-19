from threading import Thread, Lock
from server.node import Node, Connection
from typing import List
from states.state import RAFTStateMachine
from states.common import RAFTStates
from collections import defaultdict
import typing
import random
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
        self.node = Node(ip, port,
                         on_connect=self.on_connect,
                         on_message=self.on_message,
                         on_disconnect=self.on_disconnect)
        self.id = id
        self.connected_ids = set()
        self.id_to_conn = {}
        self.conn_to_id = defaultdict(lambda: "")
        self.connected_lock = Lock()
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

    def on_connect(self, conn: Connection) -> None:
        self.node.send(conn, f"ID: {self.id}")

    def on_disconnect(self, conn: Connection) -> None:
        self.connected_lock.acquire()
        id = self.conn_to_id[conn]
        self.conn_to_id.pop(conn, None)
        self.id_to_conn.pop(id, None)
        self.connected_ids.discard(id)
        self.connected_lock.release()

    def on_message(self, conn: Connection, msg: str) -> None:
        if msg.startswith("ID"):
            self.__handle_new_conn(conn, msg)
        else:
            self.state_machine.on_msg(conn, msg)

    def __handle_new_conn(self, conn: Connection, greeting: str) -> str:
        _, id = greeting.split()
        with self.connected_lock:
            if ((id == self.id) or (id in self.connected_ids)):
                print(f"{id} already connected. Dropping...")
                self.node.disconnect(conn)
            else:
                print(f"Added new connection {id}")
                self.connected_ids.add(id)
                self.id_to_conn[id] = conn
                self.conn_to_id[conn] = id


if __name__ == "__main__":
    # Usage: main.py <id> <ip> <port> <others...>
    neighbors = []
    for i in range(4, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = RAFTNode(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    raft_node.start(neighbors)
