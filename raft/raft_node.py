import threading
import typing
import collections
import sys
import time
import server.node as node
import raft.states.state as raft_state
import raft.states.common as raft_common
import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)

handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)


class RAFTNode:

    def __init__(self, id: str, ip: str, port: int):
        self.node = node.Node(ip, port,
                              on_connect=self.on_connect,
                              on_message=self.on_message,
                              on_disconnect=self.on_disconnect)
        self.id = id
        self.connected_ids = set()
        self.id_to_conn = {}
        self.conn_to_id = collections.defaultdict(lambda: "")
        self.connected_lock = threading.Lock()
        self.state_machine = None

    def start(self, initial_conns: typing.List = []):
        initial_conns = set(initial_conns)
        self.node.start(blocking=False)

        self.state_machine = raft_state.RAFTStateMachine(self.node)
        self.state_machine.state_info.id = self.id
        self.state_machine.state_info.cluster_size = len(initial_conns) + 1
        self.state_machine.change_to(raft_common.RAFTStates.FOLLOWER)

        # Connect to the other nodes in the group
        print(f"Connecting to: {initial_conns}")
        for conn in initial_conns:
            self.node.connect_to(conn[0], conn[1])
        time.sleep(3)

        while True:
            pass

    def on_connect(self, conn: node.Connection) -> None:
        self.node.send(conn, f"ID: {self.id}")

    def on_disconnect(self, conn: node.Connection) -> None:
        self.connected_lock.acquire()
        id = self.conn_to_id[conn]
        self.conn_to_id.pop(conn, None)
        self.id_to_conn.pop(id, None)
        self.connected_ids.discard(id)
        self.connected_lock.release()

    def on_message(self, conn: node.Connection, msg: str) -> None:
        if msg.startswith("ID"):
            self.__handle_new_conn(conn, msg)
        else:
            self.state_machine.on_msg(conn, msg)

    def __handle_new_conn(self, conn: node.Connection, greeting: str) -> str:
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
