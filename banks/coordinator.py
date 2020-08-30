import sys
import os
# NOTE: This is needed to allow this to be a standalone
sys.path.append(os.path.abspath(".."))

import raft.raft_node as raft_node
import server.node as node
import threading
import json


import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)

handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)

class MessageEntry:

    def __init__(self, sender_id, conn, msg):
        self.sender_id = sender_id
        self.conn = conn
        self.msg = msg
    
    def __repr__(self):
        return json.dumps({ "sender_id": self.sender_id, "conn": str(self.conn), "msg": self.msg })

    @classmethod
    def from_str(cls, serialized):
        json_obj = json.loads(serialized)
        return cls(json_obj["sender_id"], node.Connection.from_str(json_obj["conn"]), json_obj["msg"])

class Coordinator:

    def __init__(self, coord_name, ip, port, other_coordinators):
        self.coord_name = coord_name
        self.node = raft_node.RAFTNode(coord_name, ip, port,
                                       on_commit=self.on_cmd_commit,
                                       on_message=self.on_message)
        self.highest_timestamp = 0
        self.timestamp_commit_events = {}
        self.node.start(other_coordinators)

    def on_cmd_commit(self, conn_msg):
        print(f"[COORDINATOR]: {conn_msg}")
        msg_entry = MessageEntry.from_str(conn_msg)
        conn = msg_entry.conn
        msg = msg_entry.msg
        sender_id = msg_entry.sender_id
        node = self.node.node

        # Handle messages from the client
        if msg.startswith("ABORT"):
            self.on_abort(node, conn, msg)
        elif msg.startswith("DEPOSIT"):
            self.on_deposit(node, conn, msg)
        elif msg.startswith("WITHDRAW"):
            self.on_withdraw(node, conn, msg)
        elif msg.startswith("BALANCE"):
            self.on_balance(node, conn, msg)
        elif msg.startswith("COMMIT"):
            self.on_commit(node, conn, msg)
        elif msg.startswith("CAN_COMMIT"):
            self.on_can_commit(node, conn, msg)
        elif msg.startswith("BEGIN"):
            self.on_begin(sender_id, node, conn, msg)

    def on_message(self, node, conn, msg):
        if not self.node.commit(MessageEntry(self.coord_name, conn, msg)):
            leader_conn = self.node.get_raft_leader_conn()
            node.send(conn, f"REDIRECT {leader_conn}")

    def on_begin(self, sender_id, node, conn, msg):
        if sender_id == self.coord_name:
            node.send(conn, "BEGIN OK")
        

    def on_abort(self, node, conn, msg):
        
        pass

    def on_deposit(self, node, conn, msg):
        pass

    def on_withdraw(self, node, conn, msg):
        pass

    def on_balance(self, node, conn, msg):
        pass

    def on_commit(self, node, conn, msg):
        pass

    def on_can_commit(self, node, conn, msg):
        pass


if __name__ == "__main__":
    # Usage: main.py <id> <ip> <port> <others...>
    neighbors = []
    for i in range(4, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))

    coordinator = Coordinator(sys.argv[1], sys.argv[2], int(sys.argv[3]), neighbors)