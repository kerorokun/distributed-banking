import sys
import threading
import json
import queue
import argparse
import raft.raft_node as raft_node
import server.node as node
import banks.message_entry as message_entry


class Coordinator:

    def __init__(self, coord_name, ip, port, other_coordinators, branches):
        self.coord_name = coord_name
        self.node = raft_node.RAFTNode(coord_name, ip, port, "COORDINATOR",
                                       on_connect=self.on_connect,
                                       on_disconnect=self.on_disconnect,
                                       on_commit=self.on_cmd_commit,
                                       on_message=self.on_message)
        self.next_timestamp = 0
        self.conn_to_timestamp = {}
        self.timestamp_lock = threading.Lock()
        self.branch_queues = {}
        self.branch_to_conns = {}

        self.node.start(other_coordinators, blocking=False)

        for branch in branches:
            self.node.node.connect_to(branch[0], branch[1])

        while True:
            pass

    def on_connect(self, node, conn):
        pass

    def on_disconnect(self, node, conn):
        # TODO
        pass

    def on_cmd_commit(self, conn_msg):
        print(f"[COORDINATOR]: {conn_msg}")
        msg_entry = message_entry.MessageEntry.from_str(conn_msg)
        conn = msg_entry.conn
        msg = msg_entry.msg
        sender_id = msg_entry.sender_id
        node = self.node.node

        # Handle messages from the client
        if msg.startswith("ABORT"):
            self.on_abort(sender_id, node, conn, msg)
        elif msg.startswith("BEGIN"):
            self.on_begin(sender_id, node, conn, msg)
        elif msg.startswith("BRANCH_CONN"):
            _, branch = msg.split()
            print(f"Added branch: {branch}")
            self.branch_to_conns[branch] = conn

    def on_message(self, node, conn, msg):
        commit_msg = ""

        if msg.startswith("BEGIN"):
            with self.timestamp_lock:
                timestamp = self.next_timestamp
                self.next_timestamp += 1
            commit_msg = f"BEGIN {timestamp}"
            # Try to commit the updated msg
            if not self.node.commit(message_entry.MessageEntry(self.coord_name, conn, commit_msg)):
                leader_conn = self.node.get_raft_leader_conn()
                node.send(conn, f"REDIRECT {leader_conn}")
        elif msg.startswith("ABORT"):
            timestamp = self.conn_to_timestamp[conn]
            commit_msg = f"ABORT {timestamp}"
        elif msg.startswith("BANK"):
            print(msg)
            branch = msg.split()[1]
            if branch in self.branch_to_conns:
                node.send(self.branch_to_conns[branch], msg)
            else:
                node.send(conn, "FAILED: No branch")
        elif msg.startswith("BRANCH_CONN"):
            self.node.commit(message_entry.MessageEntry(self.coord_name, conn, msg))

    def on_begin(self, sender_id, node, conn, msg):
        _, timestamp = msg.split()
        timestamp = int(timestamp)
        with self.timestamp_lock:
            self.next_timestamp = max(self.next_timestamp, timestamp + 1)

        self.conn_to_timestamp[conn] = timestamp

        if sender_id == self.coord_name:
            node.send(conn, "BEGIN OK")

    def on_abort(self, sender_id, node, conn, msg):
        if sender_id == self.coord_name:
            node.send(conn, "ABORT OK")
