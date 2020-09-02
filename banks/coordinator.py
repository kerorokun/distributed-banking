import sys
import threading
import json
import queue
import argparse
import time
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
        self.connected_branches = set()

        self.node.start(other_coordinators, blocking=False)

        # TODO: This pause is so that the coordinator can safely setup everything before
        # connecting to the branches
        # This is merely for now
        time.sleep(4)
        for branch in branches:
            self.node.node.connect_to(branch[0], branch[1])

        while True:
            pass

    def on_connect(self, node, conn):
        pass

    def on_disconnect(self, node, conn):
        # TODO: Handle disconnect of branches
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
        elif msg.startswith("CAN_COMMIT"):
            self.on_can_commit(sender_id, node, conn, msg)
        elif msg.startswith("COMMIT"):
            self.on_commit(sender_id, node, conn, msg)
        elif msg.startswith("BRANCH_CONN"):
            _, branch = msg.split()
            print(f"Added branch: {branch}")
            self.branch_to_conns[branch] = conn
            self.branch_queues[branch] = queue.Queue()

    def on_message(self, node, conn, msg):
        commit_msg = None

        if msg.startswith("BEGIN"):
            with self.timestamp_lock:
                timestamp = self.next_timestamp
                self.next_timestamp += 1
            commit_msg = f"BEGIN {timestamp}"

        elif msg.startswith("ABORT"):
            timestamp = self.conn_to_timestamp[conn]
            commit_msg = f"ABORT {timestamp}"

        elif msg.startswith("COMMIT"):
            # Start the two phase commit protocol
            timestamp = self.conn_to_timestamp[conn]
            commit_msg = f"CAN_COMMIT {timestamp}"

        elif msg.startswith("BANK"):
            self.on_bank_msg(node, conn, msg)
        elif msg.startswith("BRANCH_CONN"):
            self.node.commit(message_entry.MessageEntry(self.coord_name, conn, msg))
        elif msg.startswith("BRANCH_RESPONSE"):
            branch = msg.split()[1]
            if branch in self.branch_to_conns:
                self.branch_queues[branch].put(msg)
        
        if commit_msg:
            if not self.node.commit(message_entry.MessageEntry(self.coord_name, conn, commit_msg)):
                leader_conn = self.node.get_raft_leader_conn()
                node.send(conn, f"REDIRECT {leader_conn}")

    def on_bank_msg(self, node, conn, msg):
        branch = msg.split()[1]
        if not conn in self.conn_to_timestamp:
            node.send(conn, "No transaction started")
            return

        timestamp = self.conn_to_timestamp[conn]
        if branch in self.branch_to_conns:
            node.send(self.branch_to_conns[branch], f"{msg} {timestamp}")
            try:
                response = self.branch_queues[branch].get(timeout=10)
                if "ABORT" in response:
                    # Commit an ABORT to the log
                    self.node.commit(message_entry.MessageEntry(self.coord_name, conn, f"ABORT {timestamp}"))
                else:
                    node.send(conn, response)
            except queue.Empty:
                node.send(conn, "Failed due to timeout")
        else:
            node.send(conn, "FAILED: No branch")

    def on_begin(self, sender_id, node, conn, msg):
        print(f"[COORDINATOR] BEGINNING")
        _, timestamp = msg.split()
        timestamp = int(timestamp)
        with self.timestamp_lock:
            self.next_timestamp = max(self.next_timestamp, timestamp + 1)

        self.conn_to_timestamp[conn] = timestamp

        if sender_id == self.coord_name:
            node.send(conn, "BEGIN OK")

    def on_abort(self, sender_id, node, conn, msg):
        if sender_id == self.coord_name:
            _, timestamp = msg.split()
            timestamp = int(timestamp)
            response = self.abort(timestamp)
            node.send(conn, "ABORT OK")

    def abort(self, timestamp):
        # Notify all branches to abort timestamp
        msg = f"ABORT {timestamp}"
        self.node.node.send_to_conns(self.branch_to_conns.values(), msg)
        
        for branch_queue in self.branch_queues.values():
            response = branch_queue.get()
        return "OK"

    def on_can_commit(self, sender_id, node, conn, msg):
        if sender_id != self.coord_name:
            return

        # Start the commit procedure: Ask all the branches if they're ok committing
        print("CAN_COMMIT")
        _, timestamp = msg.split()
        msg = f"CAN_COMMIT {timestamp}"
        node.send_to_conns(self.branch_to_conns.values(), msg)
        
        should_abort = False
        for branch_queue in self.branch_queues.values():
            response = branch_queue.get()
            should_abort = should_abort and "ABORT" in response

        # Double checking if committing is ok
        next_step = "COMMIT" if not should_abort else "ABORT"
        print(f"CAN_COMMIT: {next_step}")
        self.node.commit(message_entry.MessageEntry(self.coord_name, conn, f"{next_step} {timestamp}"))
        print(f"CAN_COMMIT: committed")

    def on_commit(self, sender_id, node, conn, msg):
        if sender_id != self.coord_name:
            return

        print("COMMIT")
        _, timestamp = msg.split()
        node.send_to_conns(self.branch_to_conns.values(), msg)
        for branch_queue in self.branch_queues.values():
            response = branch_queue.get()
        node.send(conn, response)
        
