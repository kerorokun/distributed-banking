import raft.raft_node as raft_node
import threading


class MessageEntry:

    def __init__(self, conn, msg):
        self.conn = conn
        self.msg = msg

class Branch:

    def __init__(self, branch_name, ip, port, branches, addresses, ports):
        self.node = raft_node.RAFTNode(branch_name, ip, port,
                                       on_commit=self.on_cmd_commit,
                                       on_message=self.on_message)
        self.highest_timestamp = 0
        self.timestamp_commit_events = {}

    def on_cmd_commit(self, conn_msg):
        conn, msg = conn_msg
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
            self.on_begin(node, conn, msg)

    def on_message(self, node, conn, msg):
        if conn in node:
            pass
        if not self.node.commit(MessageEntry(conn, msg)):
            node.send(conn, "NOT_LEADER")

    def on_begin(self, node, conn, msg):
        pass

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
