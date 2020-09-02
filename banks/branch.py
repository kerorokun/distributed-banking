import collections
import threading
import enum
import raft.raft_node as raft_node
import banks.message_entry as message_entry


class TimestampAction(enum.Enum):
    OK = 1
    NOT_FOUND = 2
    ABORT = 3


class BranchAccount:

    def __init__(self):
        self.name = ""
        self.balance = 0
        self.rts = -1
        self.wts = -1


class TransactionState:
    def __init__(self):
        self.changes = collections.defaultdict(int)
        self.dependencies = set()
        self.new_accounts = set()

    def add_dependency(self, wts):
        self.dependencies.add(wts)


class Branch:

    def __init__(self, branch_name, branch_id, ip, port, other_branches):
        self.node = raft_node.RAFTNode(branch_id, ip, port, branch_name,
                                       on_connect=self.on_connect,
                                       on_commit=self.on_cmd_commit,
                                       on_message=self.on_message)
        self.branch_name = branch_name
        self.branch_id = branch_id

        # Setup the accounts
        self.accounts = collections.defaultdict(lambda: None)
        self.acc_lock = threading.Lock()
        self.ts_lock = threading.Lock()

        self.transaction_states = {}
        self.active_transactions = set()
        self.aborted_transactions = set()
        self.replies = {}
        self.timestamp_commit_events = {}

        self.node.start(other_branches, blocking=True)
    
    def on_connect(self, node, conn):
        node.send(conn, f"BRANCH_CONN {self.branch_name}")

    def try_read(self, acc, timestamp):
        with self.acc_lock:
            if not self.accounts[acc]:
                return (TimestampAction.NOT_FOUND, None)

            account = self.accounts[acc]

            if account.wts > timestamp:
                return (TimestampAction.ABORT, None)

            if timestamp not in self.active_transactions:
                self.active_transactions.add(timestamp)
                self.transaction_states[timestamp] = TransactionState()

            self.transaction_states[timestamp].add_dependency(account.wts)
            account.rts = max(account.rts, timestamp)
            return (TimestampAction.OK, account.balance)

    def try_add(self, acc, timestamp, amt):
        with self.acc_lock:
            new_account = False
            if not self.accounts[acc]:
                self.accounts[acc] = BranchAccount()
                new_account = True
            account = self.accounts[acc]

            if account.rts > timestamp:
                return (TimestampAction.ABORT, None)
            
            account.wts = timestamp
            if timestamp not in self.active_transactions:
                self.active_transactions.add(timestamp)
                self.transaction_states[timestamp] = TransactionState()

            curr_trans = self.transaction_states[timestamp]
            if new_account:
                curr_trans.new_accounts.add(acc)

            account.balance += amt
            curr_trans.changes[acc] += amt
            return (TimestampAction.OK, account.balance)

    def on_cmd_commit(self, conn_msg):
        print(f"[BRANCH]: {conn_msg}")
        msg_entry = message_entry.MessageEntry.from_str(conn_msg)
        conn = msg_entry.conn
        msg = msg_entry.msg
        sender_id = msg_entry.sender_id

        node = self.node.node
        # Handle messages from the client
        if msg.startswith("ABORT"):
            self.on_abort(sender_id, node, conn, msg)
        elif msg.startswith("BANK_DEPOSIT"):
            self.on_deposit(sender_id, node, conn, msg)
        elif msg.startswith("WITHDRAW"):
            self.on_withdraw(sender_id, node, conn, msg)
        elif msg.startswith("BALANCE"):
            self.on_balance(sender_id, node, conn, msg)
        elif msg.startswith("COMMIT"):
            self.on_commit(sender_id, node, conn, msg)
        elif msg.startswith("CAN_COMMIT"):
            self.on_can_commit(sender_id, node, conn, msg)

    def on_message(self, node, conn, msg):
        commit_msg = ""

        if msg.startswith("BANK_DEPOSIT"):
            print(msg)
            commit_msg = msg

            if not self.node.commit(message_entry.MessageEntry(self.branch_id, conn, commit_msg)):
                leader_conn = self.node.get_raft_leader_conn()
                node.send(conn, f"REDIRECT {leader_conn}")

    def send_response(self, node, conn, msg):
        node.send(conn, f"BRANCH_RESPONSE {self.branch_name} {msg}")

    def on_abort(self, sender_id, node, conn, msg):
        # Abort your actions
        pass

    def on_deposit(self, sender_id, node, conn, msg):
        _, _, acc, amt, timestamp = msg.split()
        timestamp, amt = int(timestamp), float(amt)
        response = ""

        success, result = self.try_read(acc, timestamp)
        if success != TimestampAction.ABORT:
            success, result = self.try_add(acc, timestamp, amt)

        if success != TimestampAction.ABORT:
            response = str(result)
        else:
            response = "ABORT"
        
        if sender_id == self.branch_id:
            self.send_response(node, conn, response)

    def on_withdraw(self, sender_id, node, conn, msg):
        pass

    def on_balance(self, sender_id, node, conn, msg):
        if sender_id == self.branch_id:
            node.send(conn, f"BRANCH_RESPONSE {self.branch_name} DEPOSITTED OK")

    def on_commit(self, sender_id, node, conn, msg):
        pass

    def on_can_commit(self, sender_id, node, conn, msg):
        pass
