import raft.raft_node as raft_node
import collections
import threading
import enum


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


class CommitEvent:

    def __init__(self, timeout=5):
        self.event = threading.Event()
        self.timer = threading.Timer(timeout, self.on_timeout)
        self.success = False

    def start(self):
        self.timer.start()
        self.event.wait()
        self.timer.cancel()
        return self.success

    def complete(self):
        self.success = True
        self.event.set()

    def on_timeout(self):
        self.success = False
        self.event.set()


class Branch:

    def __init__(self, branch_name, ip, port, branches, addresses, ports):
        self.node = raft_node.RAFTNode(branch_name, ip, port,
                                       on_commit=self.on_cmd_commit,
                                       on_message=self.on_message)

        # Setup the accounts
        self.accounts = collections.defaultdict(lambda: None)
        self.acc_lock = threading.Lock()
        self.ts_lock = threading.Lock()

        self.transaction_states = {}
        self.active_transactions = set()
        self.aborted_transactions = set()
        self.replies = {}
        self.highest_timestamp = 0
        self.timestamp_commit_events = {}

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
                return TimestampAction.ABORT
            
            account.wts = timestamp
            if timestamp not in self.active_transactions:
                self.active_transactions.add(timestamp)
                self.transaction_states[timestamp] = TransactionState()

            curr_trans = self.transaction_states[timestamp]
            if new_account:
                curr_trans.new_accounts.add(acc)

            account.balance += amt
            curr_trans.changes[acc] += amt
            return TimestampAction.OK

    def on_cmd_commit(self, val):
        self.highest_timestamp = max(self.highest_timestamp, val)

    def on_message(self, node, conn, msg):
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

    def on_begin(self, node, conn, msg):
        next_timestamp = self.highest_timestamp + 1
        if self.node.commit(next_timestamp):
            # Queue up an event and wait
            self.timestamp_commit_events[next_timestamp] = CommitEvent()
            if self.timestamp_commit_events[next_timestamp].start():
                node.send(conn, f"BEGIN {next_timestamp}")
                # TODO: Add a lock here
                self.highest_timestamp = next_timestamp
            else:
                node.send(conn, "ABORT")
        else:
            node.send(conn, "NOT_LEADER")

    def on_abort(self, node, conn, msg):
        # Abort your actions
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
