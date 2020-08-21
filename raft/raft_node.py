import server.node as node
import raft.messages.append_entries as append_entries
import raft.messages.request_vote as request_vote
import logging as log
import threading
import typing
import collections
import sys
import time
import enum
import random


class RAFTStates(enum.Enum):
    FOLLOWER = 0,
    CANDIDATE = 1,
    LEADER = 2


class CommitTask:

    DEFAULT_TIMEOUT = 5

    def __init__(self, timeout=5):
        self.timeout = timeout


class GroupConnectionInfo:

    def __init__(self, owner_id):
        self.id = owner_id
        self.connected_lock = threading.Lock()
        self.connected_ids = set()
        self.connected_conns = set()
        self.id_to_conn = {}
        self.conn_to_id = collections.defaultdict(lambda: "")

    def try_add_conn(self, id: str, conn: node.Connection):
        with self.connected_lock:
            if id == self.id or id in self.connected_ids:
                return False
            else:
                self.connected_ids.add(id)
                self.connected_conns.add(conn)
                self.id_to_conn[id] = conn
                self.conn_to_id[conn] = id
                return True

    def try_remove_conn(self, conn: node.Connection):
        with self.connected_lock:
            id = self.conn_to_id[conn]
            self.conn_to_id.pop(conn, None)
            self.id_to_conn.pop(id, None)
            self.connected_ids.discard(id)
            self.connected_conns.discard(conn)

    def get_conn(self, id):
        if not id in self.connected_ids:
            return None
        return self.id_to_conn[id]

    def get_conns(self):
        return self.connected_conns

    def get_id(self, conn):
        if not conn in self.connected_conns:
            return None
        return self.conn_to_id[conn]


class RAFTNode:

    HEARTBEAT_TIME = 0.75
    ELECTION_TIME_MIN = 1.5
    ELECTION_TIME_MAX = 3

    def __init__(self, id: str, ip: str, port: int,
                 on_message: typing.Callable[[node.Node, node.Connection, str], None] = None):
        self.id = id
        self.node = node.Node(ip, port,
                              on_connect=self.on_connect,
                              on_message=self.__on_message,
                              on_disconnect=self.on_disconnect)
        self.on_message_callback = on_message
        self.conns = GroupConnectionInfo(self.id)
        self.info = None
        self.raft_lock = threading.Lock()
        self.state = None
        self.timer = threading.Timer(0, self.__on_timeout)

        self.cluster_size = 0
        self.term = 0
        self.leader_id = ""
        self.voted_for_id = 0
        self.commit_index = 0
        self.log = []
        self.num_votes = 0
        self.target_votes = 0

    def start(self, initial_conns: typing.List = []):
        self.node.start(blocking=False)
        initial_conns = set(initial_conns)
        print(f"Connecting to: {initial_conns}")
        for conn in initial_conns:
            self.node.connect_to(conn[0], conn[1])
        time.sleep(3)

        self.cluster_size = 1 + len(initial_conns)
        self.state = RAFTStates.FOLLOWER
        self.__reset_election_timer()

        while True:
            pass

    def commit(self, msg: str) -> None:
        # Launches the work to come back and reply
        pass

    def on_connect(self, conn: node.Connection) -> None:
        self.node.send(conn, f"ID: {self.id}")

    def on_disconnect(self, conn: node.Connection) -> None:
        self.conns.try_remove_conn(conn)

    def __on_message(self, conn: node.Connection, msg: str) -> None:
        if msg.startswith("ID"):
            self.__on_new_conn(conn, msg)
        elif append_entries.AppendEntriesRequest.does_match(msg):
            self.__on_append_entries(conn, msg)
        elif append_entries.AppendEntriesReply.does_match(msg):
            self.__on_append_entries_reply(conn, msg)
        elif request_vote.RequestVoteMessage.does_match(msg):
            self.__on_request_vote(conn, msg)
        elif request_vote.RequestVoteReply.does_match(msg):
            self.__on_request_vote_reply(conn, msg)
        elif self.state != RAFTStates.LEADER:
            # Send a rejection message for now
            self.node.send(conn, "Not a leader")
        elif self.on_message_callback:
            self.on_message_callback(self.node, conn, msg)

    def __on_new_conn(self, conn: node.Connection, greeting: str):
        _, id = greeting.split()
        if not self.conns.try_add_conn(id, conn):
            print(f"{id} already connected. Dropping...")
            self.node.disconnect(conn)
        else:
            print(f"Added new connection {id}")

    def __on_timeout(self):
        self.raft_lock.acquire()
        if self.state == RAFTStates.LEADER:
            self.__send_heartbeat()
        else:
            self.state = RAFTStates.CANDIDATE
            self.__start_election()
        self.raft_lock.release()

    def __on_append_entries(self, conn: node.Connection, msg: str) -> bool:
        self.raft_lock.acquire()
        msg = append_entries.AppendEntriesRequest.deserialize(msg)

        if ((msg.term > self.term) or 
            (msg.term == self.term and self.state == RAFTStates.CANDIDATE)):
            self.term = msg.term
            self.leader_id = msg.leader_id
            self.state = RAFTStates.FOLLOWER
            self.__reset_election_timer()
        elif msg.term == self.term and self.state == RAFTStates.FOLLOWER:
            # TODO: Implement reply and checking of the log
            self.__reset_election_timer()
        self.raft_lock.release()

    def __on_append_entries_reply(self, conn: node.Connection, msg: str) -> bool:
        self.raft_lock.acquire()
        # TODO: Implement committing
        self.raft_lock.release()

    def __on_request_vote(self, conn: node.Connection, msg: str) -> bool:
        msg = request_vote.RequestVoteMessage.deserialize(msg)

        self.raft_lock.acquire()
        if msg.term <= self.term:
            log.debug("[RAFT] Received vote request but already voted.")
        else:
            log.debug(
                f"[RAFT] Received request. Voting for {msg.candidate_id}.")
            
            self.term = msg.term
            reply = request_vote.RequestVoteReply(msg.term, True)
            reply = request_vote.RequestVoteReply.serialize(reply)
            self.node.send(conn, reply)
            self.state = RAFTStates.FOLLOWER
            self.__reset_election_timer()
        self.raft_lock.release()

    def __on_request_vote_reply(self, conn: node.Connection, msg: str) -> bool:
        self.raft_lock.acquire()
        if self.state != RAFTStates.CANDIDATE:
            self.raft_lock.release()
            return

        msg = request_vote.RequestVoteReply.deserialize(msg)
        if msg.vote_granted and msg.term == self.term:
            self.num_votes += 1
            log.debug(f"[RAFT] Election: {self.num_votes}/{self.target_votes}")
            if self.num_votes >= self.target_votes:
                self.state = RAFTStates.LEADER
                self.__send_heartbeat()
        self.raft_lock.release()

    def __send_heartbeat(self):
        msg = append_entries.AppendEntriesRequest(self.term, self.id, 0, None, 0, [])
        msg = append_entries.AppendEntriesRequest.serialize(msg)
        self.node.send_to_conns(self.conns.get_conns(), msg)
        self.__reset_heartbeat_timer()

    def __start_election(self):
        self.term += 1

        msg = request_vote.RequestVoteMessage(self.term, self.id, 0, None)
        msg = request_vote.RequestVoteMessage.serialize(msg)
        self.node.send_to_conns(self.conns.get_conns(), msg)

        self.num_votes = 1
        self.target_votes = self.cluster_size // 2 + 1
        self.__reset_election_timer()
        log.debug(f"[RAFT] Election {self.term}: {self.num_votes}/{self.target_votes}")

    def __reset_heartbeat_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(RAFTNode.HEARTBEAT_TIME, self.__on_timeout)
        self.timer.start()

    def __reset_election_timer(self):
        self.timer.cancel()
        time = random.uniform(RAFTNode.ELECTION_TIME_MIN, RAFTNode.ELECTION_TIME_MAX)
        self.timer = threading.Timer(time, self.__on_timeout)
        self.timer.start()
