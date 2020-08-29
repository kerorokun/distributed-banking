from __future__ import annotations
import logging as log
import threading
import typing
import collections
import time
import enum
import random
import server.node as node
import raft.messages.append_entries as append_entries
import raft.messages.request_vote as request_vote


class RAFTStates(enum.Enum):
    FOLLOWER = 0,
    CANDIDATE = 1,
    LEADER = 2


class LogEntry:

    @classmethod
    def from_json(cls, json_obj):
        return cls(json_obj["term"], json_obj["val"])

    def __init__(self, term: int, val: str):
        self.term = term
        self.val = val

    def __repr__(self):
        return f"LogEntry(term: {self.term}, val: {self.val})"


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

    def get_ids(self):
        return self.connected_ids


class RAFTNode:

    HEARTBEAT_TIME = 0.75
    ELECTION_TIME_MIN = 1.5
    ELECTION_TIME_MAX = 3

    def __init__(self, id: str, ip: str, port: int,
                 on_commit: typing.Callable[[str]] = None,
                 on_connect: typing.Callable[[
                     node.Node, node.Connection], None] = None,
                 on_disconnect: typing.Callable[[
                     node.Node, node.Connection], None] = None,
                 on_message: typing.Callable[[node.Node, node.Connection, str], None] = None):
        self.id = id
        self.node = node.Node(ip, port,
                              on_message=self.__on_message,
                              on_disconnect=self.on_disconnect)
        self.on_apply_callback = on_commit
        self.on_connect_callback = on_connect
        self.on_disconnect_callback = on_disconnect
        self.on_message_callback = on_message
        self.conns = GroupConnectionInfo(self.id)
        self.info = None
        self.raft_lock = threading.Lock()
        self.state = None
        self.timer = threading.Timer(0, self.__on_timeout)

        self.cluster_size = 0
        self.term = 0
        self.leader_id = ""
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.num_votes = 0
        self.target_votes = 0

        self.next_indices = {}
        self.match_indices = {}
        self.tentative_nexts = {}

    def start(self, initial_conns: typing.List = []):
        self.node.start(blocking=False)
        initial_conns = set(initial_conns)
        log.debug(f"[RAFT] Connecting to: {initial_conns}")
        for conn in initial_conns:
            self.node.connect_to(conn[0], conn[1], greeting_msg=f"ID: {self.id}")
        time.sleep(3)

        self.cluster_size = 1 + len(initial_conns)
        self.__change_to(RAFTStates.FOLLOWER)

        while True:
            pass

    def is_raft_conn(self, conn):
        return 
    
    def commit(self, msg: str) -> bool:
        # Launches the work to come back and reply
        self.raft_lock.acquire()
        success = False
        if self.state == RAFTStates.LEADER:
            self.log.append(LogEntry(self.term, msg))
            success = True
        self.raft_lock.release()
        return success

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
            log.debug(f"[RAFT] {id} already connected. Dropping...")
            self.node.disconnect(conn)
        else:
            log.debug(f"[RAFT] Added new connection {id}")

    def __on_timeout(self):
        self.raft_lock.acquire()
        if self.state == RAFTStates.LEADER:
            self.__send_heartbeat()
        else:
            self.__change_to(RAFTStates.CANDIDATE)
        self.raft_lock.release()

    def __on_append_entries(self, conn: node.Connection, msg: str) -> bool:
        self.raft_lock.acquire()
        msg = append_entries.AppendEntriesRequest.deserialize(msg)
        # log.debug(f"[RAFT] {msg}")

        # [RPC (1) and (2)] Check if the AppendEntries is of interest
        success = False
        if ((msg.term >= self.term) and
            (msg.prev_log_index < len(self.log)) and
                (msg.prev_log_index < 0 or self.log[msg.prev_log_index].term == msg.prev_log_term)):
            success = True

            # Update the state if necessary
            if self.state != RAFTStates.FOLLOWER:
                self.term = msg.term
                self.leader_id = msg.leader_id
                self.__change_to(RAFTStates.FOLLOWER)

            # [RPC (3), (4), (5)] Update the log
            entries = [LogEntry.from_json(entry) for entry in msg.entries]
            self.log = self.log[:msg.prev_log_index+1] + entries
            self.commit_index = min(msg.leader_commit, len(self.log))

            for i in range(self.last_applied, self.commit_index):
                if self.on_apply_callback:
                    self.on_apply_callback(self.log[i].val)
            self.last_applied = self.commit_index

            self.__reset_election_timer()

        # Reply back to the request
        reply = append_entries.AppendEntriesReply(self.term, success)
        self.node.send(conn, reply.serialize())
        self.raft_lock.release()

    def __on_append_entries_reply(self, conn: node.Connection, msg: str) -> bool:
        self.raft_lock.acquire()
        # TODO: Consider the situation when two heartbeats are sent before a reply from the first
        if self.state != RAFTStates.LEADER:
            self.raft_lock.release()
            return

        msg = append_entries.AppendEntriesReply.deserialize(msg)
        c_id = self.conns.get_id(conn)
        if not msg.success:
            self.next_indices[c_id] -= 1
            self.raft_lock.release()
            return

        self.next_indices[c_id] = max(
            self.next_indices[c_id], self.tentative_nexts[c_id])
        self.match_indices[c_id] = max(
            self.match_indices[c_id], self.next_indices[c_id]-1)
        self.tentative_nexts[c_id] = -1

        # Check to see if a new commit can be included
        match = self.match_indices[c_id]
        if match > self.commit_index and self.log[match].term == self.term:
            num_match = sum(
                1 for _, e in self.match_indices.items() if e >= match)
            target_num = self.cluster_size // 2
            if num_match >= target_num:
                # log.debug(f"[RAFT] Committed: {match}")
                self.commit_index = match

                for i in range(self.last_applied, self.commit_index):
                   if self.on_apply_callback:
                       self.on_apply_callback(self.log[i].val)
                self.last_applied = self.commit_index

        self.raft_lock.release()

    def __on_request_vote(self, conn: node.Connection, msg: str) -> bool:
        msg = request_vote.RequestVoteMessage.deserialize(msg)

        self.raft_lock.acquire()
        log_up_to_date = msg.last_log_term > self.term
        log_up_to_date = log_up_to_date or (
            msg.last_log_term == self.term and msg.last_log_index >= len(self.log) - 1)

        if self.term < msg.term and log_up_to_date:
            log.debug(
                f"[RAFT] Received request. Voting for {msg.candidate_id}.")

            self.term = msg.term
            reply = request_vote.RequestVoteReply(msg.term, True)
            reply = request_vote.RequestVoteReply.serialize(reply)
            self.node.send(conn, reply)
            self.__change_to(RAFTStates.FOLLOWER)
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
                self.__change_to(RAFTStates.LEADER)
        self.raft_lock.release()

    def __change_to(self, state: RAFTStates) -> None:
        if state == RAFTStates.LEADER:
            self.state = RAFTStates.LEADER
            log.debug("[RAFT] Now in leader state")

            self.match_indices = {}
            self.next_indices = {}
            for id in self.conns.get_ids():
                self.next_indices[id] = len(self.log)
                self.match_indices[id] = 0
            self.__send_heartbeat()
        elif state == RAFTStates.CANDIDATE:
            self.state = RAFTStates.CANDIDATE
            log.debug("[RAFT] Now in candidate state")
            self.__start_election()
        elif state == RAFTStates.FOLLOWER:
            self.state = RAFTStates.FOLLOWER
            log.debug("[RAFT] Now in follower state")
            self.__reset_election_timer()

    def __send_heartbeat(self):
        for conn in self.conns.get_conns():
            c_id = self.conns.get_id(conn)
            entries = [self.log[i]
                       for i in range(self.next_indices[c_id], len(self.log))]
            prev_log_idx = self.next_indices[c_id]-1
            prev_log_term = self.log[prev_log_idx].term if prev_log_idx >= 0 else -1
            msg = append_entries.AppendEntriesRequest(self.term,
                                                      self.id,
                                                      prev_log_idx,
                                                      prev_log_term,
                                                      self.commit_index,
                                                      entries)
            self.node.send(conn, msg.serialize())
            self.tentative_nexts[c_id] = len(self.log)
        self.__reset_heartbeat_timer()

    def __start_election(self):
        self.term += 1

        last_log_idx = len(self.log) - 1
        last_log_term = self.log[last_log_idx].term if last_log_idx >= 0 else 0
        msg = request_vote.RequestVoteMessage(
            self.term, self.id, last_log_idx, last_log_term)
        msg = request_vote.RequestVoteMessage.serialize(msg)
        self.node.send_to_conns(self.conns.get_conns(), msg)

        self.num_votes = 1
        self.target_votes = self.cluster_size // 2 + 1
        self.__reset_election_timer()
        log.debug(
            f"[RAFT] Election {self.term}: {self.num_votes}/{self.target_votes}")

    def __reset_heartbeat_timer(self):
        self.timer.cancel()
        self.timer = threading.Timer(
            RAFTNode.HEARTBEAT_TIME, self.__on_timeout)
        self.timer.start()

    def __reset_election_timer(self):
        self.timer.cancel()
        time = random.uniform(RAFTNode.ELECTION_TIME_MIN,
                              RAFTNode.ELECTION_TIME_MAX)
        self.timer = threading.Timer(time, self.__on_timeout)
        self.timer.start()
