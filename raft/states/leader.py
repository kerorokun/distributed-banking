from __future__ import annotations
import raft.messages.append_entries as raft_append_entries
import raft.messages.request_vote as raft_request_vote
import raft.states.common as raft_state_common
import raft.states.state as raft_state
import server.node as node
import threading
import time
import logging as log


class RAFTLeader:
    """
    Represents the actions of a RAFT leader.
    """

    HEARTBEAT_TIME = 0.75

    def __init__(self,
                 state_machine: raft_state.RAFTStateMachine,
                 state: raft_state_common.RAFTStates,
                 node: node.Node):
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.timer = threading.Timer(
            RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.vote_lock = threading.Lock()
        self.next_index = []
        self.match_index = []

    def on_msg(self, sock: node.Connection, msg: str) -> None:
        if raft_append_entries.AppendEntriesRequest.does_match(msg):
            self.__on_append_entries_request(sock, msg)
        elif raft_request_vote.RequestVoteMessage.does_match(msg):
            self.__on_request_vote(sock, msg)

    def on_enter(self) -> None:
        log.debug(f"[RAFT] Node is now leader for {self.info.curr_term}.")
        self.__send_heartbeat()

    def on_exit(self) -> None:
        self.timer.cancel()

    def on_timeout(self) -> None:
        self.__send_heartbeat()

    def __on_append_entries_request(self, sock: node.Connection, msg: str) -> None:
        msg = raft_append_entries.AppendEntriesRequest.deserialize(msg)

        if msg.term > self.info.curr_term:
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.state_machine.change_to(raft_state_common.RAFTStates.FOLLOWER)

    def __on_request_vote(self, sock: node.Connection, msg: str) -> None:
        msg = raft_request_vote.RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Received vote request but already voted.")
            self.vote_lock.release()
        else:
            log.debug(
                f"[RAFT] Received request. Voting for {msg.candidate_id}.")
            reply = raft_request_vote.RequestVoteReply(msg.term, True)
            self.info.curr_term = msg.term
            self.server.send(
                sock, raft_request_vote.RequestVoteReply.serialize(reply))
            self.vote_lock.release()
            self.state_machine.change_to(raft_state_common.RAFTStates.FOLLOWER)

    def __send_heartbeat(self) -> None:
        msg = raft_append_entries.AppendEntriesRequest(
            self.info.curr_term, self.info.id, 0, None, 0, [])
        self.server.send_to_all(
            raft_append_entries.AppendEntriesRequest.serialize(msg))
        self.__restart_timer()

    def __restart_timer(self) -> None:
        self.timer.cancel()
        self.timer = threading.Timer(
            RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.timer.start()
