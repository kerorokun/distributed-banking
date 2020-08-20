from __future__ import annotations
import raft.messages.append_entries as raft_append_entries
import raft.messages.request_vote as raft_request_vote
import raft.states.common as raft_state_common
import raft.states.state as raft_state
import server.node as node
import threading
import random
import time
import logging as log


class RAFTFollower:

    def __init__(self,
                 state_machine: raft_state.RAFTStateMachine,
                 state: raft_state_common.RAFTStateInfo,
                 node: node.Node) -> None:
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.vote_lock = threading.Lock()
        self.timer = threading.Timer(
            raft_state_common.get_next_election_timeout(), self.on_timeout)

    def on_enter(self) -> None:
        log.debug(f"[RAFT] Node is now follower under {self.info.leader_id}.")
        self.timer.start()

    def on_exit(self) -> None:
        self.timer.cancel()

    def on_timeout(self) -> None:
        log.debug("[RAFT] Timed out")
        self.state_machine.change_to(raft_state_common.RAFTStates.CANDIDATE)

    def on_msg(self, conn: node.Connection, msg: str) -> None:
        if raft_append_entries.AppendEntriesRequest.does_match(msg):
            self.__on_append_msg(conn, msg)
        elif raft_request_vote.RequestVoteMessage.does_match(msg):
            self.__on_request_vote(conn, msg)

    def __on_request_vote(self, conn: node.Connection, msg: str) -> None:
        msg = raft_request_vote.RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Already voted")
        else:
            log.debug(
                f"[RAFT] Received request. Voting for {msg.candidate_id}")
            reply = raft_request_vote.RequestVoteReply(msg.term, True)
            self.__restart_timer()
            self.info.curr_term = msg.term
            self.server.send(
                conn, raft_request_vote.RequestVoteReply.serialize(reply))
        self.vote_lock.release()

    def __restart_timer(self) -> None:
        self.timer.cancel()
        self.timer = threading.Timer(
            raft_state_common.get_next_election_timeout(), self.on_timeout)
        self.timer.start()

    def __on_append_msg(self, conn: node.Connection, msg: str) -> None:
        msg = raft_append_entries.AppendEntriesRequest.deserialize(msg)
        if msg.term >= self.info.curr_term:
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.__restart_timer()
