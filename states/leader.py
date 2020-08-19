from .common import RAFTStateInfo, RAFTStates
from messages.append_entries import AppendEntriesRequest
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from threading import Timer, Lock
import time
import logging as log


class RAFTLeader:
    """
    Represents the actions of a RAFT leader.
    """

    HEARTBEAT_TIME = 0.12

    def __init__(self, state_machine, state, node):
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.vote_lock = Lock()

    def on_msg(self, sock, msg) -> None:
        if AppendEntriesRequest.does_match(msg):
            self.__on_append_entries_request(sock, msg)
        elif RequestVoteMessage.does_match(msg):
            self.__on_request_vote(sock, msg)

    def on_enter(self) -> None:
        log.debug(
            f"[RAFT] Node is now in leader state for {self.info.curr_term}.")
        self.__send_heartbeat()

    def on_exit(self) -> None:
        self.timer.cancel()

    def on_timeout(self) -> None:
        self.__send_heartbeat()

    def __on_append_entries_request(self, sock, msg: str) -> None:
        msg = AppendEntriesRequest.deserialize(msg)

        if msg.term > self.info.curr_term:
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.state_machine.change_to(RAFTStates.FOLLOWER)

    def __on_request_vote(self, sock, msg: str) -> None:
        msg = RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Received vote request but already voted.")
            self.vote_lock.release()
        else:
            log.debug(
                f"[RAFT] Received request. Voting for {msg.candidate_id}.")
            reply = RequestVoteReply(msg.term, True)
            self.info.curr_term = msg.term
            self.server.send(sock, RequestVoteReply.serialize(reply))
            self.vote_lock.release()
            self.state_machine.change_to(RAFTStates.FOLLOWER)

    def __send_heartbeat(self) -> None:
        msg = AppendEntriesRequest(
            self.info.curr_term, self.info.id, 0, None, 0, [])
        self.server.send_to_all(AppendEntriesRequest.serialize(msg))
        self.__restart_timer()

    def __restart_timer(self) -> None:
        self.timer.cancel()
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.timer.start()
