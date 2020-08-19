from messages.append_entries import AppendEntriesRequest
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from threading import Lock, Timer
from .common import get_next_election_timeout, RAFTStates
import socket
import random
import time
import logging as log


class RAFTFollower:

    def __init__(self, state_machine, state, node):
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.vote_lock = Lock()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)

    def on_enter(self):
        log.debug(f"[RAFT] Node is now in follower state under {self.info.leader_id}.")
        self.timer.start()

    def on_exit(self):
        self.timer.cancel()

    def on_timeout(self):
        log.debug("[RAFT] Timed out")
        self.state_machine.change_to(RAFTStates.CANDIDATE)

    def on_msg(self, sock: socket.socket, msg: str):
        if AppendEntriesRequest.does_match(msg):
            self.__on_append_msg(sock, msg)
        elif RequestVoteMessage.does_match(msg):
            self.__on_request_vote(sock, msg)

    def __on_request_vote(self, sock: socket.socket, msg: str):
        msg = RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Already voted")
        else:
            log.debug(f"[RAFT] Received request. Voting for {msg.candidate_id}")
            reply = RequestVoteReply(msg.term, True)
            self.__restart_timer()
            self.info.curr_term = msg.term
            self.server.send(sock, RequestVoteReply.serialize(reply))
        self.vote_lock.release()

    def __restart_timer(self):
        self.timer.cancel()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)
        self.timer.start()

    def __on_append_msg(self, sock: socket.socket, msg: str):
        # Double check that this is a valid append entries
        msg = AppendEntriesRequest.deserialize(msg)
        if msg.term >= self.info.curr_term:
            if msg.term > self.info.curr_term:
                log.debug(f"[RAFT] Node is now in follower state under {self.info.leader_id}.")
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.__restart_timer()
