from __future__ import annotations
from .common import RAFTStateInfo, RAFTStates, get_next_election_timeout
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from messages.append_entries import AppendEntriesRequest
from threading import Timer, Lock
import time
import random
import logging as log


class RAFTCandidate:
    """
    Represents the actions of a RAFT Candidate.
    """

    def __init__(self, state_machine, state_info, node):
        self.state_machine = state_machine
        self.info = state_info
        self.server = node
        self.target_votes = -1
        self.vote_lock = Lock()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)

    def on_enter(self):
        log.debug("[RAFT] Node is now in candidate state.")
        self.__start_election()

    def on_exit(self):
        self.timer.cancel()

    def on_timeout(self):
        self.timer.cancel()
        self.__start_election()

    def __start_election(self):
        self.info.curr_term += 1

        request = RequestVoteMessage(
            self.info.curr_term, self.info.id, 0, None)
        self.server.send_to_all(RequestVoteMessage.serialize(request))

        self.num_votes = 1
        self.target_votes = self.info.cluster_size // 2 + 1
        self.__restart_timer()
        log.debug(f"[RAFT] Election {self.info.curr_term}: {self.num_votes}/{self.target_votes}")

    def on_msg(self, sock, msg):
        if RequestVoteReply.does_match(msg):
            self.__on_vote_reply(sock, msg)
        elif RequestVoteMessage.does_match(msg):
            self.__on_vote_request(sock, msg)
        elif AppendEntriesRequest.does_match(msg):
            self.__on_append_entries_request(sock, msg)

    def __on_append_entries_request(self, sock, msg):
        msg = AppendEntriesRequest.deserialize(msg)

        if msg.term >= self.info.curr_term:
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.state_machine.change_to(RAFTStates.FOLLOWER)

    def __on_vote_reply(self, sock, msg):
        reply = RequestVoteReply.deserialize(msg)
        if reply.vote_granted and reply.term == self.info.curr_term:
            self.num_votes += 1
            log.debug(f"[RAFT] Election: {self.num_votes}/{self.target_votes}")
            if self.num_votes >= self.target_votes:
                self.state_machine.change_to(RAFTStates.LEADER)
    
    def __on_vote_request(self, sock, msg):
        msg = RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Received vote request but already voted.")
            self.vote_lock.release()
        else:
            log.debug(f"[RAFT] Received vote request. Voting for {msg.candidate_id}")
            reply = RequestVoteReply(msg.term, True)
            self.info.curr_term = msg.term
            self.server.send(sock, RequestVoteReply.serialize(reply))
            self.vote_lock.release()
            self.state_machine.change_to(RAFTStates.FOLLOWER)

    def __restart_timer(self):
        self.timer.cancel()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)
        self.timer.start()