from messages.append_entries import AppendEntriesMessage
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from threading import Lock, Timer
from .common import get_next_election_timeout, RAFTStates
import socket
import random
import time


class RAFTFollower:

    def __init__(self, state_machine, state, node):
        self.state_machine = state_machine
        self.state = state
        self.server = node
        self.vote_lock = Lock()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)

    def on_enter(self):
        print("Node is now in follower state")
        self.timer.start()

    def on_exit(self):
        pass

    def on_timeout(self):
        print("[RAFT] Timed out")
        self.state_machine.change_to(RAFTStates.CANDIDATE)

    def on_msg(self, sock: socket.socket, msg: str):
        # TODO: Think of a more flexible way to determine the msg type
        if msg.startswith("APPEND_ENTRIES"):
            self.__on_append_msg(sock, msg)
        elif msg.startswith("REQUEST_VOTE_REQUEST"):
            self.__on_request_vote(sock, msg)

    def __on_request_vote(self, sock: socket.socket, msg: str):
        msg = RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.state.curr_term:
            print("[RAFT] Already voted")
        else:
            print(f"[RAFT] Received request. Voting for {msg.candidate_id}")
            reply = RequestVoteReply(msg.term, True)
            self.__restart_timer()
            self.state.curr_term = msg.term
            self.server.send(sock, RequestVoteReply.serialize(reply))
        self.vote_lock.release()

    def __restart_timer(self):
        self.timer.cancel()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)
        self.timer.start()

    def __on_append_msg(self, sock: socket.socket, msg: str):
        # Double check that this is a valid append entries
        msg = AppendEntriesMessage.deserialize(msg)
        self.__restart_timer()
