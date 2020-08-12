import socket
import random
import time
from .state import RAFTState
from .candidate import RAFTCandidate

class RAFTFollower(RAFTState):

    def __init__(self, prev_state=None, initial_timeout=0, node=None):
        RAFTState.__init__(self, prev_state, server_node=node)
        self.__target_time = time.time() + initial_timeout

    def step(self) -> RAFTState:
        if time.time() >= self.__target_time:
            # Timed out; go into the candidate state
            return RAFTCandidate(prev_state=self, node=self.server_node)
        return self

    def handle_msg(self, sock: socket.socket, msg: str) -> RAFTState:
        # TODO: Think of a more flexible way to determine the msg type
        if msg.startswith("APPEND_ENTRIES"):
            return self.handle_append_entries(sock, msg)
        elif msg.startswith("REQUEST_VOTE"):
            return self.handle_request_vote(sock, msg)
        return self

    def handle_request_vote(self, sock: socket.socket, msg: str) -> RAFTState:
        return self

    def handle_append_entries(self, sock: socket.socket, msg: str) -> RAFTState:
        # Double check that this is a valid append entries
        # If so, continue
        self.__target_time = time.time() + (random.uniform(150, 300) / 1000)
        return self
