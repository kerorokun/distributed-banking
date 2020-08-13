from .state import RAFTState
from .candidate import RAFTCandidate
from messages.append_entries import AppendEntriesMessage
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from threading import Lock
import socket
import random
import time


class RAFTFollower:

    def __init__(self, state, node):
        self.state = state
        self.server_node = node
        self.target_time = None
        self.vote_lock = Lock()
        print(self.state.id)
        print("Node is now in follower state")

    def step(self):
        if not self.target_time:
            self.target_time = time.time() + random.uniform(.15, 3)
        if time.time() >= self.target_time:
            # Timed out; go into the candidate state
            print("Timed out. Changing to candidate state...")
            return RAFTCandidate(state=self.state, node=self.server_node)
        return self

    def handle_msg(self, sock: socket.socket, msg: str):
        # TODO: Think of a more flexible way to determine the msg type
        if msg.startswith("APPEND_ENTRIES"):
            return self.handle_append_entries(sock, msg)
        elif msg.startswith("REQUEST_VOTE_REQUEST"):
            return self.handle_request_vote(sock, msg)
        return self

    def handle_request_vote(self, sock: socket.socket, msg: str):
        msg = RequestVoteMessage.deserialize(msg)
        with self.vote_lock:
            if msg.term <= self.state.curr_term:
                print("Already voted")
                return self
        
        print("Received request. Voting for..")
        reply = RequestVoteReply(msg.term, True)
        with self.vote_lock:
            self.state.curr_term = msg.term
        self.target_time = time.time() + random.uniform(1, 3)
        self.server_node.send(sock, RequestVoteReply.serialize(reply))
        return self

    def handle_append_entries(self, sock: socket.socket, msg: str):
        # Double check that this is a valid append entries
        msg = AppendEntriesMessage.deserialize(msg)
        self.target_time = time.time() + (random.uniform(.15, 3))
        return self
