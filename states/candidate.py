from .state import RAFTState
from .leader import RAFTLeader
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from messages.append_entries import AppendEntriesMessage
import time
import random


class RAFTCandidate:

    def __init__(self, state: RAFTState, node):
        self.state = state
        self.server_node = node

        print("Node is now in candidate state.")
        print(self.state)
        # Start the election
        self.__start_election()

    def __start_election(self):
        self.num_votes = 1
        self.state.curr_term += 1
        self.election_timeout = time.time() + random.uniform(10, 30)

        print(f"Starting election. I need {self.state.num_neighbors // 2 + 1}")

        # Send messages to all connections
        request = RequestVoteMessage(
            self.state.curr_term, self.state.id, 0, None)
        self.server_node.send_to_all(RequestVoteMessage.serialize(request))

    def step(self):
        if time.time() >= self.election_timeout:
            self.__start_election()
            return self
        if self.num_votes > self.state.num_neighbors // 2:
            return self.handle_election_win()
        return self

    def handle_election_win(self):
        # Send leader to everyone
        request = AppendEntriesMessage(self.state.curr_term,
                                       self.state.id,
                                       0, None, 0, [])
        self.server_node.send_to_all(AppendEntriesMessage.serialize(request))
        return RAFTLeader(state=self.state, node=self.server_node)

    def handle_msg(self, sock, msg):
        if msg.startswith("REQUEST_VOTE_REPLY"):
            return self.handle_vote_reply(sock, msg)
        return self

    def handle_vote_reply(self, sock, msg):
        reply = RequestVoteReply.deserialize(msg)
        print(reply)
        if reply.vote_granted:
            self.num_votes += 1
            print("Received a vote")
            if self.num_votes > self.state.num_neighbors // 2:
                return self.handle_election_win()
        return self
