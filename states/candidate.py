from .common import RAFTStateInfo, RAFTStates, get_next_election_timeout
from .leader import RAFTLeader
from messages.request_vote import RequestVoteMessage, RequestVoteReply
from messages.append_entries import AppendEntriesMessage
from threading import Timer
import time
import random


class RAFTCandidate:

    def __init__(self, state_machine, state_info: RAFTStateInfo, node):
        self.state_machine = state_machine
        self.info = state_info
        self.server_node = node
        self.target_votes = -1
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)

    def on_enter(self):
        print("[RAFT] Node is now in candidate state.")
        self.__start_election()

    def on_exit(self):
        self.timer.cancel()

    def on_timeout(self):
        self.__start_election()

    def __start_election(self):
        self.info.curr_term += 1

        request = RequestVoteMessage(
            self.info.curr_term, self.info.id, 0, None)
        self.server_node.send_to_all(RequestVoteMessage.serialize(request))

        self.num_votes = 1
        self.target_votes = self.info.cluster_size // 2 + 1
        self.__restart_timer()
        print(f"[RAFT] Election: {self.num_votes}/{self.target_votes}")

    def on_msg(self, sock, msg):
        print(f"[RAFT-CANDIDATE] {msg}")
        if msg.startswith("REQUEST_VOTE_REPLY"):
            self.__on_vote_reply(sock, msg)

    def __on_vote_reply(self, sock, msg):
        reply = RequestVoteReply.deserialize(msg)
        print(reply)
        if reply.vote_granted:
            self.num_votes += 1
            print(f"[RAFT] Election: {self.num_votes}/{self.target_votes}")
            if self.num_votes >= self.target_votes:
                self.state_machine.change_to(RAFTStates.LEADER)

    def __restart_timer(self):
        self.timer.cancel()
        self.timer = Timer(get_next_election_timeout(), self.on_timeout)
        self.timer.start()

