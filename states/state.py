from typing import List
from enum import Enum
from threading import Lock
from .candidate import RAFTCandidate
from .follower import RAFTFollower
from .leader import RAFTLeader
from .common import RAFTStateInfo, RAFTStates
import socket


class RAFTStateMachine:

    def __init__(self, server, initial_state_info=RAFTStateInfo()) -> None:
        self.server = server
        self.state_info = initial_state_info
        self.curr_state = None
        self.state_lock = Lock()

    def change_to(self, state_type: RAFTStates) -> None:
        if state_type == RAFTStates.FOLLOWER:
            if self.curr_state:
                self.curr_state.on_exit()
            self.curr_state = RAFTFollower(self, self.state_info, self.server)
            self.curr_state.on_enter()
        elif state_type == RAFTStates.CANDIDATE:
            if self.curr_state:
                self.curr_state.on_exit()
            self.curr_state = RAFTCandidate(self, self.state_info, self.server)
            self.curr_state.on_enter()
        elif state_type == RAFTStates.LEADER:
            if self.curr_state:
                self.curr_state.on_exit()
            self.curr_state = RAFTLeader(self, self.state_info, self.server)
            self.curr_state.on_enter()
        # Maybe apply an error here

    def on_msg(self, sock: socket.socket, msg: str) -> None:
        self.state_lock.acquire()
        self.curr_state.on_msg(sock, msg)
        self.state_lock.release()
