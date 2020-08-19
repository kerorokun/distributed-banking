from typing import List
from enum import Enum
from threading import Lock
from . import candidate, follower, leader
from .common import RAFTStateInfo, RAFTStates
import socket

class RAFTStateMachine:

    STATE_MAPPING = {
        RAFTStates.FOLLOWER: follower.RAFTFollower,
        RAFTStates.CANDIDATE: candidate.RAFTCandidate,
        RAFTStates.LEADER: leader.RAFTLeader
    }

    def __init__(self, server, initial_state_info=RAFTStateInfo()) -> None:
        self.server = server
        self.state_info = initial_state_info
        self.curr_state = None
        self.state_lock = Lock()

    def change_to(self, state_type: RAFTStates) -> None:
        if self.curr_state:
            self.curr_state.on_exit()
        self.curr_state = RAFTStateMachine.STATE_MAPPING[state_type](self, self.state_info, self.server)
        self.curr_state.on_enter()

    def on_msg(self, sock: socket.socket, msg: str) -> None:
        self.state_lock.acquire()
        self.curr_state.on_msg(sock, msg)
        self.state_lock.release()
