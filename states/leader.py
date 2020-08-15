from .common import RAFTStateInfo
from messages.append_entries import AppendEntriesMessage
from threading import Timer
import time


class RAFTLeader:

    HEARTBEAT_TIME = 0.3

    def __init__(self, state_machine, state, node):
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)

    def on_msg(self, sock, msg):
        pass

    def on_enter(self):
        print(f"Node is now in leader state for {self.info.curr_term}")
        self.__send_heartbeat()

    def on_exit(self):
        self.timer.cancel()
    
    def on_timeout(self):
        # Send heartbeat message
        self.__send_heartbeat()

    def __send_heartbeat(self):
        msg = AppendEntriesMessage(self.info.curr_term, self.info.id, 0, None, 0, [])
        self.server.send_to_all(AppendEntriesMessage.serialize(msg))
        self.__restart_timer()

    def __restart_timer(self):
        self.timer.cancel()
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.timer.start()


