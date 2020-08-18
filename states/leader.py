from .common import RAFTStateInfo, RAFTStates
from messages.append_entries import AppendEntriesMessage
from threading import Timer
import time


class RAFTLeader:

    HEARTBEAT_TIME = 0.12

    def __init__(self, state_machine, state, node):
        self.state_machine = state_machine
        self.info = state
        self.server = node
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)

    def on_msg(self, sock, msg) -> None:
        if msg.startswith("APPEND_ENTRIES"):
            self.__on_append_entries_request(sock, msg)

    def on_enter(self) -> None:
        print(f"Node is now in leader state for {self.info.curr_term}")
        self.__send_heartbeat()

    def on_exit(self) -> None:
        self.timer.cancel()
    
    def on_timeout(self) -> None:
        # Send heartbeat message
        self.__send_heartbeat()

    def __on_append_entries_request(self, sock, msg: str) -> None:
        msg = AppendEntriesMessage.deserialize(msg)

        if msg.term > self.info.curr_term:
            self.state_machine.change_to(RAFTStates.FOLLOWER)

    def __send_heartbeat(self) -> None:
        msg = AppendEntriesMessage(self.info.curr_term, self.info.id, 0, None, 0, [])
        self.server.send_to_all(AppendEntriesMessage.serialize(msg))
        self.__restart_timer()

    def __restart_timer(self) -> None:
        self.timer.cancel()
        self.timer = Timer(RAFTLeader.HEARTBEAT_TIME, self.on_timeout)
        self.timer.start()


