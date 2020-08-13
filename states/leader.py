from .state import RAFTState
from messages.append_entries import AppendEntriesMessage
import time


class RAFTLeader:

    HEARTBEAT_TIME = 0.12

    def __init__(self, state, node):
        self.state = state
        self.server_node = node
        self.next_heartbeat_time = None

        print("Node is now in leader state")

    def step(self):
        if not self.next_heartbeat_time:
            self.next_heartbeat_time = time.time() + RAFTLeader.HEARTBEAT_TIME
        if time.time() >= self.next_heartbeat_time:
            msg = AppendEntriesMessage(self.state.curr_term,
                                       self.state.id,
                                       0, None, 0, [])

            self.server_node.send_to_outgoing_conns(
                AppendEntriesMessage.serialize(msg))
            self.next_heartbeat_time = time.time() + RAFTLeader.HEARTBEAT_TIME
        return self

    def handle_msg(self, sock, msg):
        return self
