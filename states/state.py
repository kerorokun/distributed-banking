class RAFTState:

    def __init__(self, prev_state = None, server_node = None) -> None:
        self.curr_term = prev_state.curr_term if prev_state else 0
        self.voted_for = None
        self.log = prev_state.log if prev_state else []
        self.commit_index = prev_state.commit_index if prev_state else 0
        self.last_applied = prev_state.last_applied if prev_state else 0
        self.server_node = server_node

    def step(self):
        return self

    def handle_message(self, sock, msg):
        return self
