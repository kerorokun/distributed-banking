from .state import RAFTState

class RAFTCandidate(RAFTState):
    
    def __init__(self, prev_state: RAFTState=None, node = None):
        RAFTState.__init__(self, prev_state=prev_state, server_node=node)
        self.num_votes = 0
        self.has_initiated = False
 
    def handle_message(self, sock, msg):
        print(msg)
        return self
