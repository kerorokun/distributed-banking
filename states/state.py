from typing import List


class RAFTState:

    def __init__(self,
                 id: str = "",
                 num_neighbors: int = 0,
                 curr_term: int = 0, 
                 voted_for_id: int = 0, 
                 commit_index: int = 0, 
                 log: List = []):
        self.id = id
        self.num_neighbors = num_neighbors
        self.curr_term = curr_term
        self.voted_for_id = voted_for_id
        self.commit_index = commit_index
        self.log = []