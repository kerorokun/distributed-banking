from enum import Enum
from typing import List
import random


def get_next_election_timeout():
    return random.uniform(1.5, 3)


class RAFTStates(Enum):
    FOLLOWER = 0,
    CANDIDATE = 1,
    LEADER = 2

class RAFTStateInfo:

    def __init__(self,
                 id: str = "",
                 cluster_size: int = 0,
                 curr_term: int = 0,
                 leader_id: str = "",
                 voted_for_id: int = 0,
                 commit_index: int = 0,
                 log: List = []):
        self.id = id
        self.cluster_size = cluster_size
        self.curr_term = curr_term
        self.leader_id = leader_id
        self.voted_for_id = voted_for_id
        self.commit_index = commit_index
        self.log = []
