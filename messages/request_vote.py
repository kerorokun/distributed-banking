from __future__ import annotations
from typing import NamedTuple, List
import json


class RequestVoteMessage(NamedTuple):
    @classmethod
    def deserialize(cls, serialized: str) -> RequestVoteMessage:
        _, term, leader_id, last_log_index, last_log_term = serialized.split()
        return cls(int(term),
                   leader_id,
                   int(last_log_index),
                   json.loads(last_log_term))

    @staticmethod
    def serialize(msg: RequestVoteMessage) -> str:
        return f"REQUEST_VOTE_REQUEST {msg.term} {msg.candidate_id} {msg.last_log_index} {json.dumps(msg.last_log_term)}"

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: object


class RequestVoteReply(NamedTuple):
    @classmethod
    def deserialize(cls, serialized: str) -> RequestVoteReply:
        _, term, vote_granted = serialized.split()
        return cls(int(term),
                   bool(vote_granted))

    @staticmethod
    def serialize(msg: RequestVoteMessage) -> str:
        return f"REQUEST_VOTE_REPLY {msg.term} {msg.vote_granted}"

    term: int
    vote_granted: bool
