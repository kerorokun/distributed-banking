from __future__ import annotations
from typing import NamedTuple, List
import json


class AppendEntriesRequest(NamedTuple):

    MSG_PREFIX="APPEND_ENTRIES"

    @classmethod
    def deserialize(cls, serialized: str) -> AppendEntriesRequest:
        _, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries = serialized.split()
        return cls(int(term),
                   leader_id,
                   int(prev_log_index),
                   prev_log_term,
                   int(leader_commit),
                   json.loads(entries))

    @staticmethod
    def serialize(msg: AppendEntriesRequest) -> str:
        return f"{AppendEntriesRequest.MSG_PREFIX} {msg.term} {msg.leader_id} {msg.prev_log_index} {json.dumps(msg.prev_log_term)} {msg.leader_commit} {msg.entries}"

    @staticmethod
    def does_match(msg: str) -> bool:
        return msg.startswith(AppendEntriesRequest.MSG_PREFIX)

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: object
    leader_commit: int
    entries: List[str]

class AppendEntriesReply(NamedTuple):
    pass