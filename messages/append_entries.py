from typing import NamedTuple, List
import json
from __future__ import annotations


class AppendEntriesMessage(NamedTuple):
    @classmethod
    def deserialize(cls, serialized: str) -> AppendEntriesMessage:
        _, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries = serialized.split()
        return cls(int(term),
                   leader_id,
                   int(prev_log_index),
                   prev_log_term,
                   int(leader_commit),
                   json.loads(entries))

    @staticmethod
    def serialize(msg: AppendEntriesMessage) -> str:
        return f"APPEND_ENTRIES {msg.term} {msg.leader_id} {msg.prev_log_index} {msg.prev_log_term} {msg.leader_commit} {msg.entries}"

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    leader_commit: int
    entries: List[str]
