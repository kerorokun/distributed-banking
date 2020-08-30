from __future__ import annotations
import typing
import json


class AppendEntriesRequest(typing.NamedTuple):

    MSG_PREFIX = "APPEND_ENTRIES_REQ"
    EMPTY_ELEMENT = "{}"
    ENTRY_DELIMITER = "---"

    @classmethod
    def deserialize(cls, serialized: str) -> AppendEntriesRequest:
        _, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries = serialized.split(
            maxsplit=6)
        entries = entries.split(AppendEntriesRequest.ENTRY_DELIMITER)
        entries = [entry for entry in entries if entry != AppendEntriesRequest.EMPTY_ELEMENT]
        return cls(int(term),
                   leader_id,
                   int(prev_log_index),
                   int(prev_log_term),
                   int(leader_commit),
                   entries)

    @staticmethod
    def does_match(msg: str) -> bool:
        return msg.startswith(AppendEntriesRequest.MSG_PREFIX)

    def serialize(self) -> str:
        entries_dump = [str(entry) for entry in self.entries] if self.entries else [
            AppendEntriesRequest.EMPTY_ELEMENT]
        entries_dump = AppendEntriesRequest.ENTRY_DELIMITER.join(entries_dump)
        msg = f"{AppendEntriesRequest.MSG_PREFIX} {self.term} {self.leader_id} {self.prev_log_index} {self.prev_log_term} {self.leader_commit} {entries_dump}"
        return msg

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    leader_commit: int
    entries: typing.List[str]


class AppendEntriesReply(typing.NamedTuple):

    MSG_PREFIX = "APPEND_ENTRIES_REPLY"

    @classmethod
    def deserialize(cls, serialized: str) -> AppendEntriesReply:
        _, term, success = serialized.split()
        return cls(int(term), bool(success))

    @staticmethod
    def does_match(msg: str) -> bool:
        return msg.startswith(AppendEntriesReply.MSG_PREFIX)

    def serialize(self) -> str:
        return f"{AppendEntriesReply.MSG_PREFIX} {self.term} {self.success}"

    term: int
    success: bool
