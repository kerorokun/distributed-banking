from __future__ import annotations
import raft.states.state as raft_state
import raft.messages.request_vote as raft_request_vote
import raft.messages.append_entries as raft_append_entries
import raft.states.common as raft_state_common
import server.node as node
import threading
import logging as log


class RAFTCandidate:
    """
    Represents the actions of a RAFT Candidate.
    """

    def __init__(self, state_machine: raft_state.RAFTStateMachine, state_info: raft_state_common.RAFTStateInfo, node: node.Node) -> None:
        self.state_machine = state_machine
        self.info = state_info
        self.server = node
        self.target_votes = -1
        self.vote_lock = threading.Lock()
        self.timer = threading.Timer(raft_state_common.get_next_election_timeout(), self.on_timeout)

    def on_enter(self) -> None:
        log.debug("[RAFT] Node is now in candidate state.")
        self.__start_election()

    def on_exit(self) -> None:
        self.timer.cancel()

    def on_timeout(self) -> None:
        self.timer.cancel()
        self.__start_election()

    def __start_election(self) -> None:
        self.info.curr_term += 1

        request = raft_request_vote.RequestVoteMessage(
            self.info.curr_term, self.info.id, 0, None)
        self.server.send_to_all(raft_request_vote.RequestVoteMessage.serialize(request))

        self.num_votes = 1
        self.target_votes = self.info.cluster_size // 2 + 1
        self.__restart_timer()
        log.debug(
            f"[RAFT] Election {self.info.curr_term}: {self.num_votes}/{self.target_votes}")

    def on_msg(self, conn: node.Connection, msg: str) -> None:
        if raft_request_vote.RequestVoteReply.does_match(msg):
            self.__on_vote_reply(conn, msg)
        elif raft_request_vote.RequestVoteMessage.does_match(msg):
            self.__on_vote_request(conn, msg)
        elif raft_append_entries.AppendEntriesRequest.does_match(msg):
            self.__on_append_entries_request(conn, msg)

    def __on_append_entries_request(self, conn: node.Connection, msg: str) -> None:
        msg = raft_append_entries.AppendEntriesRequest.deserialize(msg)

        if msg.term >= self.info.curr_term:
            self.info.curr_term = msg.term
            self.info.leader_id = msg.leader_id
            self.state_machine.change_to(raft_state_common.RAFTStates.FOLLOWER)

    def __on_vote_reply(self, conn: node.Connection, msg: str) -> None:
        reply = raft_request_vote.RequestVoteReply.deserialize(msg)
        if reply.vote_granted and reply.term == self.info.curr_term:
            self.num_votes += 1
            log.debug(f"[RAFT] Election: {self.num_votes}/{self.target_votes}")
            if self.num_votes >= self.target_votes:
                self.state_machine.change_to(raft_state_common.RAFTStates.LEADER)

    def __on_vote_request(self, conn: node.Connection, msg: str) -> None:
        msg = raft_request_vote.RequestVoteMessage.deserialize(msg)

        self.vote_lock.acquire()
        if msg.term <= self.info.curr_term:
            log.debug("[RAFT] Received vote request but already voted.")
            self.vote_lock.release()
        else:
            log.debug(
                f"[RAFT] Received vote request. Voting for {msg.candidate_id}")
            reply = raft_request_vote.RequestVoteReply(msg.term, True)
            self.info.curr_term = msg.term
            self.server.send(conn, raft_request_vote.RequestVoteReply.serialize(reply))
            self.vote_lock.release()
            self.state_machine.change_to(raft_state_common.RAFTStates.FOLLOWER)

    def __restart_timer(self) -> None:
        self.timer.cancel()
        self.timer = threading.Timer(raft_state_common.get_next_election_timeout(), self.on_timeout)
        self.timer.start()
