from threading import RLock, main_thread
from repeat_timer import RepeatedTimer
from enum import Enum
import requests
import sys
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from time import monotonic as now
import json
from concurrent.futures import ThreadPoolExecutor
import random
from multiprocessing import Process


NodeState = Enum('NodeState', 'Follower Candidate Leader Dead')


class VoteRequest(BaseModel):
    candidate: int
    term: int


class VoteReply(BaseModel):
    granted: bool
    term: int


class AppendEntries(BaseModel):
    leader: int
    term: int


class AppendEntriesReply(BaseModel):
    success: bool
    term: int


class LeaderReply(BaseModel):
    leader: int


class ActionRequest(BaseModel):
    state: dict


class ActionReply(BaseModel):
    action: dict


class Node:
    def __init__(self, id, election_timeout, heartbeat):
        super().__init__()
        self.id = id
        self.election_timeout = election_timeout
        self._lock = RLock()
        self._time_last_requested_vote = now()
        self.state = NodeState.Follower
        self.peers = set()
        self._election_timer = RepeatedTimer(election_timeout,
                                             self._election_timer_timeout)
        self._leader_send_heartbeat_timer = RepeatedTimer(
                                                heartbeat,
                                                self._leader_send_heartbeat)
        self.term = 0
        self._voted_for = None
        self.leader = -1

    def add_peer(self, peer):
        """
        Add peer in the consensus cluster

        PARAMETERS
        peer: FlightComputer
            The peer to be add
        RETURN
            /
        """
        self.peers.add(peer)

    def start(self):
        """
        Start the node namely by initializing its FastApi server (see https://
        www.uvicorn.org/)

        PARAMETERS
            /
        RETURN
            /
        """
        print(f'[Node {self.id}][{self.state.name}] Starting with peers ' +
              f'{self.peers}')
        app = FastAPI()

        app.get('/vote_request')(self.get_vote_request)
        app.get('/append_entries')(self.get_append_entries)
        app.get('/get_leader')(self.get_leader)
        app.get('/request_action')(self.request_action)
        app.get('/stop')(self.stop)
        self._start_election_timer()
        self.proc = Process(target=uvicorn.run, args=(app, ),
                            kwargs={"host": "127.0.0.1",
                                    "port": "{}".format(5000 + self.id),
                                    "log_level": "error"}, daemon=True)
        self.proc.start()

    def get_leader(self):
        """
        Provides the consensus leader using the uvicorn REST API

        PARAMETERS
            /
        RETURN
        _: LeaderReply
            The leader format using LeaderReply
        """
        print(self.leader)
        return LeaderReply(leader=self.leader)

    def request_action(self, request: ActionRequest):
        """
        Change the state of the peer given an ActionRequest

        PARAMETERS
        /
        RETURN
        _: ActionReply
            The reply of the request made of nothing
        """
        state = request.state
        return ActionReply({})

    def stop(self):
        """
        Stop in an efficient way all processes used by self

        PARAMETERS
        /
        RETURN
        /
        """
        self._election_timer.stop()
        self._leader_send_heartbeat_timer.stop()
        self.state = NodeState.Dead
        self.proc.kill()
        # os.kill(self.proc.pid, signal.SIGTERM)

    def _start_election_timer(self):
        """
        Start the timer of an election used to avoid deadlock during this
        process

        PARAMETERS
        /
        RETURN
        /
        """
        # with self._lock:
        self._term_start_election_timer = self.term
        self._election_timer.start()

    def start_election(self):
        """
        Initiate the election of a leader in the consensus cluster

        PARAMETERS
        /
        RETURN
        /
        """
        print(f'[Node {self.id}][Candidate] Now Candidate')
        # with self._lock:
        self.state = NodeState.Candidate
        self.term += 1
        self.leader = -1
        saved_current_term = self.term

        # Always vote for ourself in elections
        self._time_last_requested_vote = now()
        self._voted_for = self.id

        def send_request(id):
            try:
                request = VoteRequest(candidate=self.id,
                                      term=saved_current_term)
                url = f'http://localhost:{5000 + id}/vote_request'
                resp = requests.get(url, data=request.json())
                if resp.status_code == 200:
                    reply = VoteReply.parse_raw(resp.content)
                    return reply
                else:
                    return None
            except:
                return None

        vote_received = 0
        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_request, self.peers)
            for reply in replies:
                if reply is None:
                    continue
                if reply.term > saved_current_term:
                    self.become_follower(reply.term)
                    return
                elif reply.term == saved_current_term and reply.granted:
                    vote_received += 1

        if(self.state == NodeState.Candidate and
           vote_received * 2 > len(self.peers)):
            # Won more than half of the votes, we are the leader now
            self.become_leader()
            return

        # Did win the election nor found a node with a higher term, let's start
        # a new election
        self._start_election_timer()

    def become_follower(self, term, leader):
        """
        Change the current state of self into a follower one

        PARAMETERS
        term: int
            The id of the current term
        leader: int
            The id of the current node in leader state
        RETURN
        /
        """
        self.state = NodeState.Follower
        self.term = term
        self.leader = leader
        self._voted_for = None
        self._time_last_requested_vote = now()

        # Start the election timer as a Follower switches to a Candidate if the
        # timer timeout
        self._start_election_timer()

    def become_leader(self):
        """
        Change the current state of self into a leader one

        PARAMETERS
        /
        RETURN
        /
        """
        print(f'[Node {self.id}][Leader] Now leader')
        self.state = NodeState.Leader
        self.leader = self.id
        self._leader_send_heartbeat_timer.start()

    def _leader_send_heartbeat(self):
        """
        Send the heartbeat as a leader to inform follower that it is still
        alive

        PARAMETERS
        /
        RETURN
        /
        """
        if self.state != NodeState.Leader:
            self._leader_send_heartbeat_timer.stop()
            return

        saved_current_term = self.term

        def send_heartbeat(id):
            try:
                request = AppendEntries(leader=self.id,
                                        term=saved_current_term)
                url = f'http://localhost:{5000 + id}/append_entries'
                resp = requests.get(url, data=request.json())
                if resp.status_code == 200:
                    reply = AppendEntriesReply.parse_raw(resp.content)
                    return reply
                else:
                    return None
            except:
                return None

        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_heartbeat, self.peers)
            for i, reply in enumerate(replies):
                if reply is None:
                    print(f'[Node {self.id}][Leader] did not receive a ' +
                          f'heartbeat response')
                    # Node is down/network partition
                    continue
                if reply.term > saved_current_term:
                    self.become_follower(reply.term, i)
                    return

    def get_vote_request(self, request: VoteRequest):
        """
        Get the vote of the election leader mechanism from all peers in the
        consensus cluster

        PARAMETERS
        request: VoteRequest
            A vote request made by a peer
        RETURN
        /
        """
        print(f'[Node {self.id}][{self.state.name}] Received vote request ' +
              f'from {request.candidate} at term {request.term}')
        # TODO Check state=dead ?
        if request.term > self.term:
            self.become_follower(request.term, request.candidate)

        if self.term == request.term and (self._voted_for is None or
           self._voted_for == request.candidate):
            reply = VoteReply(granted=True, term=self.term)
        else:
            reply = VoteReply(granted=False, term=self.term)

        return reply

    def get_append_entries(self, entry: AppendEntries):
        """
        Get the append entries of the election leader mechanism from all peers
        consensus cluster which are actions of computers

        PARAMETERS
        entry: AppendEntries
            An append entry from a client
        RETURN
        /
        """
        # TODO Check state=dead ?
        if entry.term > self.term:
            self.become_follower(entry.term, entry.leader)

        success = False
        if entry.term == self.term:
            if self.state == NodeState.Follower:
                self.become_follower(entry.term, entry.leader)
            self._time_last_requested_vote = now()
            success = True
        reply = AppendEntriesReply(success=success, term=self.term)

        return reply

    def _election_timer_timeout(self):
        """
        Trigger a timeout in the election mechanism and initiate a new one

        PARAMETERS
        /
        RETURN
        /
        """
        if self.state == NodeState.Leader:
            self._election_timer.stop()

        if self.term != self._term_start_election_timer:
            self._election_timer.stop()

        if now() - self._time_last_requested_vote >= self.election_timeout:
            '''
            Time elapsed since last VoteRequest received from the leader or
            from a candidate is more than timeout
            '''
            self._election_timer.stop()
            self.start_election()
