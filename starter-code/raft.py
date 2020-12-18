from threading import Lock
from repeat_timer import RepeatedTimer
from enum import Enum
import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import typing
from time import monotonic as now
from concurrent.futures import ThreadPoolExecutor
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
    leader: bool


class ActionRequest(BaseModel):
    state: dict


class ActionReply(BaseModel):
    action: typing.Optional[dict]


class CommitAction(BaseModel):
    action: dict
    current_stage_index: typing.Optional[int]

class CommitActionReply(BaseModel):
    result: bool


class Node:
    def __init__(self, id, computer, election_timeout, heartbeat):
        super().__init__()
        self.id = id
        self.election_timeout = election_timeout
        self._lock = Lock()
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
        self.computer = computer
        self.session = requests.Session()

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
        app.get('/is_leader')(self.is_leader)
        app.get('/request_action')(self.request_action)
        app.get('/propose_action')(self.propose_action)
        app.get('/stop')(self.stop)
        app.get('/start_raft')(self.start_raft)
        app.get('/commit_action')(self.commit_action)
        app.get('/update_action')(self.update_action)
        self.proc = Process(target=uvicorn.run, args=(app, ),
                            kwargs={"host": "127.0.0.1",
                                    "port": "{}".format(5000 + self.id),
                                    "log_level": "error"}, daemon=False)
        self.proc.start()
    
    def start_raft(self):
        with self._lock:
            self._start_election_timer()

    def is_leader(self):
        """
        Predicate asserting that the node is the leader

        PARAMETERS
            /
        RETURN
        _: bool
            True if leader, False otherwise
        """

        from os import getpid
        from threading import get_native_id
        pid = getpid()
        tid = get_native_id()
        
        with self._lock:
            return LeaderReply(leader=self.state == NodeState.Leader)

    def request_action(self, request: ActionRequest):
        """
        Change the state of the peer given an ActionRequest

        PARAMETERS
        /
        RETURN
        _: ActionReply
            The reply of the request made of nothing
        """
        with self._lock:
            # print('Leader recv action request')
            if self.state != NodeState.Leader:
                return ActionReply(action={})

        action_leader = self.compute_action(request.state)

        def send_propose_action(id):
            try:
                url = f'http://localhost:{5000 + id}/propose_action'
                resp = self.session.get(url, data=request.json(), timeout=1)
                if resp.status_code == 200:
                    return ActionReply.parse_raw(resp.content)
                else:
                    return None
            except:
                return None

        vote_received = 1
        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_propose_action, self.peers)
            for reply in replies:
                if reply is None:
                    continue
                elif reply.action == action_leader:
                    vote_received += 1

                if self.state == NodeState.Leader and vote_received >= len(self.peers) / 2:
                    return ActionReply(action=action_leader)
        
        return ActionReply(action={})
    
    def propose_action(self, request: ActionRequest):
        """
        Compute the action associated to a state

        PARAMETERS
        request: ActionRequest
            The given state
        RETURN
        _: ActionReply
            The action to take for this node
        """
        return ActionReply(action=self.compute_action(request.state))
    
    def commit_action(self, request: CommitAction):
        """
        Commit the action to all peers

        PARAMETERS
        request: CommitAction
            The given action
        RETURN
        _: CommitActionReply
            True if the action is commited, False otherwise
        """
        with self._lock:
            if self.state != NodeState.Leader:
                return CommitActionReply(result=False)

        old_stage_index = self.computer.current_stage_index
        self.computer.deliver_action(request.action)
        request.current_stage_index = self.computer.current_stage_index

        def send_update_action(id):
            try:
                url = f'http://localhost:{5000 + id}/update_action'
                resp = self.session.get(url, data=request.json(), timeout=1)
                if resp.status_code == 200:
                    return CommitActionReply.parse_raw(resp.content)
                else:
                    return None
            except:
                return None

        commit_received = 1
        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_update_action, self.peers)
            for reply in replies:
                if reply is None:
                    continue
                elif reply.result:
                    commit_received += 1

                if self.state == NodeState.Leader and commit_received >= len(self.peers) / 2:
                    return CommitActionReply(result=True)
        
        # Commit failed, revert leader to previous stage_index
        self.computer.deliver_action(action, old_stage_index)
        return CommitActionReply(result=False)

    def update_action(self, request: CommitAction):
        """
        Update the state of the node with a given action

        PARAMETERS
        request: CommitAction
            The action to commit
        RETURN
        _: bool
            True if commited, False otherwise
        """
        self.computer.deliver_action(request.action, request.current_stage_index)
        return CommitActionReply(result=True)

    def compute_action(self, state):
        self.computer.deliver_state(state)
        action = self.computer.stage_handler()
        return action

    def stop(self):
        """
        Stop in an efficient way all processes used by self

        PARAMETERS
        /
        RETURN
        /
        """
        with self._lock:
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
        self._term_start_election_timer = self.term
        self._election_timer.restart()

    def start_election(self):
        """
        Initiate the election of a leader in the consensus cluster

        PARAMETERS
        /
        RETURN
        /
        """
        print(f'[Node {self.id}][Candidate] Now Candidate')
        self.state = NodeState.Candidate
        self.term += 1
        self.leader = -1
        saved_current_term = self.term
        self._leader_send_heartbeat_timer.stop()

        # Always vote for ourself in elections
        self._time_last_requested_vote = now()
        self._voted_for = self.id

        def send_request(id):
            try:
                request = VoteRequest(candidate=self.id,
                                      term=saved_current_term)
                url = f'http://localhost:{5000 + id}/vote_request'
                resp = self.session.get(url, data=request.json(), timeout=1)
                if resp.status_code == 200:
                    reply = VoteReply.parse_raw(resp.content)
                    return id, reply
                else:
                    return None
            except:
                return None

        vote_received = 1
        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_request, self.peers)
            for id, reply in replies:
                if reply is None:
                    continue
                if reply.term > saved_current_term:
                    self.become_follower(reply.term, id)
                    return
                elif reply.term == saved_current_term and reply.granted:
                    vote_received += 1

                if (self.state == NodeState.Candidate and
                vote_received * 2 > len(self.peers)):
                    # Won more than half of the votes, we are the leader now
                    self.become_leader()
                    return

        # Didn't win the election nor found a node with a higher term, let's start
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
        # if self.state != NodeState.Follower or self.term != term or self.leader != leader:
        print(f'[Node {self.id}][Follower] Now follower')
        self.state = NodeState.Follower
        self.term = term
        self.leader = leader
        self._voted_for = None
        self._time_last_requested_vote = now()

        # Start the election timer as a Follower switches to a Candidate if the
        # timer timeout
        self._start_election_timer()
        self._leader_send_heartbeat_timer.stop()
    
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
        with self._lock:
            if self.state != NodeState.Leader:
                self._leader_send_heartbeat_timer.stop()
                return

            saved_current_term = self.term

        def send_heartbeat(id):
            try:
                request = AppendEntries(leader=self.id,
                                        term=saved_current_term)
                url = f'http://localhost:{5000 + id}/append_entries'
                resp = self.session.get(url, data=request.json())
                if resp.status_code == 200:
                    reply = AppendEntriesReply.parse_raw(resp.content)
                    return id, reply
                else:
                    return None
            except:
                return None

        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_heartbeat, self.peers)
            for id, reply in replies:
                if reply is None:
                    print(f'[Node {self.id}][Leader] did not receive a ' +
                          f'heartbeat response')
                    # Node is down/network partition
                    continue
                if reply.term > saved_current_term:
                    self.become_follower(reply.term, id)
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
        print('vote')
        with self._lock:
            print(f'[Node {self.id}][{self.state.name}] Received vote request ' +
                f'from {request.candidate} at term {request.term}')
            
            if request.term > self.term:
                print(f'[Node {self.id}]get vote request: req.term > self.term', request.term, self.term)
                self.become_follower(request.term, request.candidate)

            if self.term == request.term and (self._voted_for is None or self._voted_for == request.candidate):
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
        with self._lock:
            
            if entry.term > self.term:
                print(f'[Node {self.id}]get append entries: entry.term > self.term', entry.term, self.term)
                self.become_follower(entry.term, entry.leader)

            success = False
            if entry.term == self.term:
                if self.state != NodeState.Follower:
                    print(f'[Node {self.id}]get append entries: refollowing', entry)
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
        with self._lock:
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
