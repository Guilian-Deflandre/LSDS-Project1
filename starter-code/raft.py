from threading import RLock, Thread
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


NodeState = Enum('NodeState', 'Follower Candidate Leader')


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

class Node:
    def __init__(self, id, election_timeout=1, heartbeat=0.1):
        super().__init__()
        self.id = id
        self.election_timeout = election_timeout
        self._lock = RLock()
        self._time_last_requested_vote = None
        self.state = NodeState.Follower
        self.peers = set()
        self._election_timer = RepeatedTimer(election_timeout, self._election_timer_timeout)
        self._leader_send_heartbeat_timer = RepeatedTimer(heartbeat, self._leader_send_heartbeat)
        self.term = 0
        self._voted_for = None
    
    def add_peer(self, peer):
        self.peers.add(peer)
    
    def start(self):
        print(f'[Node {self.id}][{self.state.name}] Starting with peers {self.peers}')
        app = FastAPI()

        app.get('/vote_request')(self.get_vote_request)
        app.get('/append_entries')(self.get_append_entries)
        self._start_election_timer()
        uvicorn.run(app, host="127.0.0.1", port=5000 + self.id, log_level="error")
    
    def _start_election_timer(self):
        # with self._lock:
        self._term_start_election_timer = self.term
        self._election_timer.start()
    
    def start_election(self):
        print(f'[Node {self.id}][Candidate] Now Candidate')
        # with self._lock:
        self.state = NodeState.Candidate
        self.term += 1
        saved_current_term = self.term
        
        # Always vote for ourself in elections
        self._time_last_requested_vote = now()
        self._voted_for = self.id

        def send_request(id):
            try:
                request = VoteRequest(candidate=self.id, term=saved_current_term)
                resp = requests.get(f'http://localhost:{5000 + id}/vote_request', data=request.json())
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
        
        # with self._lock:
        if self.state == NodeState.Candidate and vote_received * 2 > len(self.peers):
            # Won more than half of the votes, we are the leader now
            self.become_leader()
            return

        # Did win the election nor found a node with a higher term, let's start a new election
        self._start_election_timer()
    
    def become_follower(self, term):
        print(f'[Node {self.id}][Follower] Now follower at term {term}')
        # with self._lock:
        self.state = NodeState.Follower
        self.term = term
        self._voted_for = None
        self._time_last_requested_vote = now()
        
        # Start the election timer as a Follower switches to a Candidate if the timer timeout
        self._start_election_timer()
    
    def become_leader(self):
        print(f'[Node {self.id}][Leader] Now leader')
        self.state = NodeState.Leader
        self._leader_send_heartbeat_timer.start()

    def _leader_send_heartbeat(self):
        print(f'[Node {self.id}][Leader] Sending heartbeats')
        # with self._lock:
        if self.state != NodeState.Leader:
            self._leader_send_heartbeat_timer.stop()
            return
        
        saved_current_term = self.term
        
        def send_heartbeat(id):
            try:
                request = AppendEntries(leader=self.id, term=saved_current_term)
                resp = requests.get(f'http://localhost:{5000 + id}/append_entries', data=request.json())
                if resp.status_code == 200:
                    reply = AppendEntriesReply.parse_raw(resp.content)
                    return reply
                else:
                    return None
            except:
                return None

        with ThreadPoolExecutor() as executor:
            replies = executor.map(send_heartbeat, self.peers)
            for reply in replies:
                if reply is None:
                    print(f'[Node {self.id}][Leader] did not receive a heartbeat response')
                    continue # Node is down/network partition
                if reply.term > saved_current_term:
                    self.become_follower(reply.term)
                    return
    
    def get_vote_request(self, request: VoteRequest):
        # with self._lock:
        print(f'[Node {self.id}][{self.state.name}] Received vote request from {request.candidate} at term {request.term}')
        # TODO Check state=dead ?
        if request.term > self.term:
            self.become_follower(request.term)
        
        if self.term == request.term and (self._voted_for is None or self._voted_for == request.candidate):
            reply = VoteReply(granted=True, term=self.term)
        else:
            reply = VoteReply(granted=False, term=self.term)
                
        return reply
    
    def get_append_entries(self, entry: AppendEntries):
        # with self._lock:
        # print(f'[Node {self.id}][{self.state.name}] Received AppendEntries from {entry.leader} at term {entry.term}')

        # TODO Check state=dead ?
        if entry.term > self.term:
            self.become_follower(entry.term)
        
        success = False
        if entry.term == self.term:
            if self.state == NodeState.Follower:
                self.become_follower(entry.term)
            self._time_last_requested_vote = now()
            success = True
        reply = AppendEntriesReply(success=success, term=self.term)

        return reply
    
    def _election_timer_timeout(self):
        # with self._lock:
        if self.state == NodeState.Leader:
            self._election_timer.stop()
        
        if self.term != self._term_start_election_timer:
            self._election_timer.stop()
        
        if self._time_last_requested_vote is None or now() - self._time_last_requested_vote >= self.election_timeout:
            # Time elapsed since last VoteRequest received from the leader or from a candidate is more than timeout
            self._election_timer.stop()
            self.start_election()

def start_node(id, n):
    timeout = random.uniform(1.0, 1.5)
    heartbeat = 0.1
    scale = 1
    node = Node(id, election_timeout=timeout*scale, heartbeat=heartbeat*scale)
    for i in range(n):
        if i != id:
            node.add_peer(i)
    node.start()

if __name__ == "__main__":
    from multiprocessing import Process
    nodes = []
    n = 3
    for i in range(n):
        p = Process(target=start_node, args=(i, n))
        p.start()
        nodes.append(p)

    try:
        for node in nodes:
            node.join()
    except KeyboardInterrupt:
        print('Killing processes...')
        for node in nodes:
            node.kill()
        for node in nodes:
            node.join()
