import numpy as np
import time
from threading import Thread
import requests
import json

from raft import Node, ActionRequest, ActionReply, CommitAction, CommitActionReply

class FlightComputer:
    def __init__(self, state, id, election_timeout=1, heartbeat=0.1):
        super().__init__()
        self.state = state
        self.current_stage_index = 0
        self.peers = []
        self.completed = True
        self.stage_handlers = [
            self._handle_stage_1,
            self._handle_stage_2,
            self._handle_stage_3,
            self._handle_stage_4,
            self._handle_stage_5,
            self._handle_stage_6,
            self._handle_stage_7,
            self._handle_stage_8,
            self._handle_stage_9]
        self.stage_handler = self.stage_handlers[self.current_stage_index]
        self.id = id
        self.raft = Node(id, self, election_timeout, heartbeat)

    def start(self):
        self.raft.start()

    def add_peer(self, peer):
        self.peers.append(peer)
        self.raft.add_peer(peer.id)

    def _handle_stage_1(self):
        action = {"pitch": 90, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        if self.state["altitude"] >= 1000:
            action["pitch"] = 80
            action["next_stage"] = True

        return action

    def _handle_stage_2(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Eject SRB's before the gravity turn
        if self.state["fuel_srb"] <= 1250:
            action["stage"] = True
            action["next_stage"] = True

        return action

    def _handle_stage_3(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Eject 2nd SRB + Initial Gravity turn
        if self.state["fuel_srb"] <= 10:
            action["stage"] = True
            action["pitch"] = 60.0
            action["next_stage"] = True

        return action

    def _handle_stage_4(self):
        action = {"pitch": 80, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        # Turn
        if self.state["altitude"] >= 25000:
            action["pitch"] = 0
            action["throttle"] = 0.75
            action["next_stage"] = True

        return action

    def _handle_stage_5(self):
        action = {"pitch": 0, "throttle": 0.75, "heading": 90, "stage": False, "next_state": False}
        # Cut throttle when apoapsis is 100km
        if self.state["apoapsis"] >= 100000:
            action["throttle"] = 0.0
            action["next_stage"] = True

        return action

    def _handle_stage_6(self):
        action = {"pitch": 0, "throttle": 0.0, "heading": 90, "stage": False, "next_state": False}
        # Drop stage
        if self.state["altitude"] >= 80000:
            action["stage"] = True
            action["next_stage"] = True

        return action

    def _handle_stage_7(self):
        action = {"pitch": 0, "throttle": 0.0, "heading": 90, "stage": False, "next_state": False}
        # Poor man's circularisation
        if self.state["altitude"] >= 100000:
            action["throttle"] = 1.0
            action["next_stage"] = True

        return action

    def _handle_stage_8(self):
        action = {"pitch": 0, "throttle": 1.0, "heading": 90, "stage": False, "next_state": False}
        if self.state["periapsis"] >= 90000:
            action["throttle"] = 0.0
            action["next_stage"] = True

        return action

    def _handle_stage_9(self):
        self.completed = True

    def sample_next_action(self):
        try:
            resp = requests.get(f'http://127.0.0.1:{5000 + self.id}/request_action', data=ActionRequest(state=self.state).json())
            if resp.status_code == 200:
                reply = ActionReply.parse_raw(resp.content)
                return reply.action
            else:
                return {}
        except requests.exceptions.RequestException:
            return {}

    def decide_on_state(self, state):
        self.deliver_state(state)
        return True

    def decide_on_action(self, action):
        try:
            resp = requests.get(f'http://127.0.0.1:{5000 + self.id}/commit_action', data=CommitAction(action=action).json())
            if resp.status_code == 200:
                reply = CommitActionReply.parse_raw(resp.content)
                return reply.result
            else:
                return False
        except requests.exceptions.RequestException as e:
            return False

    def acceptable_state(self, state):
        return True

    def acceptable_action(self, action):
        our_action = self.sample_next_action()
        accept = True
        for k in our_action.keys():
            if our_action[k] != action[k]:
                accept = False

        return accept
    
    def deliver_action(self, action, current_stage_index=None):
        if "next_stage" in action and action["next_stage"]:
            if current_stage_index is None:
                self.current_stage_index += 1
            else:
                self.current_stage_index = current_stage_index
            self.stage_handler = self.stage_handlers[self.current_stage_index]

    def deliver_state(self, state):
        self.state = state

    def stop(self):
        self.raft.stop()


class FullThrottleFlightComputer(FlightComputer):

    def __init__(self, state, id, election_timeout=1, heartbeat=0.1):
        super().__init__(state, id, election_timeout, heartbeat)

    def sample_next_action(self):
        action = super(FullThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = 1.0

        return action


class RandomThrottleFlightComputer(FlightComputer):

    def __init__(self, state, id, election_timeout=1, heartbeat=0.1):
        super().__init__(state, id, election_timeout, heartbeat)

    def sample_next_action(self):
        action = super(RandomThrottleFlightComputer, self).sample_next_action()
        action["throttle"] = np.random.uniform()

        return action


class SlowFlightComputer(FlightComputer):

    def __init__(self, state, id, election_timeout=1, heartbeat=0.1):
        super().__init__(state, id, election_timeout, heartbeat)

    def sample_next_action(self):
        action = super(SlowFlightComputer, self).sample_next_action()
        time.sleep(np.random.uniform() * 10) # Seconds

        return action


class CrashingFlightComputer(FlightComputer):

    def __init__(self, state, id, election_timeout=1, heartbeat=0.1):
        super().__init__(state, id, election_timeout, heartbeat)

    def sample_next_action(self):
        action = super(SlowFlightComputer, self).sample_next_action()
        # 1% probability of a crash
        if np.random.unifom() <= 0.01:
            raise Exception("Flight computer crashed")

        return action



def random_flight_computer():
    computers = [
        FullThrottleFlightComputer,
        RandomThrottleFlightComputer,
        SlowFlightComputer,
        CrashingFlightComputer,
    ]

    return computers[np.random.randint(0, len(computers))]
