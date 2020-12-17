import argparse
import math
import pickle
import numpy as np
import time
import random
import requests
import json
from multiprocessing import Process

from computers import *

# Load the pickle files
actions = pickle.load(open("data/actions.pickle", "rb"))
states = pickle.load(open("data/states.pickle", "rb"))
timestep = 0

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--correct-fraction", type=float, default=1.0, help="Fraction of correct flight computers (default 1.0).")
parser.add_argument("--flight-computers", type=int, default=3, help="Number of flight computers (default: 3).")
arguments, _ = parser.parse_known_args()


def readout_state():
    return states[timestep]


def execute_action(action):
    # print(action)
    # print(actions[timestep])
    for k in action.keys():
        assert(action[k] == actions[timestep][k])


def start_computer(id, n, state, random_computer=False):
    timeout = random.uniform(1.0, 1.5)
    heartbeat = 0.1
    scale = 1

    if random_computer:
        computer = random_flight_computer()
    else:
        computer = FlightComputer

    computer = computer(state, id, election_timeout=timeout*scale, heartbeat=heartbeat*scale)
    return computer


def allocate_flight_computers(arguments):
    flight_computers = []
    n_fc = arguments.flight_computers
    n_correct_fc = math.ceil(arguments.correct_fraction * n_fc)
    n_incorrect_fc = n_fc - n_correct_fc
    state = readout_state()

    for i in range(n_correct_fc):
        computer = start_computer(i, n_fc, state)
        flight_computers.append(computer)

    for _ in range(n_incorrect_fc):
        computer = start_computer(i, n_fc, state)
        flight_computers.append(computer)

    for computer in flight_computers:
        computer.daemon = True
        computer.start()
        peers = flight_computers[:]
        peers.remove(computer)
        for peer in peers:
            computer.add_peer(peer)

    return flight_computers

# Connect with Kerbal Space Program
flight_computers = allocate_flight_computers(arguments)


def select_leader():
    leader = 0
    while True:
        resp = requests.get(f'http://127.0.0.1:{5000 + leader}/get_leader')
        if resp.status_code != 200:
            continue

        resp_leader = json.loads(resp.content)['leader']
        if resp_leader != -1:
            return flight_computers[resp_leader]

        leader = (leader + 1) % len(flight_computers)


def request_action(leader, state):
    resp = requests.get(f'http://127.0.0.1:{5000 + leader}/request_action', data=json.dumps(state))
    if resp.status_code == 200:
        return json.loads(resp.content)
    return None


import time
time.sleep(1)

timestep = 1
complete = False
leader = select_leader()

try:
    while not complete:
        if timestep % 1000 == 0:
            print(timestep, '/', len(states))
        state = readout_state()
        state_decided = leader.decide_on_state(state)
        if not state_decided:
            print('State not decided!')
            continue
        action = leader.sample_next_action()
        if action is None:
            complete = True
            continue
        if leader.decide_on_action(action):
            execute_action(action)
            timestep += 1
        else:
            print('Action not decided!')
except Exception as e:
    import traceback
    traceback.print_exc()
except KeyboardInterrupt:
    pass

for computer in flight_computers:
    computer.stop()

time.sleep(1)

if complete:
    print("Success!")
else:
    print("Fail!")
