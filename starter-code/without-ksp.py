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
    for k in action.keys():
        try:
            assert(action[k] == actions[timestep][k])
        except Exception as e:
            print(k)
            # print(action[k])
            # print(actions[timestep-1][k])
            print(action)
            print(timestep)
            for i in range(-5, 5):
                print(('> ' if i == 0 else '') + str(actions[timestep+i]))
            raise e


def start_computer(id, n, state, random_computer=False):
    timeout = random.uniform(1.0, 1.5)
    heartbeat = 0.1
    scale = 1

    if random_computer:
        computer = random_flight_computer()
    else:
        computer = FlightComputer
    print(computer)
    computer = computer(state, id, election_timeout=timeout*scale, heartbeat=heartbeat*scale)
    return computer


def allocate_flight_computers(arguments):
    flight_computers = []
    n_fc = arguments.flight_computers
    n_correct_fc = math.ceil(arguments.correct_fraction * n_fc)
    state = readout_state()

    for i in range(n_correct_fc):
        computer = start_computer(i, n_fc, state)
        flight_computers.append(computer)

    for i in range(n_correct_fc, n_fc):
        computer = start_computer(i, n_fc, state, random_computer=True)
        flight_computers.append(computer)

    for computer in flight_computers:
        peers = flight_computers[:]
        peers.remove(computer)
        for peer in peers:
            computer.add_peer(peer)
        computer.daemon = True
        computer.start()

    time.sleep(1) # Wait for HTTP API to start

    for computer in flight_computers:
        resp = requests.get(f'http://127.0.0.1:{5000 + computer.id}/start_raft', timeout=1)
        if resp.status_code != 200:
            raise Exception('Node raft not started')

    return flight_computers


def select_leader():
    leader = 0
    while True:
        resp = requests.get(f'http://127.0.0.1:{5000 + leader}/is_leader', timeout=1)
        if resp.status_code != 200:
            continue

        resp_leader = json.loads(resp.content)['leader']
        if resp_leader:
            print('found leader', leader)
            return flight_computers[leader]
        # print(leader)
        leader = (leader + 1) % len(flight_computers)
        time.sleep(0.5)


# Connect with Kerbal Space Program
flight_computers = allocate_flight_computers(arguments)

try:
    complete = False
    leader = select_leader()
    t0 = time.time()
    timestep = 0
    state = readout_state()

    while not complete:
        if timestep % 100 == 0:
            diff = time.time() - t0
            if diff == 0:
                diff = 1
            speed = timestep / diff
            if speed == 0:
                speed = 1
            left = len(states) - timestep
            remaining_time = int(left / speed)
            print(f'{timestep}/{len(states)}', f'{speed:3.0f} item/s', remaining_time, 'sec')
        
        state_decided = leader.decide_on_state(state)
        if not state_decided: # Always False
            print('State not decided!')
            continue

        action = leader.sample_next_action()
        if action is None:
            complete = True
            continue
        elif action == {}:
            leader = select_leader()
            continue

        if leader.decide_on_action(action):
            execute_action(action)
            timestep += 1
            state = readout_state()
        else:
            print('Action not decided')
            continue
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
