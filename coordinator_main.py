import argparse
import sys
import os
import banks.coordinator as coordinator

import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)
handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)

parser = argparse.ArgumentParser(description="Start a coordinator node")
parser.add_argument("--ip", type=str)
parser.add_argument("--port", type=int)
parser.add_argument("--name", type=str)
parser.add_argument("--coordinators", nargs="+", help="the other coordinator nodes in the RAFT group")
parser.add_argument("--branches", nargs="+", help="the branches that this coordinator will work with")

if __name__ == "__main__":
    args = parser.parse_args()
    coordinators = []
    if args.coordinators:
        for i in range(0, len(args.coordinators), 2):
            coordinators.append((args.coordinators[i], args.coordinators[i+1]))

    branches = []
    if args.branches:
        for i in range(0, len(args.branches), 2):
            branches.append((args.branches[i], args.branches[i+1]))

    coord = coordinator.Coordinator(args.name, args.ip, int(args.port), coordinators, branches)