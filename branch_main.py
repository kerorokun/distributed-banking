import argparse
import sys
import os
import banks.branch as branch

import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)
handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)

parser = argparse.ArgumentParser(description="Start a branch node")
parser.add_argument("--ip", type=str)
parser.add_argument("--port", type=int)
parser.add_argument("--id", type=str)
parser.add_argument("--branch", type=str)
parser.add_argument("--branches", nargs="+", help="the other branch nodes in the RAFT group")

if __name__ == "__main__":
    args = parser.parse_args()
    branches = []
    if args.branches:
        for i in range(0, len(args.branches), 2):
            branches.append((args.branches[i], args.branches[i+1]))
    branch = branch.Branch(args.branch, args.id, args.ip, int(args.port), branches)