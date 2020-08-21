import sys
import os
import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)

handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)


# NOTE: This is required to make this module work as a standalone.
sys.path.append(os.path.abspath(".."))

import raft.raft_node as raft_node

if __name__ == "__main__":
    # Usage: main.py <id> <ip> <port> <others...>
    neighbors = []
    for i in range(4, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = raft_node.RAFTNode(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    raft_node.start(neighbors)
