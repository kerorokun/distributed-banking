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
import threading
import time

class Commit:
    def __init__(self):
        self.num = 0

commit = Commit()

def test(node):
    has_commit = False
    while True:
        has_commit = raft_node.commit(str(commit.num))
        if has_commit:
            print(f"Committed: {commit.num}")
        time.sleep(1)

def commit_num(msg):
    print(f"Applied: {msg}")
    commit.num = int(msg) + 1

if __name__ == "__main__":
    # Usage: main.py <id> <ip> <port> <others...>
    neighbors = []
    for i in range(4, len(sys.argv), 2):
        neighbors.append((sys.argv[i], sys.argv[i+1]))
    raft_node = raft_node.RAFTNode(sys.argv[1], sys.argv[2], int(sys.argv[3]), on_commit=commit_num)
    threading.Thread(target=test, args=(raft_node,), daemon=True).start()
    raft_node.start(neighbors)
