import sys
import os
import argparse
import banks.client as client

import logging as log

root = log.getLogger()
root.setLevel(log.DEBUG)
handler = log.StreamHandler(sys.stdout)
handler.setLevel(log.DEBUG)
root.addHandler(handler)

parser = argparse.ArgumentParser(description="Start a client node")
parser.add_argument("--ip", type=str)
parser.add_argument("--port", type=str)
parser.add_argument("--coordinator", nargs=2)

if __name__ == "__main__":
    args = parser.parse_args()
    client = client.Client(args.ip, int(args.port))
    client.start(start_coordinator=args.coordinator)