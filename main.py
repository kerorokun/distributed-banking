from threading import Thread
from queue import Queue
from node import Node, ConnectionType
import socket
import sys
import time

class Client:

    def __init__(self):
        self.node = Node(int(sys.argv[1]), self.test, blocking=False, msg_ending="--")
        self.node.start()

        if len(sys.argv) > 2:
            print("Trying to connect to other...")
            self.node.connect_to(sys.argv[2], sys.argv[3])
        while True:
            pass

    def test(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        print(f"Connected to: {ip}:{str}")
        if conn_type == ConnectionType.INCOMING:
            print("Sending response")
            self.node.send(sock, "Welcome")
            self.node.send(sock, f"My address is {ip}:{port}")
        leftover = ''
        while True:
            try:
                msgs, leftover = self.node.read(sock, leftover)
                for msg in msgs:
                    print(msg)
            except IOError:
                break

if __name__ == "__main__":
    client = Client()