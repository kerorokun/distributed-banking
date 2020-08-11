from threading import Thread
from queue import Queue
from typing import Callable, List
import asyncio
import socket
import sys
import enum

class ConnectionType(enum.Enum):
    INCOMING=0,
    OUTGOING=1

class Node:
    """
    Represents a Node in the P2P network. This class will open a socket on a specific address and port and listen
    to incoming connections. This class also exposes the ability to connect to other nodes.

    NOTE: This node expects all messages to end with a specific sequence of characters. By default it uses newlines.
    """

    RECV_SIZE = 1024

    def __init__(self, port: int, handle_conn_func: Callable[[socket.socket, str, str, ConnectionType], None] = None,
                 blocking: bool = True, msg_ending='\n') -> None:
        self.port = port
        self.ip = socket.gethostname()
        self.handle_conn_func = handle_conn_func
        self.sock = None
        self.blocking = blocking
        self.msg_ending = msg_ending

    def start(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.sock.listen()

        print(f"Starting node at {self.ip} {self.port}")

        if self.blocking:
            self.__listen_for_connections()
        else:
            conn_thread = Thread(
                target=self.__listen_for_connections, daemon=True)
            conn_thread.start()

    def connect_to(self, ip: str, port: str) -> None:
        Thread(target=self.__connect_to, args=(ip, port), daemon=True).start()

    def send(self, sock: socket.socket, msg: str) -> None:
        msg += self.msg_ending
        sock.sendall(msg.encode('utf-8'))

    def read(self, sock: socket.socket, leftover: str) -> (List[str], str):
        """
        Read message from a socket. It returns a list of each individual message as well as any leftover in the read.
        NOTE: It is recommended that you do not touch the returned leftover. It is intended to be fed back into the 
              call later on.
        NOTE: This method purposely does not handle exceptions. An exception occurring usually means the socket has closed
              so the caller is expected to handle that case appropriately
        """
        msg = sock.recv(Node.RECV_SIZE)
        msg = leftover + msg.decode('utf-8')
        msg_lines = msg.rstrip(self.msg_ending).split(self.msg_ending)

        if not msg.endswith(self.msg_ending):
            leftover = msg_lines[-1]
            msg_lines = msg_lines[:-1]

        return (msg_lines, leftover)

    def __listen_for_connections(self) -> None:
        while True:
            sock, address = self.sock.accept()
            peer_thread = Thread(target=self.__handle_client, args=(
                sock, address[0], address[1], ConnectionType.INCOMING))
            peer_thread.start()

    def __handle_client(self, sock: socket.socket, ip: str, port: str, conn_type: ConnectionType) -> None:
        if self.handle_conn_func:
            self.handle_conn_func(sock, ip, port, conn_type)


    def __connect_to(self, ip: str, port: str) -> None:
        try:
            sock = socket.create_connection((ip, port))
            self.__handle_client(sock, ip, port, ConnectionType.OUTGOING)
        except Exception:
            return