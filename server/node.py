from threading import Thread, Lock
from queue import Queue
from typing import Callable, List
import asyncio
import socket
import sys
import enum


class ConnectionType(enum.Enum):
    INCOMING = 0,
    OUTGOING = 1


class Node:
    """
    Represents a Node in the P2P network. This class will open a socket on a specific address and port and listen
    to incoming connections. This class also exposes the ability to connect to other nodes.

    NOTE: This node expects all messages to end with a specific sequence of characters. By default it uses newlines.
    """

    RECV_SIZE = 1024

    def __init__(self, ip: str, port: int, handle_conn_func: Callable[[socket.socket, str, str, ConnectionType], None] = None,
                 blocking: bool = True, msg_ending='\n') -> None:
        self.port = port
        self.ip = ip
        self.handle_conn_func = handle_conn_func
        self.sock = None
        self.blocking = blocking
        self.msg_ending = msg_ending

        self.socks_lock = Lock()
        self.out_socks = set()
        self.in_socks = set()

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
        try:
            sock.sendall(msg.encode('utf-8'))
        except:
            pass

    def send_to_outgoing_conns(self, msg: str) -> None:
        for sock in self.out_socks:
            self.send(sock, msg)

    def send_to_all(self, msg: str) -> None:
        self.socks_lock.acquire()
        for sock in self.out_socks:
            self.send(sock, msg)
        for sock in self.in_socks:
            self.send(sock, msg)
        self.socks_lock.release()

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

    def __handle_client(self, sock: socket.socket, ip: str, port: str,
                        conn_type: ConnectionType) -> None:
        self.__store_sock(sock, conn_type)
        if self.handle_conn_func:
            self.handle_conn_func(sock, ip, port, conn_type)
        self.__unstore_sock(sock, conn_type)

    def __connect_to(self, ip: str, port: str) -> None:
        try:
            sock = socket.create_connection((ip, port))
            self.__handle_client(sock, ip, port, ConnectionType.OUTGOING)
        except Exception:
            return

    def __store_sock(self, sock: socket.socket, conn_type: ConnectionType) -> None:
        with self.socks_lock:
            if conn_type == ConnectionType.INCOMING:
                self.in_socks.add(sock)
            else:
                self.out_socks.add(sock)

    def __unstore_sock(self, sock: socket.socket, conn_type: ConnectionType) -> None:
        with self.socks_lock:
            if conn_type == ConnectionType.INCOMING:
                self.in_socks.remove(sock)
            else:
                self.out_socks.remove(sock)