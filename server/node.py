from threading import Thread, Lock
from typing import Callable, List
from collections import defaultdict
import socket
import sys
import enum
import logging as log


class ConnectionType(enum.Enum):
    INCOMING = 0,
    OUTGOING = 1


class Node:
    """
    Represents a Node in the P2P network. This class will open a socket on a specific address and port and listen
    to incoming connections. This class also exposes the ability to connect to other nodes.

    NOTE: This node expects all messages to end with a specific sequence of characters. By default it uses newlines.
    NOTE: If you wish to register a callback function to handle a connection, you should write a function that handles
          (addr:str). You can pass this address to send and receive to send and write information to and from that 
          address.
    NOTE: This node also does not allow for simultaneous connections from the same address.
    """

    RECV_SIZE = 1024

    def __init__(self,
                 ip: str, port: int,
                 handle_conn_func: Callable[[
                     socket.socket, str, str, ConnectionType], None] = None,
                 blocking: bool = True, msg_ending='\n') -> None:
        self.port = port
        self.ip = ip
        self.sock = None
        self.blocking = blocking
        self.msg_ending = msg_ending
        self.handle_conn_func = handle_conn_func

        self.socks_lock = Lock()
        self.addr_to_sock = defaultdict(lambda: None)
        self.out_socks = set()
        self.in_socks = set()

    def start(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.sock.listen()

        log.debug(f"Starting node at {self.ip} {self.port}")

        if self.blocking:
            self.__listen_for_connections()
        else:
            Thread(target=self.__listen_for_connections, daemon=True).start()

    def connect_to(self, ip: str, port: str) -> None:
        Thread(target=self.__connect_to, args=(ip, port), daemon=True).start()

    def send(self, sock: socket.socket, msg: str) -> None:
        msg += self.msg_ending
        try:
            sock.sendall(msg.encode('utf-8'))
        except:
            pass

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
