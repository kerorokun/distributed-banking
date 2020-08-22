import collections
import typing
import threading
import socket
import sys
import enum
import logging as log

class Connection(typing.NamedTuple):
    ip: str
    port: str


class Node:
    """
    Represents a Node in the P2P network. This class will open a socket on a specific address and port and listen
    to incoming connections. This class also exposes the ability to connect to other nodes.

    NOTE: This node expects all messages to end with a specific sequence of characters. By default it uses newlines.
    NOTE: If you wish to register a callback function to handle a connection, you should write a function that handles
          <void/T> ...(Connection conn). You can pass this address to send and receive to send and write information
          to and from that address.
    NOTE: This node also does not allow for simultaneous connections from the same address.
    """

    RECV_SIZE = 1024

    def __init__(self,
                 ip: str, port: int,
                 on_connect: typing.Callable[[Connection], None] = None,
                 on_disconnect: typing.Callable[[Connection], None] = None,
                 on_message: typing.Callable[[Connection, str], None] = None,
                 msg_ending='\n') -> None:
        self.ip = ip
        self.port = port
        self.sock = None
        self.msg_ending = msg_ending
        self.on_conn_callback = on_connect
        self.on_disconn_callback = on_disconnect
        self.on_message_callback = on_message

        self.socks_lock = threading.Lock()
        self.conn_to_sock = collections.defaultdict(lambda: None)
        self.conn_to_should_conn = collections.defaultdict(lambda: True)
        self.connected_socks = set()
 
    def open(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.sock.listen()

        log.debug(f"[NODE] Starting node at {self.ip} {self.port}")

    def serve(self, blocking: bool = True) -> None:
        if blocking:
            self.__listen_for_connections()
        else:
            threading.Thread(
                target=self.__listen_for_connections, daemon=True).start()


    def start(self, blocking: bool = True) -> None:
        self.open()
        self.serve(blocking=blocking)

    def connect_to(self, ip: str, port: str) -> None:
        threading.Thread(target=self.__connect_to,
                         args=(ip, port), daemon=True).start()

    def __send(self, sock: socket.socket, msg: str) -> None:
        msg += self.msg_ending
        try:
            sock.sendall(msg.encode('utf-8'))
        except Exception as e:
            log.warn(f"[NODE] Error while sending {e}")

    def send(self, conn: Connection, msg: str) -> None:
        sock = self.conn_to_sock[conn]
        self.__send(sock, msg)

    def send_to_conns(self, conns: typing.List[Connection], msg: str) -> None:
        for conn in conns:
            self.send(conn, msg)

    def send_to_all(self, msg: str) -> None:
        self.socks_lock.acquire()
        for sock in self.connected_socks:
            self.__send(sock, msg)
        self.socks_lock.release()

    def __read(self, sock: socket.socket, leftover: str) -> (typing.List[str], str):
        """
        Read message from a socket. It returns a list of each individual message as well as any leftover in the read.
        """
        msg = sock.recv(Node.RECV_SIZE)
        msg = leftover + msg.decode('utf-8')
        msg_lines = msg.rstrip(self.msg_ending).split(self.msg_ending)

        if not msg.endswith(self.msg_ending):
            leftover = msg_lines[-1]
            msg_lines = msg_lines[:-1]

        return (msg_lines, leftover)

    def __listen_for_connections(self) -> None:
        try:
            while True:
                sock, address = self.sock.accept()
                peer_thread = threading.Thread(target=self.__handle_client, args=(
                    sock, address[0], address[1]))
                peer_thread.start()
        except KeyboardInterrupt:
            return

    def __handle_client(self, sock: socket.socket, ip: str, port: str) -> None:
        # Handle the introduction between nodes
        self.__send(sock, f"NODE-INTRODUCTION {self.ip} {self.port}")
        leftover = ""
        initial_msgs = []
        conn = None
        found_introduction = False
        try:
            while not found_introduction:
                msgs, leftover = self.__read(sock, leftover)
                for msg in msgs:
                    if msg.startswith("NODE-INTRODUCTION"):
                        # Check to see if this is a new connection
                        _, actual_ip, actual_port = msg.split()
                        conn = Connection(actual_ip, actual_port)
                        if not self.__is_new_connection(conn):
                            sock.shutdown()
                            sock.close()
                            return
                        else:
                            found_introduction = True
                    else:
                        initial_msgs.append(msg)
        except:
            return

        # Fully connected now. Can start communicating
        self.__store_sock(sock, conn)
        self.conn_to_should_conn[conn] = True
        log.debug(f"[NODE] {conn} connected.")
        if self.on_conn_callback:
            self.on_conn_callback(conn)

        # Connection loop
        msgs = initial_msgs
        try:
            while self.conn_to_should_conn[conn]:
                for msg in msgs:
                    if self.on_message_callback and self.conn_to_should_conn[conn]:
                        self.on_message_callback(conn, msg)
                msgs, leftover = self.__read(sock, leftover)
        except socket.error as e:
            log.debug(f"[NODE] {conn} disconnected.")
        except Exception as e:
            log.warn(f"[NODE] Error during execution: {e}")

        # Disconnect, cleanup
        self.conn_to_should_conn.pop(conn)
        self.__unstore_sock(sock, conn)
        if self.on_disconn_callback:
            self.on_disconn_callback(conn)

    def disconnect(self, conn: Connection) -> None:
        log.debug(f"[NODE] Dropping {conn}")
        self.conn_to_should_conn[conn] = False

    def __connect_to(self, ip: str, port: str) -> None:
        try:
            sock = socket.create_connection((ip, port))
            self.__handle_client(sock, ip, port)
        except Exception:
            return

    def __is_new_connection(self, conn: Connection) -> bool:
        self.socks_lock.acquire()
        is_new = self.conn_to_sock[conn] is None
        self.socks_lock.release()
        return is_new

    def __store_sock(self, sock: socket.socket, conn: Connection) -> None:
        self.socks_lock.acquire()
        self.conn_to_sock[conn] = sock
        self.connected_socks.add(sock)
        self.socks_lock.release()

    def __unstore_sock(self, sock: socket.socket, conn: Connection) -> None:
        self.socks_lock.acquire()
        self.conn_to_sock.pop(conn)
        self.connected_socks.remove(sock)
        self.socks_lock.release()
