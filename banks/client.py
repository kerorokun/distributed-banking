import sys
import queue
import threading
import server.node as node

class Client:

    def __init__(self, ip, port):
        self.node = node.Node(ip, port, on_connect=self.on_connect, on_message=self.on_coord_message)
        self.input_queue = queue.Queue()
        self.coord_queue = queue.Queue()
        self.is_transacting = False
        self.is_waiting_for_input = False
        self.curr_coord_conn = None

    def start(self, start_coordinator):
        self.node.start(blocking=False)
        self.node.connect_to(start_coordinator[0], start_coordinator[1])
        threading.Thread(target=self.__input_loop, daemon=True).start()

        while True:
            self.is_waiting_for_input = True
            msg = self.input_queue.get()
            self.is_waiting_for_input = False
            if 'QUIT' in msg:
                sys.exit(0)
            if not self.is_transacting and 'BEGIN' in msg:
                self.on_begin()
            elif self.is_transacting and 'DEPOSIT' in msg:
                self.on_deposit(msg)
            else:
                print('Invalid message. Make sure you begin a transaction.')

    def on_coord_message(self, conn, msg):
        self.coord_queue.put(msg)

    def on_redirect(self, msg):
        _, leader_conn = msg.split()
        leader_conn = node.Connection.from_str(leader_conn)
        self.node.disconnect(self.curr_coord_conn)
        self.node.connect_to(leader_conn.ip, leader_conn.port)

    def on_connect(self, conn):
        self.curr_coord_conn = conn

    def on_deposit(self, msg):
        _, branch, acc, amt = msg.split()
        self.node.send(self.curr_coord_conn, f"BANK {branch} {acc} {amt}")
        response = self.coord_queue.get()
        if response.startswith("REDIRECT"):
            self.on_redirect(response)
        else:
            print("DEPOSIT RESPONSE")
            print(response)

    def empty_coord_queue(self):
        while not self.coord_queue.empty():
            try:
                self.coord_queue.get(False)
            except:
                continue
    
    def on_begin(self):
        print("SENDING BEGIN")
        self.node.send(self.curr_coord_conn, "BEGIN")
        self.empty_coord_queue()
        response = self.coord_queue.get()

        if response.startswith("REDIRECT"):
            self.on_redirect(response)
        else:
            print("OK TO BEGIN")
            self.is_transacting = True

    def __input_loop(self):
        while True:
            for msg in sys.stdin:
                self.input_queue.put(msg)