import json
import server.node as node

class MessageEntry:

    def __init__(self, sender_id, conn, msg):
        self.sender_id = sender_id
        self.conn = conn
        self.msg = msg

    def __repr__(self):
        return json.dumps({"sender_id": self.sender_id, "conn": str(self.conn), "msg": self.msg})

    @classmethod
    def from_str(cls, serialized):
        json_obj = json.loads(serialized)
        return cls(json_obj["sender_id"], node.Connection.from_str(json_obj["conn"]), json_obj["msg"])