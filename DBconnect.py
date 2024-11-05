import socket
import pickle

class SocketSender:
    def __init__(self, host):
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def connect(self):
        self.sock.connect(self.host)
    def send_message(self, sender_name, message_type,message):
        data = {
            "name": sender_name,
            "type": message_type,
            "message": message
        }
        pickled_data = pickle.dumps(data)
        self.sock.sendall(pickled_data)

    def close(self):
        self.sock.close()


class SocketReceiver:
    def __init__(self, host):
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def bind(self):
        self.sock.bind(self.host)
        self.sock.listen(1)
    def receive_message(self):
        conn, _ = self.sock.accept()        
        data = conn.recv(65536)
        if data:
            data = pickle.loads(data)
        conn.close()
        return data
    def sock_close(self):
        self.sock.close()
class SocketTransiever():
    def __init__(self, host=None, target=None):
        if host:
            self.host = (*host,)
            self.host_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if target:
            self.target = (*target,)
            self.target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def bind(self):
        self.host_sock.bind(self.host)
        self.host_sock.listen(1)
    def connect(self,retry:bool = True):
        while True:
            print(f"Trying to connect to {self.target}... ",end="")
            try:
                self.target_sock.connect(self.target)
                print(f"Success")
                return
            except ConnectionRefusedError:
                if not retry: 
                    print(f"Failure")
                    return
            print("\r",end="")
            
    def accept(self):
        self.conn, addr = self.host_sock.accept()
        return self.conn, addr
    def send_message(self, sock:socket.socket=None, sender_name=None, message_type=None, message=None):
        sock = sock if sock else self.target_sock
        data = {
            "name": sender_name,
            "type": message_type,
            "message": message
        }
        pickled_data = pickle.dumps(data)
        if(len(pickled_data)>65535):return
        sock.sendall(pickled_data)
    def receive_message(self,conn:socket.socket=None):  
        conn = conn if conn else self.target_sock
        try:data = conn.recv(65536)
        except ConnectionResetError:
            conn.close()
            return None
        except KeyboardInterrupt:
            conn.close()
            self.close()
            quit()
        if data:
            try:data = pickle.loads(data)
            except pickle.UnpicklingError:
                data = {
                    "name": "CONNECTOR",
                    "type": "ERROR",
                    "message": "too large"
                }
                return data
            return data
        return None
    def close(self):
        if self.host_sock: self.host_sock.close()
        if self.target_sock: self.target_sock.close()