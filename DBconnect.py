from socket import socket, AF_INET,SOCK_STREAM
from pickle import dumps,loads,UnpicklingError
#from time import sleep
#RETRY_TIMEOUT = 0
class SocketSender:
    def __init__(self, host):
        self.host = host
        self.sock = socket(AF_INET, SOCK_STREAM)
    def connect(self):
        self.sock.connect(self.host)
    def send_message(self, sender_name, message_type,message):
        data = {
            "name": sender_name,
            "type": message_type,
            "message": message
        }
        pickled_data = dumps(data)
        self.sock.sendall(pickled_data)

    def close(self):
        self.sock.close()
class SocketReceiver:
    def __init__(self, host):
        self.host = host
        self.sock = socket(AF_INET, SOCK_STREAM)
    def bind(self):
        self.sock.bind(self.host)
        self.sock.listen(1)
    def receive_message(self):
        conn, _ = self.sock.accept()        
        data = conn.recv(65536)
        if data:
            data = loads(data)
        conn.close()
        return data
    def sock_close(self):
        self.sock.close()
class SocketTransiever():
    def __init__(self, host=None):
        self.host = None
        self.host_sock = None
        self.target = None
        self.target_sock = None
        if host:
            self.host = (*host,)
            self.host_sock = socket(AF_INET, SOCK_STREAM)
    def bind(self):
        self.host_sock.bind(self.host)
        self.host_sock.listen(1)
    def connect(self,addr,retry:int = -1):
        retry = retry
        self.target = addr
        self.target_sock = socket(AF_INET, SOCK_STREAM)
        while True:
            #print(f"Trying to connect to {self.target}... ",end="")
            try:
                self.target_sock.connect(self.target)
                #print(f"Success")
                return self.target_sock
            except ConnectionRefusedError:
                if retry>0: 
                    retry-=1
                    #print(f"Trying {retry} more times... ",end="")
                    #sleep(RETRY_TIMEOUT)
                if retry==0:
                    #print("Failure")
                    return
                    
            #print("\r",end="")
            
    def accept(self,timeout=None):
        self.host_sock.settimeout(timeout)
        try:self.conn, self.addr = self.host_sock.accept()
        except OSError:
            self.bind()
            self.conn, self.addr = self.host_sock.accept()
        return self.conn, self.addr
    def send_message(self, sock:socket=None, sender_name:str=None, target_name:str=None, message_type:str=None, message=None, close_after:bool=True,retry:int=-1):
        if not sock:
            sock = self.target_sock if self.target_sock else self.host
        if type(sock)!=socket:
            if not self.connect(sock,retry):
                return False
            sock = self.target_sock
         
        data = {
            "name": sender_name,
            "target":target_name,
            "type": message_type,
            "message": message
        }
        pickled_data = dumps(data)
        if(len(pickled_data)>65535):return False
        try:sock.sendall(pickled_data)
        except OSError as E:
            print(E)
            return False
        if close_after:
            sock.close()
            self.target = None
            self.target_sock = None
        return True
    def receive_message(self,conn:socket=None,timeout=None):  
        conn = conn if conn else self.target_sock
        if not conn:
            conn,addr = self.accept()
            conn.settimeout(timeout)
        try:data = conn.recv(65536)
        except ConnectionResetError:
            conn.close()
            return None
        except KeyboardInterrupt:
            conn.close()
            self.close()
            quit()
        if data:
            try:data = loads(data)
            except UnpicklingError:
                data = {
                    "name": "CONNECTOR",
                    "target": "whoever sent this",
                    "type": "ERROR",
                    "message": "too large"
                }
                return data
            return data
        return None
    def close(self):
        if self.host_sock: 
            self.host_sock.close()
            self.host_sock = None
        if self.target_sock: 
            self.target_sock.close()
            self.target_sock = None