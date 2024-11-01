import sqlite3,time
from DBconnect import SocketTransiever
from dotenv import load_dotenv, dotenv_values
from datetime import datetime
from threading import Thread,Event
from asyncio import Queue, QueueEmpty
MISCELLANIOUS_LOGS_TABLE = "LogsMisc"
TELEGRAM_LOGS_TABLE = "LogsTelegram"
DISCORD_LOGS_TABLE =  "LogsDiscord"
SITE_LOGS_TABLE = "LogsSite"
ROUTING_TABLE = "Routing"
CHATSITE_TABLE = "ChatSite"
FILE_TABLE = "FilenamesSite"
TABLES = (MISCELLANIOUS_LOGS_TABLE,
          TELEGRAM_LOGS_TABLE,
          DISCORD_LOGS_TABLE,
          SITE_LOGS_TABLE,
          ROUTING_TABLE,
          CHATSITE_TABLE,
          FILE_TABLE)
load_dotenv()
HOST = "127.0.0.1"
config = dotenv_values(".env")
CONTROL_THREAD_TIMEOUT = 0.1
def now():
    return datetime.now().strftime("[%d.%m.%Y@%H:%M:%S]")
class Channel():
    def __init__(self,From,To) -> None:
        self.ID_from = From
        self.ID_to = To
    def __eq__(self, __value: object) -> bool:
        return self.ID_from == __value
class Model():
    def __init__(self,filename,exitFlag:Event=None) -> None:
        self.db = sqlite3.connect(filename)
        self.cur = self.db.cursor()
        tables = self.get_all_tables()
        self.table_names = tables if tables else TABLES
        table_params = tuple(self.get_columns_info(table) for table in self.table_names)
        TABLE_PARAMS = table_params if table_params[0] else TABLE_PARAMS
        table_indexes = self.get_indexes()
        TABLE_INDEXES = table_indexes if table_indexes else TABLE_INDEXES
        self.table_params = {k:(p,i) for (k,p,i) in zip(self.table_names,TABLE_PARAMS,(*TABLE_INDEXES,*(("",""),)*(len(self.table_names)-len(TABLE_INDEXES))))}
        #print(self.table_names,self.table_params)
        self.exitFlag = exitFlag if exitFlag else Event()
        self.transievers = [SocketTransiever((HOST,int(host))) for host in config.values()]
        #print(f"amount of transievers: {len(self.transievers)}")
        [transiever.bind() for transiever in self.transievers]
        ''' decommisioned
        for Key,(Param,Indexes) in self.table_params.items():
            self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {Key}(
            id INTEGER PRIMARY KEY,
            {Param}
            )    
            """)
            if(Indexes[0]):self.cur.execute(f"CREATE INDEX IF NOT EXISTS {Indexes[0]} ON {Key} ({Indexes[1]})")
        if not self.cur.execute("SELECT ID_FROM FROM Routing").fetchall():
            for route in ROUTING:
                self.cur.execute(f"INSERT INTO Routing (ID_from,ID_to) VALUES (?,?)",(route.ID_from,route.ID_to))
            self.db.commit()
        '''
    def get_columns_info(self,table_name):
        # Execute PRAGMA to get column info
        self.cur.execute(f"PRAGMA table_info('{table_name}')")
        columns = self.cur.fetchall()
        # Format the output as "name type, name type, ..."
        formatted_columns = ', '.join(f"{col[1]} {col[2]}" for col in columns if col[1] != "id")
        return formatted_columns
    def get_all_tables(self):
        # Query to get all table names
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cur.fetchall()
        # Extract table names from the result
        table_names = [table[0] for table in tables]
        return table_names
    def get_indexes(self):
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='index';")
        indexes = self.cur.fetchall()
        table_indexes = []
        for index in indexes:
            index_name = index[0]
        
        # Получаем информацию о колонках индекса
        self.cur.execute(f"PRAGMA index_info('{index_name}');")
        columns_info = self.cur.fetchall()
        # Добавляем информацию о каждом индексе в нужном формате
        for col_info in columns_info:
            column_index = col_info[0]
            column_name = col_info[1]
            table_indexes.append((index_name, column_name))
        return table_indexes
    def process_connection(self,transiever:SocketTransiever,queue):
        try:
            while not self.exitFlag.is_set(): 
                print(f"waiting for connection @ {transiever.host}")
                conn, addr = transiever.accept()
                print(f"accepted @ {addr}")
                while True:
                    time.sleep(CONTROL_THREAD_TIMEOUT)
                    print(f"waiting to recieve @ {addr}")
                    message = transiever.receive_message(transiever.conn)
                    if not message:
                        conn.close()
                        print(f"closing @ {addr}")
                        break
                    print("From {0}, type {1}, data: {2}".format(*message.values()))
                    queue.put_nowait((transiever,message))
        finally:
            print("Thread has died")
    def process_message(self,message):
        if message["type"] == "LOG":
            result = self.DB_log(*message["message"])
            if result != True: self.DB_log(MISCELLANIOUS_LOGS_TABLE,result)
            return
        if message["type"] == "LST":
            request = message["message"]
            if not request: return
            result = self.DB_list(*request)
            return ("DB","ANS",result)
        if message["type"] == "GET":
            request = message["message"]
            if not request: return
            result = self.DB_fetch(*request)
            return ("DB","ANS",result)
        if message["type"] == "CNT":
            request = message["message"]
            if not request: return
            result = self.DB_count(*request)
            return ("DB","ANS",result)
        if message["type"] == "STP":
            print("STOPPING")
            self.exitFlag.set()
    def control_thread(self):
        try:
            MessageQueue = Queue()
            threads = []
            for transiever in self.transievers:
                thread = Thread(target=self.process_connection,args=(transiever,MessageQueue),daemon=True)
                threads.append(thread)
                thread.start()
            while not self.exitFlag.is_set():
                try:
                    transiever,message = MessageQueue.get_nowait()
                    try:result = self.process_message(message)
                    except Exception as E:
                        transiever.send_message(transiever.conn,(f"ERROR: {E}"))
                        self.DB_log(MISCELLANIOUS_LOGS_TABLE,(f"'{message}' failed: '{E}' @ {transiever}",now()))
                    if result:
                        #transiever.send_message(transiever.conn,result)
                        transiever.send_message(transiever.conn,*result)
                except QueueEmpty:time.sleep(CONTROL_THREAD_TIMEOUT)

            #[thread.join() for thread in threads]
                    
        except KeyboardInterrupt:
            self.DB_log(MISCELLANIOUS_LOGS_TABLE,("Shutdown",now()))
            self.DB_quit()
            raise KeyboardInterrupt
    def DB_log(self,table_name:str,message):
        "Logs whatever to one of logging tables"
        params = self.table_params[table_name][0].replace(" TEXT","").replace(" INTEGER","")
        params_len = len(params.split(","))
        try:
            command = f"INSERT INTO {table_name} ({params}) VALUES ({', '.join(['?']*params_len)})",[thing if str(thing) else "<nothing>" for thing in message[:params_len]]
            self.cur.execute(*command)
        except sqlite3.OperationalError as e:
            return "; ".join([str(e),table_name,*map(str,message),command[0]])
        self.DB_commit()
        return True
    def DB_count(self,table_name:str):
        return int(self.cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    def DB_remove(self,table_name:str,id):
        "Removes certain entries from database."
        self.cur.execute(f"DELETE FROM {table_name} WHERE id={id}") 
    def DB_list(self,table_name:str,limit:int=-1,offset:int=0,desc_order:bool=False):
        "Returns list of all entries in database"
        command = f"SELECT {self.table_params[table_name][0]} FROM {table_name} ORDER BY id {'DESC' if desc_order else 'ASC'} LIMIT {limit} OFFSET {offset}"
        return self.cur.execute(command).fetchall()
    def DB_fetch(self,table_name:str,column_name,filter:str,limit:int=1):
        command = f"SELECT * FROM {table_name} WHERE {column_name} = ? LIMIT ?"
        return self.cur.execute(command, (filter, limit)).fetchall()
    def DB_get_byName(self,table_name:str,name):
        "Returns entries in database by name"
        return self.cur.execute(f"SELECT FROM {table_name} WHERE nickname={name}").fetchall()
    def DB_get_byID(self,table_name:str,id):
        "Returns entries in database by ID"
        return self.cur.execute(f"SELECT FROM {table_name} WHERE id={id}").fetchall()
    def DB_edit(self,table_name:str,id,field,value):
        "Edits entries in database"
        self.cur.execute(f"UPDATE {table_name} SET {field}={value} WHERE id={id}")
    def DB_commit(self):
        self.db.commit()
    def DB_quit(self):
        self.db.commit()
        self.db.close()
if __name__ == "__main__":
    try:Model("MMM.db").control_thread()
    except KeyboardInterrupt:quit()
    