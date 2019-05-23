# Multiplication factor is 2
import configparseri, xmlrpclib
import sqlite3 as sql
from sqlite3 import Error
import time

class InodeServer:
    def __init__(self, params):
        self.no = params[0]
        self.name = params[1]
        self.id = params[2]
        self.server = params[3]
    def toDict(self):
        return 0
class LDFSMetadataMaster:
    def __init__(self, db_name, metadata_server_conf_file):
        # Load the configuration of metadata servers
        server_port = 9524
        config = configparser.ConfigParser()
        config.read(metadata_server_conf_file)
        # Metadata list
        metadata_servers = {}
        metadata_servers_list = [x for x in config.sections()]
    
        metadata_servers[config['server1']['name']] = config['server1']['addr']
        metadata_servers[config['server2']['name']] = config['server2']['addr']
        
        # We assume the metadata servers are already running
        self.metadata_servers = [xmlrpclib.ServerProxy(x['addr']) for x in metadata_servers]
        
        # After building up connections with metadata servers, we build up the the metadata database for metadata
        self.db_name = db_name
        self.db = sql.connect(self.db_name)
        self.inodeserver = {}
    def init(self):
        try:
            for server in self.metadata_servers:
            server.init()
        except:
            raise Exception("Error occurred when initializing metadata servers")

        try:
            cursor = self.db.cursor()
            cursor.execute('DROP TABLE IF EXISTS inodeserver')
            cursor.close()
            self.db.commit()
        except:
            self.db.rollback()
            raise Exception("Deleting old table failed")

        try:
            cursor = self.db.cursor()
            cursor.execute('CREATE TABLE inodeserver (no INTEGER AUTOINCREMENT,'
            'name text,'
            'id INTEGER PRIMARY KEY,'
            'server1 text,'
            'server2 text',
            'UNIQUE(ID, server))')
            cursor.close()
            self.db.commit()
        except:
            self.db.rollback()
            raise Exception("Create inodeserver failed")

    def load_inodes(self):
        # Load inodes from multiple servers
        # Load the mapping relation between inodes and servers, don't return
        try:
            cursor = self.db.cursor()
            cursor.execute('select * from inodeserver')
            rows = cursor.fetchall()
            cursor.close()
        except Error, e:
            print(e)
            raise Exception("Load inodeserver table failed")
        # If rows are not null
        m = []
        for row in rows:
            m.append(row)
        for mappping in m:
            inodeserver = InodeServer(mapping)
            if (str(inodeserver.id) in self.inodeserver):
                raise Exception("Two same ids")
            else:
                self.inodeserver[str(inodeserver.id)] = inodeserver
        print("Loaded %d mapping instances" %(len(self.inodeserver)))
        res = []
        for server in self.metadata_servers:
            a = server.get_inodes()
            for x in a:
                res.append(x)
        return res
    
    def add_node(self,inode):# This inode is an instance of LDFSInode
        # Choose two server to save on inode
        server1, server2 = self.crush(inode)
        
        self.metadataservers[server1].addinode(inode)
        self.metadataservers[server2].addinode(inode)
