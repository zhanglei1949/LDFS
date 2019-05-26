# Running on each metadata server
# only connected with master metadata server
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import sqlite3 as sql

from sqlite3 import Error
import time
from LDFSInode import LDFSInode

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_path = ('/RPC2',)
class LDFSMetadataServer:
    # Storing metadata in python-sqlite3
    def __init__(self, db_path):
        self.db_file = db_path
        self.db = sql.connect(self.db_file)
        #self.cursor = self.db.cursor()
        self.inode_table = {}
    def init(self):
        #Initialize the database
        try:
            print("Deleting old tables")
            cursor = self.db.cursor()
            cursor.execute('DROP TABLE IF EXISTS INODE')
            cursor.close()
            self.db.commit()
        except:
            self.db.rollback()
            print("Reinitialization failed: droping error")
        try:
            print("creating new tables")
            cursor = self.db.cursor()
            cursor.execute('CREATE TABLE inode (id INTEGER PRIMARY KEY,' 
                    'parent_id INTEGER,'
                    'name text, '
                    'inode_type char(1),' 
                    'perms text, '
                    'uid int, '
                    'gid int, '
                    'attrs text,' 
                    'c_time text, '
                    'm_time text, '
                    'a_time text, '
                    'size int, '
                    'UNIQUE(parent_id, name))')
            cursor.close()
            self.db.commit()
            print("Initialization succeed")
        except:
            self.db.rollback()
            print("Reinitialization failed: creating table error")
    def get_inodes(self):
        try:
            cursor = self.db.cursor()
            cursor.execute('select * from  inode')
            rows = cursor.fetchall()
            cursor.close()
        except Error, e:
            print("Get inodes error")
            print(e)
            return 0
        res = []
        for row in rows:
            res.append(row)
        return res

    def add_inode(self, inode_dict):
        try:
            print("Trying adding inode %s" % (inode_dict['name']))
            cursor = self.db.cursor()
            inode = LDFSInode(inode_dict)
            cursor.execute('insert into inode (id, parent_id, name, inode_type, perms, uid, gid, attrs,c_time, m_time, a_time, size) values (?,?,?,?,?,?,?,?,?,?,?,?)', (inode.id, inode.parent_id, inode.name, inode.inode_type, inode.perms, inode.uid, inode.gid, inode.attrs, inode.c_time, inode.m_time, inode.a_time, inode.size))
            print("added")
            cursor.close()
            self.db.commit()
            return 1
        except Error, e:
            self.db.rollback()
            print("Adding node error %s" % (inode_dict['name']))
            print(e)
            return 0
    def delete_inode(self, inode_id):
        try:
            print("Deleting inode %s" %(inode_id))
            cursor = self.db.cursor()
            cursor.execute('delete from inode where id = ?', (inode_id, ))
            cursor.close()
            self.db.commit()
            return 1
        except Error, e:
            self.db.rollback()
            print("Failed to delete inode %d" % (inode_id))
            print(e)
            return 0

    def search_inode_with_parent(self, parent_inode_id, filename):
        #Return the search result of search filename under parent_inode
        res = []
        try:
            cursor = self.db.cursor()
            cursor.execute('select * from inode where parent_id = ? and name = ?', (parent_inode_id, filename))
            rows = cursor.fetchall()    
            cursor.close()
        except Error, e:
            print("Failed to search %s under inode %d" % (filename, parent_inode_id))
            print(e)
            return 0
        for row in rows:
            res.append(row)
        assert len(res) == 1
        return res[0]

    def get_child_inodes(self, inode_id):
        #Return all inodes under this inode
        res = []
        try:
            cursor = self.db.cursor()
            cursor.execute('select * from inode where parent_id = ?', (inode_id, ))
            rows = cursor.fetchall()
            cursor.close()
        except Error, e:
            print("Failed to get child inode for inode %d" % (inode_id))
            print(e)
            return 0
        for row in rows:
            res.append(row)
        return res
    def update_name_parent_id(self, inode_id, new_filename, new_parent_id):
        try:
            cursor = self.db.cursor()
            cursor.execute('update inode set name  = ?,parent_id = ? where id = ?', (new_filename, new_parent_id, inode_id))
            cursor.close()
            self.db.commit()
            return 1
        except Error, e:
            self.db.rollback()
            print("Failed to update name for inode %d" % (inode_id))
            print(e)
            return 0
    '''
    def load_inodes(self):
        inodes = self.get_inodes()
        print("Get %d inodes from db" % len(inodes))
        #Load the inodes, and store them in memory
        for row in inodes:
            inode = LDFSInode()
            inode.update(row)
            if (str(inode.id) in self.inode_table):
                raise Exception("%s already in memory table" %(str(inode.id)))
            self.inode_table[str(inode.id)] = inode
        if len(self.inode_table) == 0:
            root_inode = self.create_inode('/', '0')
            #Dump root inode into metadata in the CREATE_NODE function
            assert root_inode != 0
            #Assertion error if creation failed
        else:
            root_inode = self.get_inode_by_name('/')
        
        self.root_inode_id = root_inode.id
        print("Load %s inodes from db" % (len(self.inode_table)))
    '''
    def get_status(self):
        return 0
if __name__ == '__main__':
    host = 'localhost'
    port = 9524
    db_path = '/tmp/data.db'
    server = SimpleXMLRPCServer((host, port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
    server.register_introspection_functions()
    server.register_instance(LDFSMetadataServer(db_path))
    server.serve_forever()
'''
    sql = LDFSMetadataSqlite('/tmp/ldfs.db')
    sql.init()
    inode1 = LDFSInode({"inode_type":"d", "name": "/", "parent_id" : 0, "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
    inode2 = LDFSInode({"inode_type":"d", "name": "a", "parent_id" : 1, "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
    inode3 = LDFSInode({"inode_type":"d", "name": "b", "parent_id" : 1, "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
    sql.add_inode(inode1)
    sql.add_inode(inode2)
    sql.add_inode(inode3)
    inodes = sql.get_inodes()
    print(inodes)
    #res = sql.search_inode_with_parent(0, '/sd')
    #res = sql.get_child_inodes(1)
    res = sql.delete_inode(1)
    print(res) 
    inodes = sql.get_inodes()
    print(inodes)
    print(res)
'''

