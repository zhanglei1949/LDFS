####
# Running on master node
####
import argparse

import os, time
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib
#from LDFSMetadataServer import LDFSMetadata
from LDFSInode import LDFSInode
# Store all ids in STRING 
from utils import load_metadata_conf, InodeServer, myhash
import sqlite3 as sql
from sqlite3 import Error
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_path = ('/RPC2',)
class LDFSMaster:
    def __init__(self, metadata_backend, rootfs, db_filename, init, metadata_server_conf):
        #Currently don't store real data, just fake metadata
        self.metadata_backend = metadata_backend
        self.multiplication = 2
        self.rootfs = rootfs
        self.db_filename = db_filename# This file stor the metadata for metadata
        
        self.root_inode_id = '0'
        self.next_id = 2 # root is 1
        self.inode_table = {}# {inode.id : LDFSInode}
        self.inode_map = {} # {inode.id : inode.id}
        self.inodeserver = {} #{inode.id : server.name}

        #connect to db
        assert self.metadata_backend == 'sqlite'
        self.db_path = os.path.join(self.rootfs, self.db_filename)
        self.db = sql.connect(self.db_path) 

        # load configuration
        self.metadata_servers_conf = load_metadata_conf(metadata_server_conf)
        # ['server1'] = ip:port

        # build up connections with other servers 
        self.metadata_servers = []
        for server in self.metadata_servers_conf:
            self.metadata_servers.append(xmlrpclib.ServerProxy('http://' + self.metadata_servers_conf[server]))
        # self.metadata[0] = rpc to 192.168.1.34
        if init == 1:
            self.init()
        # Load inodes into inode_table
        self.load_inodes()
        if (len(self.inode_table ) == 0):
            self.create_root()
        #self.printStat()
    def init(self):
        #try:
        for server in self.metadata_servers:
            server.init()
        #except:
        #    raise Exception("Error occurred when initializing metadata servers")
        # Initial master
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
            cursor.execute('CREATE TABLE inodeserver (no INTEGER AUTO_INCREMENT,'
            'name text,'
            'id INTEGER PRIMARY KEY,'
            'server1 text,'
            'server2 text)')
            cursor.close()
            self.db.commit()
        except Exception, e:
            self.db.rollback()
            print(e)
            #raise Exception("Create inodeserver failed")
    def printStat(self):
        print('Inode table')
        for k in self.inode_table:
            print(self.inode_table[k].name, self.inode_table[k].id),
        print('')
        print('Inode map')
        for k in self.inode_map:
            print(k, self.inode_map[k])
        
        # print total status
    def exist(self, filename):
        
        res = self.get_inode_by_name(filename)
        if (res == 0):
            return 0
        else:
            return 1
    def load_inodes(self):
        print("Loading inodes on master")
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
            if (str(inodeserver.id) in self.inodeservers):
                raise Exception("Two same ids")
            else:
                self.inodeservers[str(inodeserver.id)] = inodeserver
        print("Loaded %d mapping instances" %(len(self.inodeserver)))
        for server in self.metadata_servers:
            _inodes = server.get_inodes()
            _inode_table = {}
            for row in _inodes:
                inode = LDFSInode()
                inode.update(row)
                if (str(inode.id) in _inode_table):
                    raise Exception("%s already in memory table" %(str(inode.id)))
                _inode_table[str(inode.id)] = inode
            self.inode_table = dict(self.inode_table, **_inode_table) 
        # build up inode_map
        for inode_id in self.inode_table:
            inode = self.inode_table['inode_id']
            if (str(inode.parent_id) in self.inode_map):
                self.inode_map[str(inode.parent_id)].append(inode.id)
            else:
                self.inode_map[str(inode.parent_id)] = [inode.id]
    def create_root(self): # default parent node is root 0
        filename = '/'
        parent_inode_id = '0'
        print("creating root inode")
        
        inode = LDFSInode({"id" : "1", "inode_type":"d", "name": '/', "parent_id" : '0', "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
        # Decide which server to save the root node
        print(inode.id)
        servers = myhash(str(inode.id), self.multiplication)
        for server in servers:
            self.metadata_servers[server].add_inode(inode.toDict())
        # Update inode table and inode map
        self.inode_table[str(inode.id)] = inode
        if parent_inode_id in self.inode_map:
            self.inode_map[str(parent_inode_id)].append(str(inode.id))
        else:
            self.inode_map[str(parent_inode_id)] = [str(inode.id)]
        return inode
            
    def get_parent_inode_from_filename(self,filename):
        dirname = os.path.dirname(filename)
        return self.get_inode_by_name(dirname)
    def get_inode_by_name(self, filename):
        #Note that filename is the full path
        # Return LDFSInode instance
        filename = os.path.normpath(filename)
        if (filename == '/'):
            if (self.inode_table == {}):
                return 0
            return self.inode_table['1']
        else:
            filename_splited = filename.split('/')[1:] # abanding the leading \/
            parent_inode_id = '1'
            flag = False
            for name in filename_splited:
                if (parent_inode_id not in self.inode_map):
                    return 0
                child_inode_ids = self.inode_map[parent_inode_id]
                flag = False
                for child_inode_id in child_inode_ids:
                    if name == self.inode_table[child_inode_id].name:
                        flag = True
                        parent_inode_id = str(self.inode_table[child_inode_id].id)
                        res = child_inode_id
                        break
                    else:
                        flag = False
                        continue
                if (flag == False):
                    break
            if (flag == False):
                return 0
            else:
                return self.inode_table[res]
    def delete_file(self, filename, res):
        print("Inode.id for file %s is %s" %(filename, str(res.id)))
        servers = myhash(res.id, self.multiplication)
        deletion = 0
        for server in servers:
            deletion += self.metadata_servers[server].delete_inode(res.id)
        if (deletion == len(servers)):
            print("Deletion succeed")
            assert str(res.id) in self.inode_table
            del self.inode_table[str(res.id)]
            # We also need to delete res.id in inode_map for its parent
            self.inode_map[str(res.parent_id)].remove(str(res.id))
           
            return 1
        else:
            print("Failed to delete %s with inode id %s" %(filename, str(res.id)))
            return 0

    def delete(self, filename):
        print("Deleting file %s" %(filename))
        inode = self.get_inode_by_name(filename)
        if (inode == 0): 
            print("No such file %s" % (filename))
            return 0
        else:
            print("Inode.id for %s is %s" % (filename, str(inode.id)))
            if (inode.inode_type == 'd'):
                print("directory")
                if (str(inode.id) in self.inode_map):
                    if (self.inode_map[str(inode.id)] != []):
                        print("Direction %s is not empty!" % (dirname))
                        return 0
                return self.delete_file(filename, inode)
            elif (inode.inode_type == 'f'):
                print("file")
                return self.delete_file(filename, inode)
            else:
                return 0
    def rename(self, filename, new_filename):
        #Same work schema for file and directory
        # Check whether the parent directory is the same
        filename = os.path.normpath(filename)
        new_filename = os.path.normpath(new_filename)
        print("Master: rename %s to %s" % (filename, new_filename))
        # check whether new filename exist
        if self.exist(new_filename):
            raise Exception("File %s already exists" % (new_filename))
        #assert os.path.dirname(filename) == os.path.dirname(new_filename)
        new_filename_base = os.path.basename(new_filename)
        if (not self.exist(os.path.dirname(new_filename))):
            raise Exception("Direction doesn't exist")
        new_parent = self.get_parent_inode_from_filename(new_filename)
        print("Master : target directory %s , id %s "% (new_parent.name, new_parent.id))
        res = self.get_inode_by_name(filename)
        if (res == 0):
            print("No such file %s" % (filename))
            return 0
        else:
            print("Inode.id for file %s is %s" %(filename, res.id))
            servers = myhash(res.id, self.multiplication)
            rename_res = 0
            for server in servers:
                rename_res += self.metadata_servers[server].update_name_parent_id(res.id, new_filename_base, str(new_parent.id))
            if (rename_res == len(servers)):
                res.name = new_filename_base
                self.printStat()
                print(res.parent_id)
                # Update inode map
                self.inode_map[str(res.parent_id)].remove(str(res.id))
                if (str(new_parent.id) not in self.inode_map):
                    self.inode_map[str(new_parent.id)] = [str(res.id)]
                else:
                    self.inode_map[str(new_parent.id)].append(str(res.id))
                res.parent_id = str(new_parent.id)
                self.printStat()

            else:
                print("Failed to update %d with new name %s" %(res.id, new_filename))
                return 0
    def list_files(self, filename):
        print("Master listing under %s" % (filename))
        filename = os.path.normpath(filename)
        file_list = []
        res = self.get_inode_by_name(filename)
        if (res == 0):
            return file_list
        else:
            if (str(res.id) in self.inode_map):
                print("child nodes under %s " % (str(res.id)))
                print(self.inode_map[str(res.id)])
                for child_node_id in self.inode_map[str(res.id)]:
                    file_list.append({'name' : self.inode_table[child_node_id].name,
                                'inode_type' : self.inode_table[child_node_id].inode_type,
                                'size' : self.inode_table[child_node_id].size,
                                'm_time' : self.inode_table[child_node_id].m_time,
                                'a_time' : self.inode_table[child_node_id].a_time,
                                'c_time' : self.inode_table[child_node_id].c_time})
            return file_list
    def alloc(self, filename, attributes = 0):
        print("Alloc %s" % (filename))
        filename = os.path.normpath(filename)
        if (self.exist(filename)):
            raise Exception("%s file already exist")
            return 0
        # We don't alloc real chunks
        if attributes != 0:
            inode =  LDFSInode(attributes)
            inode.id = str(self.next_id)
            self.next_id += 1
            inode.name = os.path.basename(filename)
            inode.parent_id = self.get_parent_inode_from_filename(filename).id
            if (inode.parent_id == 0):
                raise Exception("Direction doesn't exists" + os.path.dirname(filename))
            servers = myhash(inode.id, self.multiplication)
            success = 0
            for server in servers:
                success += self.metadata_servers[server].add_inode(inode.toDict())
            if (success != len(servers)):
                return 0
            assert str(inode.id) not in self.inode_table
            # Update inode table and inode map
            self.inode_table[str(inode.id)] = inode
            if str(inode.parent_id) in self.inode_map:
                self.inode_map[str(inode.parent_id)].append(str(inode.id))
            else:
                self.inode_map[str(inode.parent_id)] = [str(inode.id)]
            return inode
        else:
            raise Exception("Need attributes for allocation of a file")
        return 0
    def stat(self, path):
        inode = self.get_inode_by_name(path)
        if (inode == 0):
            return 0
        return inode.toDict()
    def dump_metadata(self):
        return 1

def main():
    parser = argparse.ArgumentParser(description='LDFS master server')
    parser.add_argument('--config', dest = 'config', default = './metadata_server.ini', help = 'Metadata server configuration file')
    parser.add_argument('--host', dest = 'host', default = 'localhost', help = 'The address of master node')
    parser.add_argument('--port', dest = 'port', default = 9523, help = 'The port listen on')
    parser.add_argument('--backend', dest = 'backend', default = 'sqlite', help = 'The database we utilized to save the metadata file') 
    parser.add_argument('--rootfs', dest = 'rootfs', default = '/tmp', help = 'Data path')
    parser.add_argument('--db_filename', dest = 'db_filename', default = 'ldfs.db', help = 'Data path')
    parser.add_argument('--init', dest = 'init', default = 1, type = int, help = 'Initialize the file system or not')
    #Parse argmuents
    args = parser.parse_args()
    
    #Load configuration
    config = args.config
    #Need try exception desing

    host = args.host
    port = args.port
    backend = args.backend
    rootfs = args.rootfs
    db_filename = args.db_filename
    init = args.init

    #Create server
    server = SimpleXMLRPCServer((host, port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
    server.register_introspection_functions()
    server.register_instance(LDFSMaster(backend, rootfs, db_filename, init, config))
    server.serve_forever()
if __name__ == '__main__':
    main()
