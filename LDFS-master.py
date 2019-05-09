import argparse
import os, time
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

from LDFSMetadata import LDFSMetadataSqlite
from LDFSInode import LDFSInode
class LDFSMaster:
    def __init__(self, metadata_backend, rootfs, db_filename, init):
         #Currently don't store real data, just fake metadata
         self.metadata_backend = backend
         self.rootfs = rootfs
         self.db_filename = db_filename
         self.root_inode_id = 0
         self.inode_table = {}
         self.inode_map = {}

         #connect to db
         assert self.metadata_backend == 'sqlite'
         self.db_path = os.path.join(self.rootfs, self.db_filename)
         self.metadata = LDFSMetadataSqlite(self.db_path)
         # Database connected in initialization
         #
         if init == 1:
             self.metadata.init()
         # Load inodes into inode_table
         self.load_inodes()
    def load_inodes():
        
        try:
            #cursor = self.metadata.db.cursor()
            inodes = self.metadata.get_inodes()
            #Load the inodes, and store them in memory
            for row in inodes:
                inode = LDFSInode()
                inode.update(row)
                
                if inode.parent_id in self.inode_map:
                    self.inode_map[inode.parent_id].append(inode.id)
                else:
                    self.inode_map[inode.parent_id] = [inode.id]
                self.inode_table[inode.id] = inode
            if len(self.inode_table) == 0:
                root_inode = self.create_inode('/')
                #Dump root inode into metadata in the CREATE_NODE function
                assert root_inode != 0
                #Assertion error if creation failed
            else:
                root_inode = self.get_inode_by_name('/')

            self.root_inode_id = root_inode.id

    def create_inode(self, filename, parent_inode_id = -1):
        filename_last = os.path.basename(filename)
        if (self.exist(filename, parent_inode_id)):
            print('File already exists')
            return 0
        else:
            if (filename == '/'):
                inode = LDFSInode({"inode_type":"d", "name": '/', "parent_id" : '-1', "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
            else:
                inode = LDFSInode({"inode_type":"d", "name": filename_last, "parent_id" : parent_inode_id, "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
            self.metadata.add_inode(inode)
            # Write the new node to the database
            
            #But we don't know the inode_id for this inode, so we query back for inode it
            rows = self.metadata.search_inode_with_parent(parent_inode_id, filename_last) 
            if rows == 0:
                print('Create inode failed')
                return 0
            else:
                inode.id = rows[0]
                assert inode.id not in self.inode_table
                self.inode_table[inode.id] = inode
                if parent_inode_id in self.inode_map:
                    self.inode_map[parent_inode_id].append(inode.id)
                else:
                    self.inode_map[parent_inoded_id] = [inode_id]
            return inode


        def get_inode_by_name(self, filename):
            #Note that filename is the full path
            if (filename == '/'):
                return self.inode_table['0']
            else:
                filename_splited = filename.split('/')[1:] # abanding the leading \/
                parent_inode_id = '0'
                for name in filename_splited:
                    child_inodes_ids = self.inode_map[parent_node_id]
                    flag = False
                    for child_inode_id in child_inode_ids:
                        if name == self.inode_table[child_inode_id].name:
                            flag = True
                            parent_inode_id = self.inode_table[child_inode_id].id
                            res = child_inode_id
                            break
                        else:
                            flag = False
                            continue
                    if (flag == False):
                        break
                if (flag == False):
                    print("Can not find %s" % (filename))
                    return 0
                else:
                    return self.inode_table[res]


def main():
    parser = argparse.ArgumentParser(description='LDFS master server')
    parser.add_argument('--config', dest = 'config', default = '/etc/ldfs/master.cfg', help = 'Configuration file')
    parser.add_argument('--host', dest = 'host', default = 'localhost', help = 'The address of master node')
    parser.add_argument('--port', dest = 'port', default = '9523', help = 'The port listen on')
    parser.add_argument('--backend', dest = 'backend', default = 'sqlite', help = 'The database we utilized to save the metadata file') 
    parser.add_argument('--rootfs', dest = 'rootfs', default = '/tmp', help = 'Data path')
    parser.add_argument('--db_filename', dest = 'db_filename', default = 'ldfs.db', help = 'Data path')
    parser.add_argument('--init', dest = 'init', default = 0, type = int, help = 'Initialize the file system or not')
    #Parse argmuents
    args = parser.parse_args()
    
    #Load configuration
    config = args.config
    #Need try exception desing

    host = args.host
    port = args.port
    backend = args.backend
    db_filename = args.db_filename
    init = args.init

    #Create server
    server = SimpleXMLRPCServer((host, port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
    server.register_introspection_functions()
    server.register_instance(LDFSMaster(backend, rootfs, db_filename, init)
    server.serve_forever()
if __name__ == '__main__':
    main()
