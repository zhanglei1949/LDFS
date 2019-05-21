####
# Running on master node
####
import argparse

import os, time
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

from LDFSMetadata import LDFSMetadataSqlite
from LDFSInode import LDFSInode
# Store all ids in STRING 

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_path = ('/RPC2',)
class LDFSMaster:
    def __init__(self, metadata_backend, rootfs, db_filename, init):
         #Currently don't store real data, just fake metadata
         self.metadata_backend = metadata_backend
         self.rootfs = rootfs
         self.db_filename = db_filename
         self.root_inode_id = '0'
         self.inode_table = {}# {inode.id : LDFSInode}
         self.inode_map = {} # {inode.id : inode.id}

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
         self.printStat()
    def printStat(self):
        print('Inode table')
        for k in self.inode_table:
            print(self.inode_table[k].name, self.inode_table[k].id),
        print('')
        print('Inode map')
        for k in self.inode_map:
            print(k, self.inode_map[k])

    def exist(self, filename):
        
        res = self.get_inode_by_name(filename)
        if (res == 0):
            return 0
        else:
            return 1
    def load_inodes(self):
        
        print("Loading inodes")
        try:
            #cursor = self.metadata.db.cursor()
            inodes = self.metadata.get_inodes()
            print("Get %d inodes from db" % len(inodes))
            #Load the inodes, and store them in memory
            for row in inodes:
                inode = LDFSInode()
                inode.update(row)
                
                if inode.parent_id in self.inode_map:
                    self.inode_map[str(inode.parent_id)].append(str(inode.id))
                else:
                    self.inode_map[str(inode.parent_id)] = [str(inode.id)]
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
        except Exception, e:
            print(e)
            exit("Load inodes failed")
            

    def create_inode(self, filename, parent_inode_id): # default parent node is root 0
        print("creating inode %s under %s" % (filename, parent_inode_id))
        filename_last = os.path.basename(filename)
        if (filename == '/'):
            filename_last = '/'
        # Get the last string of filename
        if (self.exist(filename)):
            print('File already exists')
            return 0
        else:
            if (filename == '/'):
                inode = LDFSInode({"inode_type":"d", "name": '/', "parent_id" : '0', "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
            else:
                inode = LDFSInode({"inode_type":"d", "name": filename_last, "parent_id" : parent_inode_id, "perms": 777, "uid" : 0, "gid": 0, "attrs":"", "c_time":int(time.time()), "m_time":int(time.time()), "a_time":int(time.time()), "size" : 0 })
            self.metadata.add_inode(inode)
            # Write the new node to the database
            #But we don't know the inode_id for this inode, so we query back for inode it
            rows = self.metadata.search_inode_with_parent(parent_inode_id, filename_last) 
            # rows's length is at most 1
            if rows == 0:
                print('Create inode failed')
                return 0
            else:
                inode.id = rows[0]
                print("Successfully create inode %s" % (inode.id))
                assert str(inode.id) not in self.inode_table
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
    def delete_file(self, filename):
        # Delete the file according to name
        # Remove the both the database record and the in-memory record.
        # Recursively
        res = self.get_inode_by_name(filename)
        if (res == 0):
            print("No such file %s" % (filename))
            return 0
        else:
            print("Inode.id for file %s is %d" %(filename, res.id))
            deletion = self.metadata.delete_inode(res.id)
            if (deletion == 1):
                print("Deletion succeed")
                #assert (str(res.id) not in self.inode_map) | (self.inode_map[str(res.id)] == [])
                if (str(res.id) in self.inode_map):
                    if (self.inode_map[str(res.id)] != []):
                        print("Can not delete %s" % filename)
                        return 0
                assert str(res.id) in self.inode_table
                del self.inode_table[str(res.id)]
                # We also need to delete res.id in inode_map for its parent
                self.inode_map[str(res.parent_id)].remove(str(res.id))
               
                return 1
            else:
                print("Failed to delete %s with inode id %d" %(filename, res.id))
                return 0
    def delete_dir(self, dirname):
        print("Deleting direction %s" %(dirname))
        inode = self.get_inode_by_name(dirname)
        if (inode == 0):
            print("No such direction %s" % (dirname))
            return 0
        else:
            #print("Inode.id for dirname %s is %d" %(dirname, inode.id))
            # Check whether this is an empy direction
            if (str(inode.id) in self.inode_map):
                if (self.inode_map[str(inode.id)] != []):
                    print("Direction %s is not empty!" % (dirname))
                    return 0

            deletion = self.metadata.delete_inode(inode.id)
            if (deletion == 1):
                #assert (str(res.id) not in self.inode_map) | (self.inode_map[str(res.id)] == [])
                if (str(inode.id) not in self.inode_table):
                    raise Exception("Delete dir in database but not found in inode table")
                del self.inode_table[str(inode.id)]
                if (str(inode.id) in self.inode_map):
                    del self.inode_map[str(inode.id)]# must be []
                # We also need to delete res.id in inode_map for its parent
                self.inode_map[str(inode.parent_id)].remove(str(inode.id))
                if (self.inode_map[str(inode.parent_id)] == []):
                    del self.inode_map[str(inode.parent_id)]
                print("Deletion succeed")
                return 1
            else:
                print("Failed to delete direction %s with inode id %d" %(dirname, inode.id))
                return 0

    def delete(self, filename):
        print("Deleting file %s" %(filename))
        res = self.get_inode_by_name(filename)
        if (res == 0): 
            print("No such file %s" % (filename))
            return 0
        else:
            if (res.inode_type == 'd'):
                return self.delete_dir(filename)
            elif (res.inode_type == 'f'):
                return self.delete_file(filename)
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
        assert os.path.dirname(filename) == os.path.dirname(new_filename)
        new_filename_base = os.path.basename(new_filename)
        res = self.get_inode_by_name(filename)
        if (res == 0):
            print("No such file %s" % (filename))
            return 0
        else:
            print("Inode.id for file %s is %d" %(filename, res.id))
            rename_res = self.metadata.update_name(res.id, new_filename_base)
            res.name = new_filename_base
            if (rename_res == 1):
                return 0
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
        # We don't alloc real chunks
        if attributes != 0:
            inode =  LDFSInode(attributes)
            inode.name = os.path.basename(filename)
            inode.parent_id = self.get_parent_inode_from_filename(filename).id
            if (inode.parent_id == 0):
                raise Exception("Direction doesn't exists" + os.path.dirname(filename))
            self.metadata.add_inode(inode)

            #after insertion, search
            res = self.metadata.search_inode_with_parent(inode.parent_id, inode.name)
            if res == 0:
                print('Allocation failed')
                return 0
            else:
                inode.id = res[0]
                print("Successfully alloc inode %s" % (inode.id))
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
    parser.add_argument('--config', dest = 'config', default = '/etc/ldfs/master.cfg', help = 'Configuration file')
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
    server.register_instance(LDFSMaster(backend, rootfs, db_filename, init))
    server.serve_forever()
if __name__ == '__main__':
    main()
