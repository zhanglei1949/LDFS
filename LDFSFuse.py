from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from stat import S_IFDIR, S_IFLNK, S_IFREG
import os, time, base64, xmlrpclib,argparse
from errno import ENOENT
import logging

#from  LDFSClientLib import LDFSClientLib 
#class LDFSClientFuse(LDFSClientLib, Operations):
class LDFSClientFuse(LoggingMixIn, Operations):
    def __init__(self, master_host):
        self.master = xmlrpclib.ServerProxy(master_host)
        self.fd = 1
    def exist(self, path):
        return self.master.exist(path)

    def rename(self, old_name, new_name):
        print("FUSE rename %s to %s" % (old_name, new_name))
        return self.master.rename(old_name, new_name)
    
    def ldfs_write(self, path, data, offset, fh):
        # No consideration of size
        if self.exist(path):
            # If file exist, we update the attributes
            inode = self.master.get_inode_by_name(path)
            if (inode != 0):
                inode.c_time = int(time.time())
                inode.a_time = int(time.time())
                inode.m_time = int(time.time())
                inode.size = inode.size + len(data)
                #self.master.dump()
                return 1
            else:
                return 0
        else :
            attributes = {"inode_type":"f", "a_time":int(time.time()), "c_time":int(time.time()), "m_time":int(time.time()), "size": len(data), "attrs":""}
            if self.master.alloc(path, attributes) == 0:
                return 0
            else:
                return 1
    def write(self, path, data, offset, fh):# file head
        #if offset > 0:
        #    return self.eafs_write_append(path, data, offset, fh)
        #return self.eafs_write(path, data, fh)
        return self.ldfs_write(path, data, offset, fh)

    def mkdir(self, dir_path, mode):
        print("FUSE mkdir %s" % (dir_path))
        attributes = {"inode_type" : "d",
                    "a_time" : int(time.time()),
                    "c_time" : int(time.time()),
                    "m_time" : int(time.time()),
                    "attrs" : ""}
        res = self.master.alloc(dir_path, attributes)
    def create(self, path, mode):# create path without storing info
        print("FUSE creating %s" % (path))
        attributes = {"inode_type" : "f",
                    "a_time" : int(time.time()),
                    "c_time" : int(time.time()),
                    "m_time" : int(time.time()),
                    "size" : 0,
                    "attrs" : ""}
        self.master.alloc(path, attributes)
        #self.master.dump_metadata()
        self.fd += 1
        return self.fd


    def rmdir(self, dir_path):
        print("FUSE deleting dir %s " % (dir_path))
        self.master.delete(dir_path)
        #let master determine this as dir or file

    def readdir(self, path, fh):
        # read one direction?
        files = self.master.list_files(path)
        res = {}
        now = time.time()
        
        for f in files:
            print(f)
            #new_filename = base64.b64decode(f['name'])
            new_filename = f['name']
            if f['inode_type'] == 'd':
                res[new_filename] = dict(st_mode=( S_IFDIR | 0755), st_size = 4096, st_ctime = now, st_mtime = f['m_time'], st_atime = f['a_time'], st_nlink = 2)
            else:
                res[new_filename] = dict(st_mode=( S_IFREG | 0755), st_size = f['size'], st_ctime = now, st_mtime = f['m_time'], st_atime = f['a_time'], st_nlink = 1)
        return ['.', '..'] + [ x for x in res]
        #return ['.', '..'] 

    def ldfs_read(self, path, size, offset):
        if self.exist(path):
            # read changes the last access  time
            inode = self.get_inode_by_name(path)
            if (inode == 0):
                raise Exception("metadata server said exists while not found" + path)
                
            else:
                inode.a_time = int(time.time())
                #self.master.dump_metadata()
                return 1
        else:
            raise Exception("file doesn't exist" + path)
    def read(self, path, size, offset, fh):
        return self.ldfs_read(path, size, offset)

    
    def getattr(self, path, fh = None):
        # return the status of a file
        now = int(time.time())
        print("Getting attrs for %s " % path)
        file_attrs = self.master.stat(path)
        if (file_attrs == 0):
            raise FuseOSError(ENOENT)
        if file_attrs['inode_type'] == 'd':
                file_stat = dict(st_mode=( S_IFDIR | 0755), st_size = 4096, st_ctime = file_attrs['c_time'], st_mtime = file_attrs['m_time'], st_atime = file_attrs['a_time'], st_nlink = 0)
        elif file_attrs['inode_type'] == 'f':
                file_stat = dict(st_mode=( S_IFREG | 0755), st_size = file_attrs['size'], st_ctime =file_attrs['c_time'], st_mtime = file_attrs['m_time'], st_atime = file_attrs['a_time'], st_nlink = 0)
        else:
            raise FuseOSError(ENOENT)
        return file_stat
    def unlink(self, path):
        print("FUSE delete %s" % path)
        self.master.delete(path)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LDFS Fuse Client')
    parser.add_argument('--mount', dest='mount_point', default='/home/lei/2019/bigdata/LDFS/LDFS/mnt', help='Mount point for LDFS fuse')
    parser.add_argument('--master', dest='master', default='localhost:9523', help='Master server address')
    args = parser.parse_args()
    master = 'http://' + args.master
    logging.basicConfig(level = logging.DEBUG)
    fuse = FUSE(LDFSClientFuse(master), args.mount_point, foreground=True)
