from LDFSMaster import LDFSMaster
import time
backend = 'sqlite'
rootfs = '/tmp'
db_filename = 'ldfs.db'
init = 1

master = LDFSMaster(backend, rootfs, db_filename, init)
master.printStat()
master.create_inode('/home', '1')
master.create_inode('/mnt', '1')
master.create_inode('/home/lei', '2')
master.create_inode('/home/lei/a.txt','4')
master.printStat()
# Test exist
'''
print(master.exist('/home'))
print(master.exist('home'))
print(master.exist('/home/'))
print(master.exist('/'))
print(master.exist('/home/lei/'))
print(master.exist('/home/lei/a.txt'))
print(master.exist('////'))
'''
# Test delete
'''
master.delete_file('/home/lei/a.txt')
master.create_inode('/home/lei/b.txt','4')
master.delete_dir('/mnt')
master.printStat()
master.delete_dir('/home')
master.printStat()
'''
# Test rename
#print(master.rename('/home/lei/a.txt', '/home/lei/b.txt'))

# Test list file
#file_list = master.list_files('/')
#print(file_list)
#file_list = master.list_files('/home')
#print(file_list)
#all_inodes = master.metadata.get_inodes()

# Test stat
#print(master.stat('/'))
#print(master.stat('/home/lei'))
#print(master.stat('/home/lei/a.txt'))

# Test alloc
master.alloc('/home/lei/b.txt', {"inode_type" : "f", "a_time" : int(time.time())})
master.alloc('/home/lei/lei', {"inode_type" : "d", "a_time" : int(time.time())})
master.alloc('/home/lei/lei/c.txt', {"inode_type" : "f", "a_time" : int(time.time())})
master.printStat()
master.delete('/home/lei/lei/c.txt')
master.printStat()
master.delete('/home/lei/lei/')
master.printStat()

#print(all_inodes)

