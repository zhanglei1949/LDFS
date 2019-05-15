from LDFSMaster import LDFSMaster

backend = 'sqlite'
rootfs = '/tmp'
db_filename = 'ldfs.db'
init = 1

master = LDFSMaster(backend, rootfs, db_filename, init)
master.printStat()


