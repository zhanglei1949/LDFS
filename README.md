# LDFS
A distributed metadata management module for Distirbuted File system

## Distributed metadata storage
One master, with sever metadata servers. Master determines storing which metadata one which machine.

Of couse, Master storing the linking table
Master table:
inodeId, filename? -> metadata server

metadata server table:
inodeId -> metadata
