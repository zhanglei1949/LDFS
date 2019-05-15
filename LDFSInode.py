import os
class LDFSInode:
    def __init__(self, params):
        self.id = ""
        self.name = ""
        self.inode_type = ""
        self.parent_id = ""
        self.perms = ""
        self.uid = 0 #user id
        self.gid = 0 #group id
        self.attrs = ""
        self.c_time = 0 #create time
        self.m_time = 0 #last modified time
        self.a_time = 0
        #self.links = 0
        self.size = 0
        #self.chunks = []
        if params is not None:
            for attr in ['id', 'name', 'inode_type', 'parent_id', 'c_time','a_time','m_time', 'uid', 'gid']:
                if attr in params:
                    setattr(self, attr, params[attr])
        self.size = 0
    def update(self, inode_attr):
        try:
            assert len(inode_attr) == 12
            self.id = inode_attr[0]
            self.parent_id = inode_attr[1]
            self.name = inode_attr[2]
            self.inode_type = inode_attr[3]
            self.perms = inode_attr[4]
            self.uid = int(inode_attr[5])
            self.gid = int(inode_attr[6])
            self.attrs = inode_attr[7]
            self.c_time = inode_attr[8]
            self.m_time = inode_attr[9]
            self.a_time = inode_attr[10]
            self.size = int(inode_attr[11])
        except:
            print("update inode" + self.id + "failed")

if __name__ == '__main__':
    params = { 'id' : 1, 'name' : 'first', 'inode_type' : 'file'}
    node = LDFSInode(params)
    print(node.id)
    print(node.inode_type)
