import configparser
class InodeServer:
    def __init__(self, params):
        self.no = params[0]
        self.name = params[1]
        self.id = params[2]
        self.server = params[3]
    def toDict(self):
        return 0
def load_metadata_conf(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    # Metadata list
    metadata_servers = {}
    metadata_servers_list = [x for x in config.sections()]

    for server in metadata_servers_list:
        metadata_servers[config[server]['name']] = config[server]['addr']
    
    return metadata_servers
    # We assume the metadata servers are already running

def myhash(inode_id, multiplication, num_candidates):
    #select multiplication servers from num_candidates
    hash_seed = 23
    res = []
    for i in range(multiplication):
        res.append((int(inode_id) + hash_seed + i) % num_candidates)
    return res
