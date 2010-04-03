#!/usr/bin/env python

import glob
import os
import re
import lsst.pex.policy as pexPolicy

class Registry(object):
    def __init__(self):
        pass

    @staticmethod
    def create(location, policy):
        if re.match(r'.*\.registry', location):
            return FileRegistry(location, policy)
        # if re.match(r'.*\.sqlite3', location):
        #     return SqliteRegistry(location, policy)
        if re.match(r'mysql:', location):
            return DbRegistry(location, policy)
        return FsRegistry(location, policy)

class FsRegistry(Registry):
    def __init__(self, location, policy):
        Registry.__init__(self)

        fsRegistryDict = pexPolicy.DefaultPolicyFile("daf_persistence",
                "FsRegistryDictionary.paf", "policy")
        fsRegistryDefaults = pexPolicy.Policy.createPolicy(fsRegistryDict,
                fsRegistryDict.getRepositoryPath())
        self.policy = policy
        self.policy.mergeDefaults(fsRegistryDefaults)
        globString = policy.get("globString")
        self.fieldList = list(policy.getArray("fieldList"))
        pathTemplate = policy.get("pathTemplate")
        intFields = policy.getArray("intFields")

        pathList = glob.glob(os.path.join(location, globString))
        self.tuples = []
        for path in pathList:
            m = re.search(pathTemplate, path)
            dataId = list(m.groups())
            for i in xrange(len(self.fieldList)):
                if intFields[i]:
                    dataId[i] = int(dataId[i])
            self.tuples.append(tuple(dataId))

    def getCollection(self, keys, dataId):
        mappedFields = set()
        for k in dataId.keys():
            if not k in self.fieldList:
                mappedFields += k
        # TODO -- handle mapped fields
        keySet = set()
        keyLocs = []
        if isinstance(keys, str):
            keyLocs.append(self.fieldList.index(keys))
        else:
            for k in keys:
                keyLocs.append(self.fieldList.index(k))
        for t in self.tuples:
            selected = True
            for i in xrange(len(self.fieldList)):
                field = self.fieldList[i]
                if dataId.has_key(field) and t[i] != dataId[field]:
                    selected = False
                    break
            if selected:
                if len(keyLocs) == 1:
                    keySet.add(t[keyLocs[0]])
                else:
                    result = []
                    for l in keyLocs:
                        result.append(t[l])
                    keySet.add(tuple(result))
        return list(keySet)

class DbRegistry(Registry):
    def __init__(self, location, policy):
        Registry.__init__(self)
        # TODO -- initialize registry
        pass

    def getCollection(self, keys, dataId):
        # TODO -- select distinct keys from registry
        pass

class FileRegistry(Registry):
    def __init__(self, location, policy):
        Registry.__init__(self)
        pass

    def getCollection(self, keys, dataId):
        pass

# class SqliteRegistry(Registry):
#     def __init__(self, location):
#         Registry.__init__(self)
#         # TODO -- initialize registry
#         pass
# 
#     def getCollection(self, keys, dataId):
#         # TODO -- select distinct keys from registry
#         pass
