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
        if re.match(r'.*\.paf', location):
            return CalibRegistry(location, policy)
        # if re.match(r'.*\.sqlite3', location):
        #     return SqliteRegistry(location, policy)
        if re.match(r'mysql:', location):
            return DbRegistry(location, policy)
        return FsRegistry(location, policy)

class FsRegistry(Registry):
    def __init__(self, location, pathTemplate):
        Registry.__init__(self)

        fmt = re.compile(r'%\((\w+)\).*?([diouxXeEfFgGcrs])')
        globString = fmt.sub('*', pathTemplate)
        last = 0
        self.fieldList = []
        intFields = []
        reString = ""
        n = 0
        for m in fmt.finditer(pathTemplate):
            fieldName = m.group(1)
            if fieldName in self.fieldList:
                fieldName += "_%d" % (n,)
                n += 1
            self.fieldList.append(fieldName)

            if m.group(2) not in 'crs':
                intFields.append(fieldName)
        
            prefix = pathTemplate[last:m.start(0)]
            last = m.end(0)
            reString += prefix

            if m.group(2) in 'crs':
                reString += r'(?P<' + fieldName + '>.+?)'
            elif m.group(2) in 'xX':
                reString += r'(?P<' + fieldName + '>[\dA-Fa-f]+?)'
            else:
                reString += r'(?P<' + fieldName + '>[\d.eE+-]+?)'

        reString += pathTemplate[last:] 

        curdir = os.getcwd()
        os.chdir(location)
        pathList = glob.glob(globString)
        os.chdir(curdir)
        self.tuples = []
        for path in pathList:
            dataId = re.search(reString, path).groupdict()
            idList = []
            for f in self.fieldList:
                if f in intFields:
                    idList.append(int(dataId[f]))
                else:
                    idList.append(dataId[f])
            self.tuples.append(tuple(idList))

    def getFields(self):
        return self.fieldList

    def queryMetadata(self, key, format, dataId):
        keySet = set()
        keyLocs = []
        # TODO fix below
        if isinstance(format, str):
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

class CalibRegistry(Registry):
    def __init__(self, location):
        Registry.__init__(self)

        self.calibDb = CalibDb(location)

    def getFields(self):
        return ["ccd", "amp", "filter", "expTime", "taiObs"]

    def queryMetadata(self, datasetType, key, format, dataId):
        # TODO fix below
        ccd = "CCD009"
        if dataId.has_key("ccd"):
            ccd = dataId['ccd']
        amp = 1
        if dataId.has_key("amp"):
            amp = dataId['amp']
        filter = None
        if dataId.has_key("filter"):
            filter = dataId['filter']
        expTime = None
        if dataId.has_key("expTime"):
            expTime = dataId['expTime']
        calibs = self.calibDb.lookup(dateTime, datasetType,
                ccd, amp, filter, expTime, all=True)
        result = []
        for c in calibs:
            if len(keys) == 1:
                result.append(getattr(c, k))
            else:
                tuple = []
                for k in keys:
                    tuple.append(getattr(c, k))
                result.append(tuple)
        return result

    def getPath(self, datasetType, dataId):
        ccd = "CCD009"
        if dataId.has_key("ccd"):
            ccd = dataId['ccd']
        amp = 1
        if dataId.has_key("amp"):
            amp = dataId['amp']
        filter = None
        if dataId.has_key("filter"):
            filter = dataId['filter']
        expTime = None
        if dataId.has_key("expTime"):
            expTime = dataId['expTime']
        return self.calibDb.lookup(dataId['taiObs'], datasetType, ccd, amp,
                filter, expTime)

class DbRegistry(Registry):
    def __init__(self, location, policy):
        Registry.__init__(self)
        # TODO -- initialize registry
        pass

    def queryMetadata(self, keys, dataId):
        # TODO -- select distinct keys from registry
        pass

class FileRegistry(Registry):
    def __init__(self, location, policy):
        Registry.__init__(self)
        pass

    def queryMetadata(self, keys, dataId):
        pass


# class SqliteRegistry(Registry):
#     def __init__(self, location):
#         Registry.__init__(self)
#         # TODO -- initialize registry
#         pass
# 
#     def queryMetadata(self, keys, dataId):
#         # TODO -- select distinct keys from registry
#         pass
