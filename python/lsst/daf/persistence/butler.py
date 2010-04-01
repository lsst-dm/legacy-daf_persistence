#!/usr/bin/env python
# -*- python -*-

"""
Butler provides a generic mechanism for persisting and retrieving data
using mappers.
"""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
from lsst.daf.persistence import StorageList, LogicalLocation, ReadProxy

class Butler(object):
    """This class persists and retrieves data."""

    def __init__(self, inputMapper, outputMapper, persistence, partialId={}):
        self.inputMapper = inputMapper
        self.outputMapper = outputMapper
        self.persistence = persistence
        self.partialId = partialId

    def inputKeys(self):
        return self.inputMapper.keys()

    def outputKeys(self):
        return self.outputMapper.keys()

    def getCollection(self, dataSetType, key, dataId={}, **rest):
        dataId = self._combineDicts(dataId, **rest)
        self.inputMapper.getCollection(dataSetType, key, dataId)

    def fileExists(self, dataSetType, dataId={}, **rest):
        dataId = self._combineDicts(dataId, **rest)
        location = self.inputMapper.map(dataSetType, dataId)
        additionalData = location.getAdditionalData()
        for (storageName, locationString) in location.getStorageInfo():
            if storageName == 'BoostStorage' or storageName == 'FitsStorage':
                logLoc = LogicalLocation(locationString, additionalData)
                return os.path.exists(logLoc.locString())

    def get(self, dataSetType, dataId={}, **rest):
        dataId = self._combineDicts(dataId, **rest)
        location = self.inputMapper.map(dataSetType, dataId)

        # import this pythonType dynamically 
        pythonTypeTokenList = location.getPythonType().split('.')
        importClassString = pythonTypeTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(pythonTypeTokenList)
        importType = __import__(importPackage, globals(), locals(), \
                [importClassString], -1) 
        pythonType = getattr(importType, importClassString)
        callback = lambda: self.inputMapper.standardize(dataSetType,
                self._read(pythonType, location))
        return ReadProxy(callback)

    def put(self, obj, dataSetType, dataId={}, **rest):
        dataId = self._combineDicts(dataId, **rest)
        location = self.outputMapper.map(dataSetType, dataId)
        additionalData = location.getAdditionalData()

        # Create a list of Storages for the item.
        storageList = StorageList()
        for storageName, locationString in location.getStorageInfo():
            logLoc = LogicalLocation(locationString, additionalData)
            # self.log.log(Log.INFO, "persisting %s as %s" % (item, logLoc.locString()))
            storage = self.persistence.getPersistStorage(storageName, logLoc)
            storageList.append(storage)

        # Persist the item.
        if '__deref__' in dir(obj):
            # We have a smart pointer, so dereference it.
            self.persistence.persist(
                    obj.__deref__(), storageList, additionalData)
        else:
            self.persistence.persist(obj, storageList, additionalData)

    def _combineDicts(self, dataId, **rest):
        finalId = {}
        finalId.update(self.partialId)
        finalId.update(dataId)
        finalId.update(rest)
        return finalId

    def _map(self, mapper, dataSetType, dataId):
        return mapper.map(dataSetType, dataId)

    def _read(self, pythonType, location):
        # print "Loading", pythonType, "from", location
        additionalData = location.getAdditionalData()
        # Create a list of Storages for the item.
        storageList = StorageList()
        for (storageName, locationString) in location.getStorageInfo():
            logLoc = LogicalLocation(locationString, additionalData)
            # self.log.log(Log.INFO, "loading %s as %s" % (item, logLoc.locString()))
            storage = self.persistence.getRetrieveStorage(storageName, logLoc)
            storageList.append(storage)

        itemData = self.persistence.unsafeRetrieve(
                location.getCppType(), storageList, additionalData)
        finalItem = pythonType.swigConvert(itemData)

        return self.inputMapper.standardize(pythonType, finalItem)
