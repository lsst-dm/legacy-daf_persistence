#!/usr/bin/env python
# -*- python -*-

"""This module defines the Butler class."""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
from lsst.daf.persistence import StorageList, LogicalLocation, ReadProxy

class Butler(object):
    """Butler provides a generic mechanism for persisting and retrieving data using mappers.

    Butlers should always be created using ButlerFactory.create().
    
    A Butler manages an input collection and an output collection.  Each
    collection is composed of data sets.  Each data set has a type
    representing its intended usage and a location.  Note that the data set
    type is not the same as the C++ or Python type of the object containing
    the data.  For example, an ExposureF object might be used to hold the data
    for a raw image, a post-ISR image, a calibrated science image, or a
    difference image.  These would all be different data set types.

    Each Butler is responsible for a subset of its input collection defined by
    the partial data identifier used to create it.  A Butler can produce a
    collection of possible values for a key (or tuples of values for multiple
    keys) if given a partial data identifier.  It can check for the existence
    of a file containing a data set given its type and data identifier.  The
    Butler can then retrieve the data set.  Similarly, it can persist an
    object to an appropriate location when given its associated data
    identifier.

    Note that the Butler has two more advanced features when retrieving a data
    set.  First, the retrieval is lazy.  Input does not occur until the data
    set is actually accessed.  This allows data sets to be retrieved and
    placed on a clipboard prospectively with little cost, even if the
    algorithm of a stage ends up not using them.  Second, the Butler will call
    a standardization hook upon retrieval of the data set.  This function,
    contained in the input mapper object, must perform any necessary
    manipulations to force the retrieved object to conform to standards,
    including translating metadata.

    Public methods:

    inputKeys(self)

    outputKeys(self)

    getCollection(self, dataSetType, keys, dataId={}, **rest)

    fileExists(self, dataSetType, dataId={}, **rest)

    get(self, dataSetType, dataId={}, **rest)

    put(self, obj, dataSetType, dataId={}, **rest)
    """

    def __init__(self, inputMapper, outputMapper, persistence, partialId={}):
        """Construct the Butler.  Only called via the ButlerFactory."""

        self.inputMapper = inputMapper
        self.outputMapper = outputMapper
        self.persistence = persistence
        self.partialId = partialId

    def inputKeys(self):
        """Returns the valid data id keys for the input collection."""

        return self.inputMapper.keys()

    def outputKeys(self):
        """Returns the valid data id keys for the output collection."""

        return self.outputMapper.keys()

    def getCollection(self, dataSetType, keys, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.
        
        @param dataSetType    the type of data set to inquire about.
        @param keys           one or more keys to retrieve values for.
        @param dataId         the partial data id.
        @param **rest         keyword arguments for the partial data id.
        @returns a list of valid values or tuples of valid values.
        """

        dataId = self._combineDicts(dataId, **rest)
        self.inputMapper.getCollection(dataSetType, keys, dataId)

    def fileExists(self, dataSetType, dataId={}, **rest):
        """Determines if a data set file exists.

        @param dataSetType    the type of data set to inquire about.
        @param dataId         the data id of the data set.
        @param **rest         keyword arguments for the data id.
        @returns True if the data set is file-based and exists.
        """

        dataId = self._combineDicts(dataId, **rest)
        location = self.inputMapper.map(dataSetType, dataId)
        additionalData = location.getAdditionalData()
        for (storageName, locationString) in location.getStorageInfo():
            if storageName == 'BoostStorage' or storageName == 'FitsStorage':
                logLoc = LogicalLocation(locationString, additionalData)
                return os.path.exists(logLoc.locString())
        return False

    def get(self, dataSetType, dataId={}, **rest):
        """Retrieves a data set given an input collection data id.
        
        @param dataSetType    the type of data set to retrieve.
        @param dataId         the data id.
        @param **rest         keyword arguments for the data id.
        @returns an object retrieved from the data set.
        """
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
        """Persists a data set given an output collection data id.
        
        @param obj            the object to persist.
        @param dataSetType    the type of data set to persist.
        @param dataId         the data id.
        @param **rest         keyword arguments for the data id.
        """
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
            if storageName == "PafStorage":
                finalItem = pexPolicy.Policy.createPolicy(logLoc.locString())
                return finalItem

            storage = self.persistence.getRetrieveStorage(storageName, logLoc)
            storageList.append(storage)

        itemData = self.persistence.unsafeRetrieve(
                location.getCppType(), storageList, additionalData)
        finalItem = pythonType.swigConvert(itemData)

        return finalItem
