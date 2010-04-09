#!/usr/bin/env python
# -*- python -*-

"""This module defines the Butler class."""

import lsst.daf.base as dafBase
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
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

    queryMetadata(self, datasetType, keys, format=None, dataId={}, **rest)

    fileExists(self, datasetType, dataId={}, **rest)

    get(self, datasetType, dataId={}, **rest)

    put(self, obj, datasetType, dataId={}, **rest)
    """

    def __init__(self, mapper, persistence, partialId={}):
        """Construct the Butler.  Only called via the ButlerFactory."""

        self.mapper = mapper
        self.persistence = persistence
        self.partialId = partialId

    def getKeys(self):
        """Returns the valid data id keys for the dataset collection."""

        return self.mapper.getKeys()

    def queryMetadata(self, datasetType, key, format=None, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.
        
        @param datasetType the type of data set to inquire about.
        @param key         a key giving the level of granularity of the inquiry.
        @param format      an optional key or tuple of keys to be returned. 
        @param dataId      the partial data id.
        @param **rest      keyword arguments for the partial data id.
        @returns a list of valid values or tuples of valid values as specified
        by the format (defaulting to the same as the key) at the key's level
        of granularity.
        """

        dataId = self._combineDicts(dataId, **rest)
        if format is None:
            format = key
        return self.mapper.queryMetadata(datasetType, key, format, dataId)

    def fileExists(self, datasetType, dataId={}, **rest):
        """Determines if a data set file exists.

        @param datasetType    the type of data set to inquire about.
        @param dataId         the data id of the data set.
        @param **rest         keyword arguments for the data id.
        @returns True if the data set is file-based and exists.
        """

        dataId = self._combineDicts(dataId, **rest)
        location = self.mapper.map(datasetType, dataId)
        additionalData = location.getAdditionalData()
        for (storageName, locationString) in location.getStorageInfo():
            if storageName == 'BoostStorage' or storageName == 'FitsStorage':
                logLoc = LogicalLocation(locationString, additionalData)
                return os.path.exists(logLoc.locString())
        return False

    def get(self, datasetType, dataId={}, **rest):
        """Retrieves a data set given an input collection data id.
        
        @param datasetType    the type of data set to retrieve.
        @param dataId         the data id.
        @param **rest         keyword arguments for the data id.
        @returns an object retrieved from the data set (or a proxy for one).
        """
        dataId = self._combineDicts(dataId, **rest)
        location = self.mapper.map(datasetType, dataId)

        # import this pythonType dynamically 
        pythonTypeTokenList = location.getPythonType().split('.')
        importClassString = pythonTypeTokenList.pop()
        importClassString = importClassString.strip()
        importPackage = ".".join(pythonTypeTokenList)
        importType = __import__(importPackage, globals(), locals(), \
                [importClassString], -1) 
        pythonType = getattr(importType, importClassString)
        if self.mapper.canStandardize(datasetType):
            callback = lambda: self.mapper.standardize(datasetType,
                    self._read(pythonType, location), dataId)
        else:
            callback = lambda: self._read(pythonType, location)
        return ReadProxy(callback)

    def put(self, obj, datasetType, dataId={}, **rest):
        """Persists a data set given an output collection data id.
        
        @param obj            the object to persist.
        @param datasetType    the type of data set to persist.
        @param dataId         the data id.
        @param **rest         keyword arguments for the data id.
        """
        dataId = self._combineDicts(dataId, **rest)
        location = self.outputMapper.map(datasetType, dataId)
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

    def _map(self, mapper, datasetType, dataId):
        return mapper.map(datasetType, dataId)

    def _read(self, pythonType, location):
        # print "Loading", pythonType, "from", location
        additionalData = location.getAdditionalData()
        # Create a list of Storages for the item.
        storageName = location.getStorageName()
        results = []
        locations = location.getLocations()
        returnList = True
        if not hasattr(locations, "__iter__"):
            locations = [locations]
            returnList = False

        for locationString in locations:
            logLoc = LogicalLocation(locationString, additionalData)
            # self.log.log(Log.INFO, "loading %s as %s" % (item, logLoc.locString()))
            if storageName == "PafStorage":
                finalItem = pexPolicy.Policy.createPolicy(logLoc.locString())
            else:
                storageList = StorageList()
                storage = self.persistence.getRetrieveStorage(storageName, logLoc)
                storageList.append(storage)
                itemData = self.persistence.unsafeRetrieve(
                        location.getCppType(), storageList, additionalData)
                finalItem = pythonType.swigConvert(itemData)
            results.append(finalItem)

        if not returnList:
            results = results[0]
        return results
