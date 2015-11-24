#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2015 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# -*- python -*-

"""This module defines the Butler class."""

from __future__ import with_statement
import cPickle
import importlib
import os
import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import StorageList, LogicalLocation, ReadProxy, ButlerSubset, ButlerDataRef, \
    Persistence
from .safeFileIo import SafeFilename

class Butler(object):
    """Butler provides a generic mechanism for persisting and retrieving data using mappers.

    A Butler manages a collection of datasets known as a repository.  Each
    dataset has a type representing its intended usage and a location.  Note
    that the dataset type is not the same as the C++ or Python type of the
    object containing the data.  For example, an ExposureF object might be
    used to hold the data for a raw image, a post-ISR image, a calibrated
    science image, or a difference image.  These would all be different
    dataset types.

    A Butler can produce a collection of possible values for a key (or tuples
    of values for multiple keys) if given a partial data identifier.  It can
    check for the existence of a file containing a dataset given its type and
    data identifier.  The Butler can then retrieve the dataset.  Similarly, it
    can persist an object to an appropriate location when given its associated
    data identifier.

    Note that the Butler has two more advanced features when retrieving a data
    set.  First, the retrieval is lazy.  Input does not occur until the data
    set is actually accessed.  This allows datasets to be retrieved and
    placed on a clipboard prospectively with little cost, even if the
    algorithm of a stage ends up not using them.  Second, the Butler will call
    a standardization hook upon retrieval of the dataset.  This function,
    contained in the input mapper object, must perform any necessary
    manipulations to force the retrieved object to conform to standards,
    including translating metadata.

    Public methods:

    __init__(self, root, mapper=None, **mapperArgs)

    defineAlias(self, alias, datasetType)

    getKeys(self, datasetType=None, level=None)

    queryMetadata(self, datasetType, keys, format=None, dataId={}, **rest)

    datasetExists(self, datasetType, dataId={}, **rest)

    get(self, datasetType, dataId={}, immediate=False, **rest)

    put(self, obj, datasetType, dataId={}, **rest)

    subset(self, datasetType, level=None, dataId={}, **rest)

    dataRef(self, datasetType, level=None, dataId={}, **rest)
    """

    @staticmethod
    def getMapperClass(root):
        """Return the mapper class associated with a repository root."""

        # Find a "_mapper" file containing the mapper class name
        basePath = root
        mapperFile = "_mapper"
        globals = {}
        while not os.path.exists(os.path.join(basePath, mapperFile)):
            # Break abstraction by following _parent links from CameraMapper
            if os.path.exists(os.path.join(basePath, "_parent")):
                basePath = os.path.join(basePath, "_parent")
            else:
                raise RuntimeError(
                        "No mapper provided and no %s available" %
                        (mapperFile,))
        mapperFile = os.path.join(basePath, mapperFile)

        # Read the name of the mapper class and instantiate it
        with open(mapperFile, "r") as f:
            mapperName = f.readline().strip()
        components = mapperName.split(".")
        if len(components) <= 1:
            raise RuntimeError("Unqualified mapper name %s in %s" %
                    (mapperName, mapperFile))
        pkg = importlib.import_module(".".join(components[:-1]))
        return getattr(pkg, components[-1])

    def __init__(self, root, mapper=None, **mapperArgs):
        """Construct the Butler.  If no mapper class is provided, then a file
        named "_mapper" is expected to be found in the repository, which
        must be a filesystem path.  The first line in that file is read and
        must contain the fully-qualified name of a Mapper subclass, which is
        then imported and instantiated using the root and the mapperArgs.

        @param root (str)       the repository to be managed (at least
                                initially).  May be None if a mapper is
                                provided.
        @param mapper (Mapper)  if present, the Mapper subclass instance
                                to be used as the butler's mapper.
        @param **mapperArgs     arguments to be passed to the mapper's
                                __init__ method, in addition to the root.
        """

        self.datasetTypeAliasDict = {}

        if mapper is not None:
            self.mapper = mapper
        else:
            cls = Butler.getMapperClass(root)
            self.mapper = cls(root=root, **mapperArgs)

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)
        self.log = pexLog.Log(pexLog.Log.getDefaultLog(),
                "daf.persistence.butler")

    def defineAlias(self, alias, datasetType):
        """Register an alias that will be substituted in datasetTypes.

        @param alias (str) the alias keyword. it may start with @ or not. It may not contain @ except as the
                           first character.
        @param datasetType (str) the string that will be substituted when @alias is passed into datasetType. It may
                                 not contain '@'
        """

        #verify formatting of alias:
        # it can have '@' as the first character (if not it's okay, we will add it) or not at all.
        atLoc = alias.rfind('@')
        if atLoc is -1:
            alias = "@" + str(alias)
        elif atLoc > 0:
            raise RuntimeError("Badly formatted alias string: %s" %(alias,))

        # verify that datasetType does not contain '@'
        if datasetType.count('@') != 0:
            raise RuntimeError("Badly formatted type string: %s" %(datasetType))

        # verify that the alias keyword does not start with another alias keyword,
        # and vice versa
        for key in self.datasetTypeAliasDict:
            if key.startswith(alias) or alias.startswith(key):
                raise RuntimeError("Alias: %s overlaps with existing alias: %s" %(alias, key))

        self.datasetTypeAliasDict[alias] = datasetType

    def getKeys(self, datasetType=None, level=None):
        """Returns a dict.  The dict keys are the valid data id keys at or
        above the given level of hierarchy for the dataset type or the entire
        collection if None.  The dict values are the basic Python types
        corresponding to the keys (int, float, str).

        @param datasetType (str)  the type of dataset to get keys for, entire
                                  collection if None.
        @param level (str)        the hierarchy level to descend to or None.
        @returns (dict) valid data id keys; values are corresponding types.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        return self.mapper.getKeys(datasetType, level)

    def queryMetadata(self, datasetType, key, format=None, dataId={}, **rest):
        """Returns the valid values for one or more keys when given a partial
        input collection data id.

        @param datasetType (str)    the type of dataset to inquire about.
        @param key (str)            a key giving the level of granularity of the inquiry.
        @param format (str, tuple)  an optional key or tuple of keys to be returned.
        @param dataId (dict)        the partial data id.
        @param **rest               keyword arguments for the partial data id.
        @returns (list) a list of valid values or tuples of valid values as
        specified by the format (defaulting to the same as the key) at the
        key's level of granularity.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = self._combineDicts(dataId, **rest)
        if format is None:
            format = (key,)
        elif not hasattr(format, '__iter__'):
            format = (format,)
        tuples = self.mapper.queryMetadata(datasetType, key, format, dataId)
        if len(format) == 1:
            return [x[0] for x in tuples]
        return tuples

    def datasetExists(self, datasetType, dataId={}, **rest):
        """Determines if a dataset file exists.

        @param datasetType (str)   the type of dataset to inquire about.
        @param dataId (dict)       the data id of the dataset.
        @param **rest              keyword arguments for the data id.
        @returns (bool) True if the dataset exists or is non-file-based.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = self._combineDicts(dataId, **rest)
        location = self.mapper.map(datasetType, dataId)
        additionalData = location.getAdditionalData()
        storageName = location.getStorageName()
        if storageName in ('BoostStorage', 'FitsStorage', 'PafStorage',
                'PickleStorage', 'ConfigStorage', 'FitsCatalogStorage'):
            locations = location.getLocations()
            for locationString in locations:
                logLoc = LogicalLocation(locationString, additionalData).locString()
                if storageName == 'FitsStorage':
                    # Strip off directives for cfitsio (in square brackets, e.g., extension name)
                    bracket = logLoc.find('[')
                    if bracket > 0:
                        logLoc = logLoc[:bracket]
                if not os.path.exists(logLoc):
                    return False
            return True
        self.log.log(pexLog.Log.WARN,
                "datasetExists() for non-file storage %s, dataset type=%s, keys=%s" %
                (storageName, datasetType, str(dataId)))
        return True

    def get(self, datasetType, dataId={}, immediate=False, **rest):
        """Retrieves a dataset given an input collection data id.

        @param datasetType (str)   the type of dataset to retrieve.
        @param dataId (dict)       the data id.
        @param immediate (bool)    don't use a proxy for delayed loading.
        @param **rest              keyword arguments for the data id.
        @returns an object retrieved from the dataset (or a proxy for one).
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = self._combineDicts(dataId, **rest)
        location = self.mapper.map(datasetType, dataId)
        self.log.log(pexLog.Log.DEBUG, "Get type=%s keys=%s from %s" %
                (datasetType, dataId, str(location)))

        if location.getPythonType() is not None:
            # import this pythonType dynamically
            pythonTypeTokenList = location.getPythonType().split('.')
            importClassString = pythonTypeTokenList.pop()
            importClassString = importClassString.strip()
            importPackage = ".".join(pythonTypeTokenList)
            importType = __import__(importPackage, globals(), locals(), \
                    [importClassString], -1)
            pythonType = getattr(importType, importClassString)
        else:
            pythonType = None
        if hasattr(self.mapper, "bypass_" + datasetType):
            bypassFunc = getattr(self.mapper, "bypass_" + datasetType)
            callback = lambda: bypassFunc(datasetType, pythonType,
                    location, dataId)
        else:
            callback = lambda: self._read(pythonType, location)
        if self.mapper.canStandardize(datasetType):
            innerCallback = callback
            callback = lambda: self.mapper.standardize(datasetType,
                    innerCallback(), dataId)
        if immediate:
            return callback()
        return ReadProxy(callback)

    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        """Persists a dataset given an output collection data id.

        @param obj                 the object to persist.
        @param datasetType (str)   the type of dataset to persist.
        @param dataId (dict)       the data id.
        @param doBackup            if True, rename existing instead of overwriting
        @param **rest         keyword arguments for the data id.

        WARNING: Setting doBackup=True is not safe for parallel processing, as it
        may be subject to race conditions.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        if doBackup:
            self.mapper.backup(datasetType, dataId)
        dataId = self._combineDicts(dataId, **rest)
        location = self.mapper.map(datasetType, dataId, write=True)
        self.log.log(pexLog.Log.DEBUG, "Put type=%s keys=%s to %s" %
                (datasetType, dataId, str(location)))
        additionalData = location.getAdditionalData()
        storageName = location.getStorageName()
        locations = location.getLocations()
        # TODO support multiple output locations
        with SafeFilename(locations[0]) as locationString:
            logLoc = LogicalLocation(locationString, additionalData)
            trace = pexLog.BlockTimingLog(self.log, "put",
                                          pexLog.BlockTimingLog.INSTRUM+1)
            trace.setUsageFlags(trace.ALLUDATA)

            if storageName == "PickleStorage":
                trace.start("write to %s(%s)" % (storageName, logLoc.locString()))
                with open(logLoc.locString(), "wb") as outfile:
                    cPickle.dump(obj, outfile, cPickle.HIGHEST_PROTOCOL)
                trace.done()
                return

            if storageName == "ConfigStorage":
                trace.start("write to %s(%s)" % (storageName, logLoc.locString()))
                obj.save(logLoc.locString())
                trace.done()
                return

            if storageName == "FitsCatalogStorage":
                trace.start("write to %s(%s)" % (storageName, logLoc.locString()))
                flags = additionalData.getInt("flags", 0)
                obj.writeFits(logLoc.locString(), flags=flags)
                trace.done()
                return

            # Create a list of Storages for the item.
            storageList = StorageList()
            storage = self.persistence.getPersistStorage(storageName, logLoc)
            storageList.append(storage)
            trace.start("write to %s(%s)" % (storageName, logLoc.locString()))

            if storageName == 'FitsStorage':
                self.persistence.persist(obj, storageList, additionalData)
                trace.done()
                return

            # Persist the item.
            if hasattr(obj, '__deref__'):
                # We have a smart pointer, so dereference it.
                self.persistence.persist(obj.__deref__(), storageList, additionalData)
            else:
                self.persistence.persist(obj, storageList, additionalData)
            trace.done()

    def subset(self, datasetType, level=None, dataId={}, **rest):
        """Extracts a subset of a dataset collection.

        Given a partial dataId specified in dataId and **rest, find all
        datasets at a given level specified by a dataId key (e.g. visit or
        sensor or amp for a camera) and return a collection of their dataIds
        as ButlerDataRefs.

        @param datasetType (str)  the type of dataset collection to subset
        @param level (str)        the level of dataId at which to subset
        @param dataId (dict)      the data id.
        @param **rest             keyword arguments for the data id.
        @returns (ButlerSubset) collection of ButlerDataRefs for datasets
        matching the data id.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        if level is None:
            level = self.mapper.getDefaultLevel()
        dataId = self._combineDicts(dataId, **rest)
        return ButlerSubset(self, datasetType, level, dataId)

    def dataRef(self, datasetType, level=None, dataId={}, **rest):
        """Returns a single ButlerDataRef.

        Given a complete dataId specified in dataId and **rest, find the
        unique dataset at the given level specified by a dataId key (e.g.
        visit or sensor or amp for a camera) and return a ButlerDataRef.

        @param datasetType (str)  the type of dataset collection to reference
        @param level (str)        the level of dataId at which to reference
        @param dataId (dict)      the data id.
        @param **rest             keyword arguments for the data id.
        @returns (ButlerDataRef) ButlerDataRef for dataset matching the data id
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        subset = self.subset(datasetType, level, dataId, **rest)
        if len(subset) != 1:
            raise RuntimeError, """No unique dataset for:
    Dataset type = %s
    Level = %s
    Data ID = %s
    Keywords = %s""" % (str(datasetType), str(level), str(dataId), str(rest))
        return ButlerDataRef(subset, subset.cache[0])

    def _combineDicts(self, dataId, **rest):
        finalId = {}
        finalId.update(dataId)
        finalId.update(rest)
        return finalId

    def _read(self, pythonType, location):
        trace = pexLog.BlockTimingLog(self.log, "read",
                                      pexLog.BlockTimingLog.INSTRUM+1)

        additionalData = location.getAdditionalData()
        # Create a list of Storages for the item.
        storageName = location.getStorageName()
        results = []
        locations = location.getLocations()
        returnList = True
        if len(locations) == 1:
            returnList = False

        for locationString in locations:
            logLoc = LogicalLocation(locationString, additionalData)
            trace.start("read from %s(%s)" % (storageName, logLoc.locString()))

            if storageName == "PafStorage":
                finalItem = pexPolicy.Policy.createPolicy(logLoc.locString())
            elif storageName == "PickleStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, \
                            "No such pickle file: " + logLoc.locString()
                with open(logLoc.locString(), "rb") as infile:
                    finalItem = cPickle.load(infile)
            elif storageName == "FitsCatalogStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, \
                            "No such FITS catalog file: " + logLoc.locString()
                hdu = additionalData.getInt("hdu", 0)
                flags = additionalData.getInt("flags", 0)
                finalItem = pythonType.readFits(logLoc.locString(), hdu, flags)
            elif storageName == "ConfigStorage":
                if not os.path.exists(logLoc.locString()):
                    raise RuntimeError, \
                            "No such config file: " + logLoc.locString()
                finalItem = pythonType()
                finalItem.load(logLoc.locString())
            else:
                storageList = StorageList()
                storage = self.persistence.getRetrieveStorage(storageName, logLoc)
                storageList.append(storage)
                itemData = self.persistence.unsafeRetrieve(
                        location.getCppType(), storageList, additionalData)
                finalItem = pythonType.swigConvert(itemData)
            trace.done()
            results.append(finalItem)

        if not returnList:
            results = results[0]
        return results

    def __reduce__(self):
        return (_unreduce, (self.mapper, self.datasetTypeAliasDict))

    def _resolveDatasetTypeAlias(self, datasetType):
        """ Replaces all the known alias keywords in the given string with the alias value.
        @param (str)datasetType
        @return (str) the de-aliased string
        """

        for key in self.datasetTypeAliasDict:
            # if all aliases have been replaced, bail out
            if datasetType.find('@') == -1:
                break
            datasetType = datasetType.replace(key, self.datasetTypeAliasDict[key])

        # If an alias specifier can not be resolved then throw.
        if datasetType.find('@') != -1:
            raise RuntimeError("Unresolvable alias specifier in datasetType: %s" %(datasetType))

        return datasetType

def _unreduce(mapper, datasetTypeAliasDict):
    butler = Butler(root=None, mapper=mapper)
    butler.datasetTypeAliasDict = datasetTypeAliasDict
    return butler
