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
import collections
import copy
import cPickle
import inspect
import itertools
import os

import lsst.pex.logging as pexLog
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import StorageList, LogicalLocation, ReadProxy, ButlerSubset, ButlerDataRef, \
    Persistence, repository, Access, PosixStorage, butlerExceptions, Policy

def posixRepoCfg(root=None, mapper=None, mapperArgs=None, parentRepoCfgs=[], id=None, parentJoin='left',
                 peerCfgs=[]):
    storageCfg = PosixStorage.cfg(root=root)
    accessCfg = Access.cfg(storageCfg=storageCfg)
    repoCfg = repository.Repository.cfg(id=id, accessCfg=accessCfg, mapper=mapper, mapperArgs=mapperArgs,
                                        parentCfgs=parentRepoCfgs, parentJoin=parentJoin, peerCfgs=peerCfgs)
    return repoCfg


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

    @classmethod
    def cfg(cls, repoCfg):
        return Policy({'repoCfg':repoCfg})


    def initWithCfg(self, cfg):
        self._cfg = cfg
        self.datasetTypeAliasDict = {}

        self.repository = repository.Repository.Repository(cfg['repoCfg'])

        # Always use an empty Persistence policy until we can get rid of it
        persistencePolicy = pexPolicy.Policy()
        self.persistence = Persistence.getPersistence(persistencePolicy)
        self.log = pexLog.Log(pexLog.Log.getDefaultLog(), "daf.persistence.butler")


    def __init__(self, root, mapper=None, **mapperArgs):
        # prefered init arg is to pass a single arg; an instance of butlerCfg:
        # butler = Butler(butlerCfg(...))
        # For backward compatibility: init signature can take a posix root path, and optionally a mapper class
        # that will be init'ed with mapperArgs.
        # However, it is NOT preferred to have a repo be both input (parent) and output (self/peer), and will
        # likely break any provenance system we have in place.
        if (isinstance(root, Policy)):
            config = root
        else:
            parentCfg = posixRepoCfg(root=root, mapper=mapper, mapperArgs=mapperArgs)
            repoCfg = posixRepoCfg(root=root, mapper=mapper, mapperArgs=mapperArgs, parentRepoCfgs=(parentCfg,))
            config = Butler.cfg(repoCfg=repoCfg)
        self.initWithCfg(config)


    @staticmethod
    def getMapperClass(root):
        """posix-only; gets the mapper class at the path specifed by root (if a file _mapper can be found at
        that location or in a parent location.

        As we abstract the storage and support different types of storage locaitons this method will be
        moved entirely into Butler Access, or made more dynamic, and the API will very likely change."""
        return PosixStorage.getMapperClass(root)


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
        @param level (str)        the hierarchy level to descend to.
                                  None if it should not be restricted.
                                  empty string if the mapper should lookup the default level.
        @returns (dict) valid data id keys; values are corresponding types.
        """
        datasetType = self._resolveDatasetTypeAlias(datasetType)
        return self.repository.getKeys(datasetType, level)


    def queryMetadata(self, datasetType, format=None, dataId={}, **rest):
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
        dataId = copy.copy(dataId)
        dataId.update(**rest)

        if format is None:
            format = (key,)
        elif not hasattr(format, '__iter__'):
            format = (format,)

        tuples = self.repository.queryMetadata(datasetType, format, dataId)

        if tuples is None:
            return []

        if len(format) == 1:
            ret = []
            for x in tuples:
                try:
                    ret.append(x[0])
                except TypeError:
                    ret.append(x)
            return ret

        return tuples


    def datasetExists(self, datasetType, dataId={}, **rest):
        """Determines if a dataset file exists.

        @param datasetType (str)   the type of dataset to inquire about.
        @param dataId (dict)       the data id of the dataset.
        @param **rest              keyword arguments for the data id.
        @returns (bool) True if the dataset exists or is non-file-based.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        dataId = copy.copy(dataId)
        dataId.update(**rest)

        locations = self.repository.map(datasetType, dataId)
        if locations is None:
            return False
        try:
            if len(locations) is not 1:
                raise RuntimeError("Multiple (or none) locations for datasetExists(%s, %s)" %(datasetType, dataId))
            location = locations[0]
        except TypeError:
            # locations might not be a list; that's ok.
            location = locations

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
        dataId = copy.copy(dataId)
        dataId.update(**rest)

        locations = self.repository.map(datasetType, dataId)
        if locations is None:
            raise butlerExceptions.NoResults("No locations for get:", datasetType, dataId)
        try:
            if len(locations) is 0:
                raise butlerExceptions.NoResults("No locations for get:", datasetType, dataId)
            if len(locations) is not 1:
                raise butlerExceptions.MultipleResults("Multiple locations for get:", datasetType, dataId,
                                                       locations)
            location = locations[0]
        except TypeError:
            # locations might not be a list; that's ok.
            location = locations

        self.log.log(pexLog.Log.DEBUG, "Get type=%s keys=%s from %s" % (datasetType, dataId, str(location)))

        pythonType = location.getPythonType()
        if pythonType is not None:
            if isinstance(pythonType, basestring):
                # import this pythonType dynamically
                pythonTypeTokenList = location.getPythonType().split('.')
                importClassString = pythonTypeTokenList.pop()
                importClassString = importClassString.strip()
                importPackage = ".".join(pythonTypeTokenList)
                importType = __import__(importPackage, globals(), locals(), \
                        [importClassString], -1)
                pythonType = getattr(importType, importClassString)


        if hasattr(location.mapper, "bypass_" + datasetType):
            bypassFunc = getattr(location.mapper, "bypass_" + datasetType)
            callback = lambda: bypassFunc(datasetType, pythonType, location, dataId)
        else:
            callback = lambda: self._read(pythonType, location)
        if location.mapper.canStandardize(datasetType):
            innerCallback = callback
            callback = lambda: location.mapper.standardize(datasetType, innerCallback(), dataId)
        if immediate:
            return callback()
        return ReadProxy(callback)


    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        """Persists a dataset given an output collection data id.

        @param obj                 the object to persist.
        @param datasetType (str)   the type of dataset to persist.
        @param dataId (dict)       the data id.
        @param doBackup            if True, rename existing instead of overwriting
        @param **rest              keyword arguments for the data id.

        WARNING: Setting doBackup=True is not safe for parallel processing, as it
        may be subject to race conditions.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)
        if doBackup:
            self.repository.backup(datasetType, dataId)
        dataId = copy.copy(dataId)
        dataId.update(**rest)

        locations = self.repository.map(datasetType, dataId, write=True)
        if locations is not None:
            for location in locations:
                location.repository.write(location, obj)

    def subset(self, datasetType, level=None, dataId={}, **rest):
        """Extracts a subset of a dataset collection.

        Given a partial dataId specified in dataId and **rest, find all
        datasets at a given level specified by a dataId key (e.g. visit or
        sensor or amp for a camera) and return a collection of their dataIds
        as ButlerDataRefs.

        @param datasetType (str)  the type of dataset collection to subset
        @param level (str)        the level of dataId at which to subset. Use an empty string if the mapper
                                  should look up the default level.
        @param dataId (dict)      the data id.
        @param **rest             keyword arguments for the data id.
        @returns (ButlerSubset) collection of ButlerDataRefs for datasets
        matching the data id.
        """

        datasetType = self._resolveDatasetTypeAlias(datasetType)

        # Currently expected behavior of subset is that if specified level is None then the mapper's default
        # level should be used. Convention for level within Butler is that an empty string is used to indicate
        # 'get default'.
        if level is None:
            level = ''

        dataId = copy.copy(dataId)
        dataId.update(**rest)
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


    def _read(self, pythonType, location):
        trace = pexLog.BlockTimingLog(self.log, "read", pexLog.BlockTimingLog.INSTRUM+1)

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
        return (_unreduce, (self._cfg, self.datasetTypeAliasDict))

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

def _unreduce(cfg, datasetTypeAliasDict):
    butler = Butler(cfg)
    butler.datasetTypeAliasDict = datasetTypeAliasDict
    return butler
