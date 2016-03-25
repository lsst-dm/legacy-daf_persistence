#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

"""This module defines the ButlerSubset class and the ButlerDataRefs contained
within it as well as an iterator over the subset."""

from __future__ import with_statement

class ButlerSubset(object):

    """ButlerSubset is a container for ButlerDataRefs.  It represents a
    collection of data ids that can be used to obtain datasets of the type
    used when creating the collection or a compatible dataset type.  It can be
    thought of as the result of a query for datasets matching a partial data
    id.

    The ButlerDataRefs are generated at a specified level of the data id
    hierarchy.  If that is not the level at which datasets are specified, the
    ButlerDataRef.subItems() method may be used to dive further into the
    ButlerDataRefs.

    ButlerSubsets should generally be created using Butler.subset().

    This mechanism replaces the creation of butlers using partial dataIds.

    Public methods:

    __init__(self, butler, datasetType, level, dataId)

    __len__(self)

    __iter__(self)

    """

    def __init__(self, butler, datasetType, level, dataId):
        """
        Create a ButlerSubset by querying a butler for data ids matching a
        given partial data id for a given dataset type at a given hierarchy
        level.

        @param butler (Butler)    butler that is being queried.
        @param datasetType (str)  the type of dataset to query.
        @param level (str)        the hierarchy level to descend to. if empty string will look up the default
                                  level.
        @param dataId (dict)      the (partial or complete) data id.
        """
        self.butler = butler
        self.datasetType = datasetType
        self.dataId = dataId
        self.cache = []
        self.level = level

        keys = self.butler.getKeys(datasetType, level)
        if keys is None:
            return
        fmt = list(keys.iterkeys())

        # Don't query if we already have a complete dataId
        completeId = True
        for key in fmt:
            if key not in dataId:
                completeId = False
                break
        if completeId:
            self.cache.append(dataId)
            return

        idTuples = butler.queryMetadata(self.datasetType, fmt, self.dataId)
        for idTuple in idTuples:
            tempId = dict(self.dataId)
            if len(fmt) == 1:
                tempId[fmt[0]] = idTuple
            else:
                for i in xrange(len(fmt)):
                    tempId[fmt[i]] = idTuple[i]
            self.cache.append(tempId)

    def __repr__(self):
        return "ButlerSubset(butler=%s, datasetType=%s, dataId=%s, cache=%s, level=%s)" % (
            self.butler, self.datasetType, self.dataId, self.cache, self.level)

    def __len__(self):
        """
        Number of ButlerDataRefs in the ButlerSubset.

        @returns (int)
        """

        return len(self.cache)

    def __iter__(self):
        """
        Iterator over the ButlerDataRefs in the ButlerSubset.

        @returns (ButlerIterator)
        """

        return ButlerSubsetIterator(self)

class ButlerSubsetIterator(object):
    """
    An iterator over the ButlerDataRefs in a ButlerSubset.
    """

    def __init__(self, butlerSubset):
        self.butlerSubset = butlerSubset
        self.iter = iter(butlerSubset.cache)

    def __iter__(self):
        return self

    def next(self):
        return ButlerDataRef(self.butlerSubset, self.iter.next())

class ButlerDataRef(object):
    """
    A ButlerDataRef is a reference to a potential dataset or group of datasets
    that is portable between compatible dataset types.  As such, it can be
    used to create or retrieve datasets.

    ButlerDataRefs are (conceptually) created as elements of a ButlerSubset by
    Butler.subset().  They are initially specific to the dataset type passed
    to that call, but they may be used with any other compatible dataset type.
    Dataset type compatibility must be determined externally (or by trial and
    error).

    ButlerDataRefs may be created at any level of a data identifier hierarchy.
    If the level is not one at which datasets exist, a ButlerSubset
    with lower-level ButlerDataRefs can be created using
    ButlerDataRef.subItems().

    Public methods:

    get(self, datasetType=None, **rest)

    put(self, obj, datasetType=None, **rest)

    subItems(self, level=None)

    datasetExists(self, datasetType=None, **rest)

    getButler(self)
    """

    def __init__(self, butlerSubset, dataId):
        """
        For internal use only.  ButlerDataRefs should only be created by
        ButlerSubset and ButlerSubsetIterator.
        """

        self.butlerSubset = butlerSubset
        self.dataId = dataId

    def __repr__(self):
        return 'ButlerDataRef(butlerSubset=%s, dataId=%s)' %(self.butlerSubset, self.dataId)

    def get(self, datasetType=None, **rest):
        """
        Retrieve a dataset of the given type (or the type used when creating
        the ButlerSubset, if None) as specified by the ButlerDataRef.

        @param datasetType (str)  dataset type to retrieve.
        @param **rest             keyword arguments with data identifiers
        @returns object corresponding to the given dataset type.
        """
        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        return self.butlerSubset.butler.get(datasetType, self.dataId, **rest)

    def put(self, obj, datasetType=None, doBackup=False, **rest):
        """
        Persist a dataset of the given type (or the type used when creating
        the ButlerSubset, if None) as specified by the ButlerDataRef.

        @param obj                object to persist.
        @param datasetType (str)  dataset type to persist.
        @param doBackup           if True, rename existing instead of overwriting
        @param **rest             keyword arguments with data identifiers

        WARNING: Setting doBackup=True is not safe for parallel processing, as it
        may be subject to race conditions.
        """

        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        self.butlerSubset.butler.put(obj, datasetType, self.dataId, doBackup=doBackup, **rest)

    def subLevels(self):
        """
        Return a list of the lower levels of the hierarchy than this
        ButlerDataRef.

        @returns (iterable)  list of strings with level keys."""

        return set(
                self.butlerSubset.butler.getKeys(
                    self.butlerSubset.datasetType).keys()
            ) - set(
                self.butlerSubset.butler.getKeys(
                    self.butlerSubset.datasetType,
                    self.butlerSubset.level).keys()
            )

    def subItems(self, level=None):
        """
        Generate a ButlerSubset at a lower level of the hierarchy than this
        ButlerDataRef, using it as a partial data id.  If level is None, a
        default lower level for the original ButlerSubset level and dataset
        type is used.

        @param level (str)   the hierarchy level to descend to.
        @returns (ButlerSubset) resulting from the lower-level query or () if
                                there is no lower level.
        """

        if level is None:
            mappers = self.butlerSubset.butler.repository.mappers()
            if len(mappers) != 1:
                raise RuntimeError("Support for multiple repositories not yet implemented!")
            mapper = mappers[0]

            # todo: getDefaultSubLevel is not in the mapper API!
            level = mapper.getDefaultSubLevel(self.butlerSubset.level)
            if level is None:
                return ()
        return self.butlerSubset.butler.subset(self.butlerSubset.datasetType,
                level, self.dataId)

    def datasetExists(self, datasetType=None, **rest):
        """
        Determine if a dataset exists of the given type (or the type used when
        creating the ButlerSubset, if None) as specified by the ButlerDataRef.

        @param datasetType (str) dataset type to check.
        @param **rest            keywords arguments with data identifiers
        @returns bool
        """
        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        return self.butlerSubset.butler.datasetExists(
                datasetType, self.dataId, **rest)

    def getButler(self):
        """
        Return the butler associated with this data reference.
        """
        return self.butlerSubset.butler
