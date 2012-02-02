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

    """ButlerSubset is a container for ButlerDataRefs.

    [...]

    Public methods:

    [...]
    """

    def __init__(self, butler, datasetType, level, dataId):
        """
        """

        self.butler = butler
        self.datasetType = datasetType
        self.level = level
        self.dataId = dataId
        self.cache = []

        fmt = list(self.butler.getKeys(datasetType, level))
        for tuple in butler.queryMetadata(self.datasetType,
                level, fmt, self.dataId):
            tempId = dict(self.dataId)
            for i in xrange(len(fmt)):
                tempId[fmt[i]] = tuple[i]
            self.cache.append(tempId)

    def __len__(self):
        """
        """

        return len(self.cache)

    def __iter__(self):
        """
        """

        return ButlerIterator(self)

class ButlerIterator(object):
    """
    """

    def __init__(self, butlerSubset):
        """
        """

        self.butlerSubset = butlerSubset
        self.iter = iter(butlerSubset.cache)

    def __iter__(self):
        """
        """

        return self

    def next(self):
        """
        """

        return ButlerDataRef(self.butlerSubset, self.iter.next())

class ButlerDataRef(object):
    """
    """

    def __init__(self, butlerSubset, dataId):
        """
        """

        self.butlerSubset = butlerSubset
        self.dataId = dataId

    def get(self, datasetType=None):
        """
        """

        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        return self.butlerSubset.butler.get(datasetType, self.dataId)

    def put(self, obj, datasetType=None):
        """
        """

        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        self.butlerSubset.butler.put(obj, datasetType, self.dataId)

    def subItems(self, level=None):
        """
        """

        if level is None:
            level = self.butlerSubset.butler.mapper.getDefaultSubLevel(
                    self.butlerSubset.level)
        return ButlerSubset(self.butlerSubset.butler,
                self.butlerSubset.datasetType, level, self.dataId)
