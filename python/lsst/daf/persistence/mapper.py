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
from builtins import object

import yaml

from . import Policy

"""This module defines the Mapper base class."""


class Mapper(object):
    """Mapper is a base class for all mappers.

    Subclasses may define the following methods:

    map_{datasetType}(self, dataId, write)
        Map a dataset id for the given dataset type into a ButlerLocation.
        If write=True, this mapping is for an output dataset.

    query_{datasetType}(self, key, format, dataId)
        Return the possible values for the format fields that would produce
        datasets at the granularity of key in combination with the provided
        partial dataId.

    std_{datasetType}(self, item)
        Standardize an object of the given data set type.

    Methods that must be overridden:

    keys(self)
        Return a list of the keys that can be used in data ids.

    Other public methods:

    __init__(self)

    getDatasetTypes(self)

    map(self, datasetType, dataId, write=False)

    queryMetadata(self, datasetType, key, format, dataId)

    canStandardize(self, datasetType)

    standardize(self, datasetType, item, dataId)

    validate(self, dataId)
    """

    @staticmethod
    def Mapper(cfg):
        '''Instantiate a Mapper from a configuration.
        In come cases the cfg may have already been instantiated into a Mapper, this is allowed and
        the input var is simply returned.

        :param cfg: the cfg for this mapper. It is recommended this be created by calling
                    Mapper.cfg()
        :return: a Mapper instance
        '''
        if isinstance(cfg, Policy):
            return cfg['cls'](cfg)
        return cfg

    def __new__(cls, *args, **kwargs):
        """Create a new Mapper, saving arguments for pickling.

        This is in __new__ instead of __init__ to save the user
        from having to save the arguments themselves (either explicitly,
        or by calling the super's __init__ with all their
        *args,**kwargs.  The resulting pickling system (of __new__,
        __getstate__ and __setstate__ is similar to how __reduce__
        is usually used, except that we save the user from any
        responsibility (except when overriding __new__, but that
        is not common).
        """
        self = super(Mapper, cls).__new__(cls)
        self._arguments = (args, kwargs)
        return self

    def __init__(self, **kwargs):
        pass

    def __getstate__(self):
        return self._arguments

    def __setstate__(self, state):
        self._arguments = state
        args, kwargs = state
        self.__init__(*args, **kwargs)

    def keys(self):
        raise NotImplementedError("keys() unimplemented")

    def queryMetadata(self, datasetType, format, dataId):
        """Get possible values for keys given a partial data id.

        :param datasetType: see documentation about the use of datasetType
        :param key: this is used as the 'level' parameter
        :param format:
        :param dataId: see documentation about the use of dataId
        :return:
        """
        func = getattr(self, 'query_' + datasetType)

        val = func(format, self.validate(dataId))
        return val

    def getDatasetTypes(self):
        """Return a list of the mappable dataset types."""

        list = []
        for attr in dir(self):
            if attr.startswith("map_"):
                list.append(attr[4:])
        return list

    def map(self, datasetType, dataId, write=False):
        """Map a data id using the mapping method for its dataset type."""

        func = getattr(self, 'map_' + datasetType)
        return func(self.validate(dataId), write)

    def canStandardize(self, datasetType):
        """Return true if this mapper can standardize an object of the given
        dataset type."""

        return hasattr(self, 'std_' + datasetType)

    def standardize(self, datasetType, item, dataId):
        """Standardize an object using the standardization method for its data
        set type, if it exists."""

        if hasattr(self, 'std_' + datasetType):
            func = getattr(self, 'std_' + datasetType)
            return func(item, self.validate(dataId))
        return item

    def validate(self, dataId):
        """Validate a dataId's contents.

        If the dataId is valid, return it.  If an invalid component can be
        transformed into a valid one, copy the dataId, fix the component, and
        return the copy.  Otherwise, raise an exception."""

        return dataId

    def backup(self, datasetType, dataId):
        """Rename any existing object with the given type and dataId.

        Not implemented in the base mapper.
        """
        raise NotImplementedError("Base-class Mapper does not implement backups")

    def getRegistry(self):
        """Get the registry"""
        return None
