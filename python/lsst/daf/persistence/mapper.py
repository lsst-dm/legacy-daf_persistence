#!/usr/bin/env python

"""This module defines the Mapper base class."""

class Mapper(object):
    """Mapper is a base class for all mappers.

    Subclasses may define the following methods:
    
    map_{datasetType}(self, dataId)
        Map a data id for the given data set type into a ButlerLocation.

    std_{datasetType}(self, item)
        Standardize an object of the given data set type.

    Methods that must be overridden:

    keys(self)
        Return a list of the keys that can be used in data ids.

    getCollection(self, datasetType, keys, dataId)
        Return a list of the values or tuples of values that are legal when
        combined with the given partial data id.

    Other public methods:

    __init__(self)

    getDataSetTypes(self)

    map(self, datasetType, dataId)

    standardize(self, datasetType, item)
    """

    def __init__(self):
        pass

    def keys(self):
        raise NotImplementedError("keys() unimplemented")

    def getCollection(self, datasetType, keys, dataId):
        raise NotImplementedError("getCollection() unimplemented")

    def getDataSetTypes(self):
        """Return a list of the mappable data set types."""

        list = []
        for attr in dir(self):
            if attr.startswith("map_"):
                list += attr[5:]
        return list

    def map(self, datasetType, dataId):
        """Map a data id using the mapping method for its data set type."""

        func = getattr(self, 'map_' + datasetType)
        return func(dataId)

    def standardize(self, datasetType, item):
        """Standardize an object using the standardization method for its data
        set type, if it exists."""

        try:
            func = getattr(self, 'std_' + datasetType)
            return func(item)
        except AttributeError:
            return item
