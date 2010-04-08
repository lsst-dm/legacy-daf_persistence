#!/usr/bin/env python

"""This module defines the Mapper base class."""

class Mapper(object):
    """Mapper is a base class for all mappers.

    Subclasses may define the following methods:
    
    map_{datasetType}(self, dataId)
        Map a dataset id for the given dataset type into a ButlerLocation.

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

    getDataSetTypes(self)

    map(self, datasetType, dataId)

    queryMetadata(self, datasetType, key, format, dataId)

    standardize(self, datasetType, item, dataId)
    """

    def __init__(self):
        pass

    def keys(self):
        raise NotImplementedError("keys() unimplemented")

    def queryMetadata(self, datasetType, key, format, dataId):
        """Return possible values for keys given a partial data id."""

        func = getattr(self, 'coll_' + datasetType)
        return func(key, format, dataId)

    def getDataSetTypes(self):
        """Return a list of the mappable dataset types."""

        list = []
        for attr in dir(self):
            if attr.startswith("map_"):
                list += attr[5:]
        return list

    def map(self, datasetType, dataId):
        """Map a data id using the mapping method for its dataset type."""

        func = getattr(self, 'map_' + datasetType)
        return func(dataId)

    def canStandardize(self, datasetType):
        """Return true if this mapper can standardize an object of the given
        dataset type."""

        return hasattr(self, 'std_' + datasetType)

    def standardize(self, datasetType, item, dataId):
        """Standardize an object using the standardization method for its data
        set type, if it exists."""

        if hasattr(self, 'std_' + datasetType):
            func = getattr(self, 'std_' + datasetType)
            return func(item, dataId)
        return item
