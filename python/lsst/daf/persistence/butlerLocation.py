#!/usr/bin/env python

"""This module defines the ButlerLocation class."""

import lsst.daf.base as dafBase

class ButlerLocation(object):
    """ButlerLocation is a struct-like class that holds information needed to
    persist and retrieve an object using the LSST Persistence Framework.
    
    Mappers should create and return ButlerLocations from their
    map_{datasetType} methods."""

    def __init__(self, pythonType, cppType, storageName, locationList, dataId):
        self.pythonType = pythonType
        self.cppType = cppType
        self.storageName = storageName
        self.locationList = locationList
        self.additionalData = dafBase.PropertySet()
        for k, v in dataId.iteritems():
            self.additionalData.set(k, v)

    def __str__(self):
        s = "%s at" % (self.pythonType,)
        for storageName, locString in self.storageInfoList:
            s += " %s(%s)" % (storageName, locString)
        return s

    def getPythonType(self):
        return self.pythonType

    def getCppType(self):
        return self.cppType

    def getStorageName(self):
        return self.storageName

    def getLocations(self):
        return self.locationList

    def getAdditionalData(self):
        return self.additionalData
