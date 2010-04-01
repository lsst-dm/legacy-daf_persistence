#!/usr/bin/env python

"""This module defines the ButlerLocation class."""

import lsst.daf.base as dafBase

class ButlerLocation(object):
    """ButlerLocation is a struct-like class that holds information needed to
    persist and retrieve an object using the LSST Persistence Framework.
    
    Mappers should create and return ButlerLocations from their
    map_{dataSetType} methods."""

    def __init__(self, pythonType, cppType, storageInfoList, dataId):
        self.pythonType = pythonType
        self.cppType = cppType
        self.storageInfoList = storageInfoList
        self.additionalData = dafBase.PropertySet()
        for k, v in dataId:
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

    def getStorageInfo(self):
        return self.storageInfoList

    def getAdditionalData(self):
        return additionalData
