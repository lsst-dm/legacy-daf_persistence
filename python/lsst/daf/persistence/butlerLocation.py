#!/usr/bin/env python

import lsst.daf.base as dafBase

class ButlerLocation(object):
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
