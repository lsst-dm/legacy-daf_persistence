#!/usr/bin/env python

class Mapper(object):
    def __init__(self):
        pass

    def getDataSetTypes(self):
        list = []
        for attr in dir(self):
            if attr.startswith("map_"):
                list += attr[5:]
        return list

    def map(self, dataSetType, dataId):
        func = getattr(self, 'map_' + dataSetType)
        return func(dataId)

    def standardize(self, dataSetType, item):
        func = getattr(self, 'std_' + dataSetType)
        return func(item)
