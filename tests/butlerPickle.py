#!/usr/bin/env python

from lsst.daf.persistence import ButlerFactory, ButlerLocation, Mapper

class MinMapper(Mapper):
    def __init__(self):
        pass

    def map_x(self, id):
        return ButlerLocation(None, None, "PickleStorage", "foo.pickle", {})


bf = ButlerFactory(mapper=MinMapper())
butler = bf.create()
bbox = [[3, 4], [5, 6]]
butler.put(bbox, "x")

y = butler.get("x")
assert y == bbox
