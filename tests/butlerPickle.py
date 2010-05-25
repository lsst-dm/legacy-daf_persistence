#!/usr/bin/env python

import unittest
import lsst.utils.tests as utilsTests

import lsst.daf.persistence as dafPersist

class MinMapper(dafPersist.Mapper):
    def __init__(self):
        pass

    def map_x(self, dataId):
        path = "foo%(ccd)d.pickle" % dataId
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {})

class ButlerPickleTestCase(unittest.TestCase):
    """A test case for the data butler using PickleStorage"""

    def testPickle(self):
        bf = dafPersist.ButlerFactory(mapper=MinMapper())
        butler = bf.create()
        bbox = [[3, 4], [5, 6]]
        butler.put(bbox, "x", ccd=3)

        y = butler.get("x", ccd=3)
        self.assertEqual(bbox, y)

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(ButlerPickleTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
