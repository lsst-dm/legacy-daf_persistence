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


import unittest
import lsst.utils.tests as utilsTests

import re
import lsst.daf.persistence as dafPersist


class ImgMapper(dafPersist.Mapper):
    def __init__(self):
        self.templates = dict(
            raw="raw_v%(visit)d_R%(raft)s_S%(sensor)s_C%(amp)s_E%(snap)03d.pickle",
            flat="flat_R%(raft)s_S%(sensor)s_C%(amp)s_E%(snap)03d.pickle",
            calexp="calexp_v%(visit)d_R%(raft)s_S%(sensor)s.pickle")
        self.levels = dict(
                skyTile=["visit", "raft", "sensor"],
                visit=["snap", "raft", "sensor", "amp"],
                raft=["snap", "sensor", "amp"],
                sensor=["snap", "amp"],
                amp=[])

        self.registry = [
                dict(visit=123456, raft="1,1", sensor="2,2", amp="0,0",
                    snap=0, skyTile=5),
                dict(visit=123456, raft="1,1", sensor="2,2", amp="0,0",
                    snap=1, skyTile=5),
                dict(visit=123456, raft="1,1", sensor="2,2", amp="0,1",
                    snap=0, skyTile=5),
                dict(visit=123456, raft="1,1", sensor="2,2", amp="1,0",
                    snap=1, skyTile=5),
                dict(visit=123456, raft="1,1", sensor="2,2", amp="1,1",
                    snap=0, skyTile=5),

                dict(visit=123456, raft="1,2", sensor="2,1", amp="0,0",
                    snap=1, skyTile=6),
                dict(visit=123456, raft="1,2", sensor="2,2", amp="0,0",
                    snap=0, skyTile=6),

                dict(visit=654321, raft="1,3", sensor="1,1", amp="0,0",
                    snap=1, skyTile=6),
                dict(visit=654321, raft="1,3", sensor="1,2", amp="0,0",
                    snap=0, skyTile=6)
                ]


    def getKeys(self, datasetType, level):
        keySet = set()
        if datasetType is None:
            for t in self.templates.iterkeys():
                keySet.update(self.getKeys(t))
        else:
            keySet.update(re.findall(r'\%\((\w+)\)',
                self.templates[datasetType]))
        if level is not None:
            keySet -= set(self.levels[level])
        return keySet

    def defaultLevel(self):
        return "sensor"

    def query_raw(self, key, format, dataId):
        result = set()
        for d in self.registry:
            where = True
            for k in dataId.iterkeys():
                if k not in d:
                    raise RuntimeError("%s not in %s" % (k, repr(d)))
                if d[k] != dataId[k]:
                    where = False
                    break
            if where:
                values = []
                for k in format:
                    values.append(d[k])
                result.add(tuple(values))
        return result

    def query_calexp(self, key, format, dataId):
        return self.query_raw(key, format, dataId)

    def map_raw(self, dataId):
        path = self.templates["raw"] % dataId
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {})

    def map_flat(self, dataId):
        path = self.templates["flat"] % dataId
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {})

    def map_calexp(self, dataId):
        path = self.templates["calexp"] % dataId
        return dafPersist.ButlerLocation(None, None, "PickleStorage", path, {})


class ButlerSubsetTestCase(unittest.TestCase):
    """A test case for the subset capability of the data butler."""

    def testSingleIteration(self):
        bf = dafPersist.ButlerFactory(mapper=ImgMapper())
        butler = bf.create()

        subset = butler.subset("calexp", skyTile=6)
        # all sensors overlapping that skyTile
        self.assertEqual(len(subset), 4)
        for iterator in subset:
            # calexp is by sensor, so get the data directly
            self.assertEqual(iterator.dataId["skyTile"], 6)
            if iterator.dataId["raft"] == "1,2":
                self.assert_(iterator.dataId["sensor"] in ["2,1", "2,2"])
                self.assertEqual(iterator.dataId["visit"], 123456)
            elif iterator.dataId["raft"] == "1,3":
                self.assert_(iterator.dataId["sensor"] in ["1,1", "1,2"])
                self.assertEqual(iterator.dataId["visit"], 654321)
            else:
                self.assert_(iterator.dataId["raft"] in ["1,2", "1,3"])
            image = iterator.get("calexp")

    def testDoubleIteration(self):
        bf = dafPersist.ButlerFactory(mapper=ImgMapper())
        butler = bf.create()

        subset = butler.subset("raw", visit=123456)
        # Note: default level = "sensor"
        n = len(subset)
        self.assertEqual(n, 3)
        for iterator in subset:
            # this is a sensor iterator, but raw data is by amplifier, so
            # iterate over amplifiers
            for ampIterator in iterator.subItems(): # default level = "amp"
                ampImage = ampIterator.get("raw")
                # equivalent to butler.get("raw", ampIterator)
                flat = ampIterator.get("flat")
                # ...perform ISR, assemble and calibrate the CCD, then persist
                calexp = flat
                iterator.put(calexp, "calexp")

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(ButlerSubsetTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
