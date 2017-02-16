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

import shutil
import unittest

import os
import pickle
import lsst.daf.persistence as dafPersist
import lsst.utils.tests
from cameraMapper import CameraMapper

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class Registry(object):

    def __init__(self, dictList):
        self.dictList = dictList

    def query(self, datasetType, format, dataId):
        result = set()
        for d in self.dictList:
            where = True
            for k in dataId.keys():
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


class ImgMapper(CameraMapper):

    def __init__(self, root, **kwargs):
        CameraMapper.__init__(self, root, **kwargs)
        self.root = os.path.join(ROOT, root)
        self.registry = Registry([
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
        ])


class ButlerSubsetTestCase(unittest.TestCase):
    """A test case for the subset capability of the data butler."""

    # create some random alias typenames
    calexpTypeName = "@goBears"
    rawTypeName = "@veggies"

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(os.path.join(ROOT, 'butlerSubset')):
            shutil.rmtree(os.path.join(ROOT, 'butlerSubset'))
        if os.path.exists('repositoryCfg.yaml'):
            os.remove('repositoryCfg.yaml')

    @staticmethod
    def registerAliases(butler):
        butler.defineAlias(ButlerSubsetTestCase.calexpTypeName, "calexp")
        butler.defineAlias(ButlerSubsetTestCase.rawTypeName, "raw")
        return butler

    def testSingleIteration(self):
        butler = dafPersist.Butler(
            outputs={'mode': 'rw',
                     'root': os.path.join(ROOT, 'butlerSubset'),
                     'mapper': ImgMapper})

        ButlerSubsetTestCase.registerAliases(butler)

        inputList = ["calexp_v123456_R1,2_S2,1.pickle",
                     "calexp_v123456_R1,2_S2,2.pickle",
                     "calexp_v654321_R1,3_S1,1.pickle",
                     "calexp_v654321_R1,3_S1,2.pickle"]
        for fileName in inputList:
            with open(os.path.join(ROOT, 'butlerSubset', fileName), "wb") as f:
                pickle.dump(inputList, f)

        subset = butler.subset(self.calexpTypeName, skyTile=6)
        # all sensors overlapping that skyTile
        self.assertEqual(len(subset), 4)
        for iterator in subset:
            # calexp is by sensor, so get the data directly
            self.assertEqual(iterator.dataId["skyTile"], 6)
            if iterator.dataId["raft"] == "1,2":
                self.assertIn(iterator.dataId["sensor"], ["2,1", "2,2"])
                self.assertEqual(iterator.dataId["visit"], 123456)
            elif iterator.dataId["raft"] == "1,3":
                self.assertIn(iterator.dataId["sensor"], ["1,1", "1,2"])
                self.assertEqual(iterator.dataId["visit"], 654321)
            else:
                self.assertIn(iterator.dataId["raft"], ["1,2", "1,3"])
            image = iterator.get(self.calexpTypeName, immediate=False)  # succeeds since deferred
            self.assertEqual(type(image), dafPersist.readProxy.ReadProxy)
            image = iterator.get(self.calexpTypeName, immediate=True)  # real test
            self.assertEqual(type(image), list)
            self.assertEqual(image, inputList)

        for fileName in inputList:
            os.unlink(os.path.join(ROOT, 'butlerSubset', fileName))

    def testNonexistentValue(self):
        butler = dafPersist.Butler(
            outputs={'mode': 'rw',
                     'root': os.path.join(ROOT, 'butlerSubset'),
                     'mapper': ImgMapper})
        ButlerSubsetTestCase.registerAliases(butler)
        subset = butler.subset(self.calexpTypeName, skyTile=2349023905239)
        self.assertEqual(len(subset), 0)

    def testInvalidValue(self):
        butler = dafPersist.Butler(
            outputs={'mode': 'rw',
                     'root': os.path.join(ROOT, 'butlerSubset'),
                     'mapper': ImgMapper})
        ButlerSubsetTestCase.registerAliases(butler)
        subset = butler.subset(self.calexpTypeName, skyTile="foo")
        self.assertEqual(len(subset), 0)
        subset = butler.subset(self.calexpTypeName, visit=123456, sensor="1;2")
        self.assertEqual(len(subset), 0)

    def testDoubleIteration(self):
        # create a bunch of files for testing
        inputList = ["flat_R1,1_S2,2_C0,0_E000.pickle",
                     "flat_R1,1_S2,2_C0,0_E001.pickle",
                     "flat_R1,1_S2,2_C0,1_E000.pickle",
                     "flat_R1,1_S2,2_C1,0_E001.pickle",
                     "flat_R1,1_S2,2_C1,1_E000.pickle",
                     "flat_R1,2_S2,1_C0,0_E001.pickle",
                     "flat_R1,2_S2,2_C0,0_E000.pickle"]
        for fileName in inputList:
            with open(fileName, "wb") as f:
                pickle.dump(inputList, f)

        butler = dafPersist.Butler(
            outputs={'mode': 'rw',
                     'root': '.',
                     'mapper': ImgMapper})

        ButlerSubsetTestCase.registerAliases(butler)

        subset = butler.subset(self.rawTypeName, visit=123456)
        # Note: default level = "sensor"
        n = len(subset)
        self.assertEqual(n, 3)
        for iterator in subset:
            # this is a sensor iterator, but raw data is by amplifier, so
            # iterate over amplifiers
            self.assertEqual(set(iterator.subLevels()), set(["snap", "amp"]))
            if iterator.dataId["raft"] == "1,1":
                self.assertEqual(len(iterator.subItems()), 5)
            else:
                self.assertEqual(iterator.dataId["raft"], "1,2")
                self.assertEqual(len(iterator.subItems()), 1)
            for ampIterator in iterator.subItems():  # default level = "amp"
                if iterator.dataId["raft"] == "1,1":
                    self.assertEqual(iterator.dataId["sensor"], "2,2")
                    self.assertIn(ampIterator.dataId["amp"], ["0,0", "0,1",
                                                              "1,0", "1,1"])
                    self.assertIn(ampIterator.dataId["snap"], [0, 1])
                else:
                    self.assertIn(iterator.dataId["sensor"], ["2,1", "2,2"])
                # equivalent to butler.get(self.rawTypeName, ampIterator)
                self.assertTrue(ampIterator.datasetExists("flat"))
                flat = ampIterator.get("flat")
                self.assertEqual(flat, inputList)
                flat = ampIterator.get("flat", immediate=True)
                self.assertEqual(flat, inputList)
                # ...perform ISR, assemble and calibrate the CCD, then persist
                calexp = flat
                iterator.put(calexp, self.calexpTypeName)

        for fileName in inputList:
            os.unlink(fileName)
        ref = butler.dataRef(self.rawTypeName,
                             visit=123456, raft="1,1", sensor="2,2", amp="0,1", snap=0)
        self.assertFalse(ref.datasetExists("flat"))

        for fileName in ["calexp_v123456_R1,1_S2,2.pickle",
                         "calexp_v123456_R1,2_S2,1.pickle",
                         "calexp_v123456_R1,2_S2,2.pickle"]:
            self.assertTrue(os.path.exists(fileName))
            os.unlink(fileName)

    def testCompleteDataRef(self):
        butler = dafPersist.Butler(
            outputs={'mode': 'rw',
                     'root': os.path.join(ROOT, 'butlerSubset'),
                     'mapper': ImgMapper})
        ButlerSubsetTestCase.registerAliases(butler)

        # Test by using junk data
        tuples = butler.queryMetadata(self.rawTypeName, "amp",
                                      dict(visit=314159, raft="ab", sensor="cd", amp="ef", snap=9))
        self.assertEqual(len(tuples), 0)
        subset = butler.subset(self.rawTypeName, "amp", visit=314159, raft="ab", sensor="cd")
        self.assertEqual(len(subset), 0)

        # But allow user to fully specify dataId, even if it doesn't exist
        subset = butler.subset(self.rawTypeName, "sensor", visit=314159, raft="ab", sensor="cd")
        self.assertEqual(len(subset), 1)
        ref = butler.dataRef(self.rawTypeName, visit=314159, raft="ab", sensor="cd", amp="ef", snap=9)
        self.assertFalse(ref.datasetExists(self.rawTypeName))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
