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
import lsst.utils.tests
import os

import lsst.daf.persistence as dafPersist


class MinMapper(dafPersist.Mapper):

    def __init__(self):
        pass

    def map_x(self, dataId, write):
        path = "foo%(ccd)d.pickle" % dataId
        if not write:
            path = "parent/" + path
        return dafPersist.ButlerLocation(
            "lsst.afw.image.BBox", "lsst::afw::image::BBox", "PickleStorage",
            path, {}, self, dafPersist.Storage.makeFromURI(os.getcwd()))

    def map_badSourceHist(self, dataId, write):
        path = "badSourceHist%(ccd)d.pickle" % dataId
        return dafPersist.ButlerLocation(
            "lsst.afw.image.BBox", "lsst::afw::image::BBox", "PickleStorage",
            path, {}, self, dafPersist.Storage.makeFromURI(os.getcwd()))

    def query_x(self, format, dataId):
        return [1, 2, 3]

    def std_x(self, item, dataId):
        return float(item)


class MapperTestCase(unittest.TestCase):
    """A test case for the mapper used by the data butler."""

    def setUp(self):
        self.mapper = MinMapper()

    def testGetDatasetTypes(self):
        self.assertEqual(set(self.mapper.getDatasetTypes()),
                         set(["x", "badSourceHist"]))

    def testMap(self):
        loc = self.mapper.map("x", {"ccd": 27})
        self.assertEqual(loc.getPythonType(), "lsst.afw.image.BBox")
        self.assertEqual(loc.getCppType(), "lsst::afw::image::BBox")
        self.assertEqual(loc.getStorageName(), "PickleStorage")
        self.assertEqual(loc.getLocations(), ["parent/foo27.pickle"])
        self.assertEqual(loc.getAdditionalData().toString(), "")

    def testMapWrite(self):
        loc = self.mapper.map("x", {"ccd": 27}, write=True)
        self.assertEqual(loc.getPythonType(), "lsst.afw.image.BBox")
        self.assertEqual(loc.getCppType(), "lsst::afw::image::BBox")
        self.assertEqual(loc.getStorageName(), "PickleStorage")
        self.assertEqual(loc.getLocations(), ["foo27.pickle"])
        self.assertEqual(loc.getAdditionalData().toString(), "")

    def testQueryMetadata(self):
        self.assertEqual(self.mapper.queryMetadata("x", None, None),
                         [1, 2, 3])

    def testStandardize(self):
        self.assertEqual(self.mapper.canStandardize("x"), True)
        self.assertEqual(self.mapper.canStandardize("badSourceHist"), False)
        self.assertEqual(self.mapper.canStandardize("notPresent"), False)
        result = self.mapper.standardize("x", 3, None)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.0)
        result = self.mapper.standardize("x", 3.14, None)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.14)
        result = self.mapper.standardize("x", "3.14", None)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.14)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
