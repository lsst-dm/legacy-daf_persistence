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


import pickle
import unittest
import lsst.utils.tests
import os

import lsst.daf.persistence as dafPersist


class MinMapper(dafPersist.Mapper):

    def __init__(self):
        pass

    def map_x(self, dataId, write):
        path = "foo%(ccd)d.pickle" % dataId
        return dafPersist.ButlerLocation(
            None, None, "PickleStorage", path, {},
            self, dafPersist.Storage.makeFromURI(os.getcwd()))


class ButlerPickleTestCase(unittest.TestCase):
    """A test case for the data butler using PickleStorage"""

    localTypeName = "@myPreferredType"
    localTypeNameIsAliasOf = "x"

    def setUp(self):
        self.butler = dafPersist.Butler(root='.', mapper=MinMapper())
        self.butler.defineAlias(self.localTypeName, self.localTypeNameIsAliasOf)

    def tearDown(self):
        del self.butler

    def checkIO(self, butler, bbox, ccd):
        butler.put(bbox, self.localTypeName, ccd=ccd)
        y = butler.get(self.localTypeName, ccd=ccd, immediate=True)
        self.assertEqual(bbox, y)

    def testIO(self):
        bbox = [[3, 4], [5, 6]]
        self.checkIO(self.butler, bbox, 3)

    def testPickle(self):
        pickledButler = pickle.dumps(self.butler)
        butler = pickle.loads(pickledButler)
        bbox = [[1, 2], [8, 9]]
        self.checkIO(butler, bbox, 1)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
