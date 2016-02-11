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

import lsst.daf.persistence as dafPersist
import lsst.daf.base as dafBase

class MinMapper(dafPersist.Mapper):
    def __init__(self):
        pass

    def map_x(self, dataId, write):
        path = "foo%(ccd)d.yaml" % dataId
        return dafPersist.ButlerLocation(None, None, "YamlStorage", path, {})

class ButlerYamlTestCase(unittest.TestCase):
    """A test case for the data butler using YamlStorage"""

    localTypeName = "@myPreferredType"
    localTypeNameIsAliasOf = "x"

    def setUp(self):
        bf = dafPersist.ButlerFactory(mapper=MinMapper())
        self.butler = bf.create()
        self.butler.defineAlias(self.localTypeName, self.localTypeNameIsAliasOf)

    def tearDown(self):
        del self.butler

    def checkIO(self, butler, pset, ccd):
        butler.put(pset, self.localTypeName, ccd=ccd)
        y = butler.get(self.localTypeName, ccd=ccd, immediate=True)
        self.assertEqual(pset.names(), y.names())
        for i in pset.names():
            self.assertEqual(pset.get(i), y.get(i))

    def testIO(self):
        pset = dafBase.PropertySet()
        pset.set("foo", 3)
        pset.set("bar", dafBase.DateTime.now())
        self.checkIO(self.butler, pset, 3)

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(ButlerYamlTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
