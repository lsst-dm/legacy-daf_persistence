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
import shutil
import tempfile
import lsst.utils.tests
import os

import lsst.daf.persistence as dafPersist
import lsst.daf.base as dafBase


class MinMapper(dafPersist.Mapper):

    def __init__(self, root, parentRegistry, repositoryCfg):
        pass

    def map_x(self, dataId, write):
        path = "foo%(ccd)d.yaml" % dataId
        return dafPersist.ButlerLocation(
            "lsst.daf.base.PropertySet", "PropertySet", "YamlStorage",
            [path], dataId, self, dafPersist.Storage.makeFromURI(os.getcwd()))


class ButlerYamlTestCase(unittest.TestCase):
    """A test case for the data butler using YamlStorage"""

    localTypeName = "@myPreferredType"
    localTypeNameIsAliasOf = "x"

    def setUp(self):
        self.tempRoot = tempfile.mkdtemp()
        self.butler = dafPersist.Butler(root=self.tempRoot, mapper=MinMapper)
        self.butler.defineAlias(self.localTypeName, self.localTypeNameIsAliasOf)

    def tearDown(self):
        del self.butler
        shutil.rmtree(self.tempRoot, ignore_errors=True)

    def testPropertySet(self):
        pset = dafBase.PropertySet()
        pset.set("foo", 3)
        pset.set("bar", dafBase.DateTime.now())
        pset.set("baz", ['a', 'b', 'c'])
        pset.set("e", 2.71828)
        pset.set("f.a", [1, 2, 3])
        pset.set("f.b", 201805241715)
        pset.setBool("bool0", False)
        pset.setBool("bool1", True)
        pset.setShort("short1", 32767)
        pset.setShort("short2", -32768)
        pset.setFloat("float1", 9.8765)
        pset.setFloat("float2", -1.234)
        self.butler.put(pset, self.localTypeName, ccd=3)
        y = self.butler.get(self.localTypeName, ccd=3, immediate=True)
        self.assertEqual(set(pset.names(False)), set(y.names(False)))
        for i in pset.paramNames(False):
            self.assertEqual(pset.get(i), y.get(i))
            self.assertEqual(pset.typeOf(i), y.typeOf(i))

    def testPropertyList(self):
        plist = dafBase.PropertyList()
        plist.set("foo", 3)
        plist.set("bar", dafBase.DateTime.now())
        plist.set("baz", ['a', 'b', 'c'])
        plist.set("e", 2.71828)
        plist.set("f.a", [1, 2, 3])
        plist.set("f.b", 201805241715)
        plist.setBool("bool0", False)
        plist.setBool("bool1", True)
        plist.setShort("short1", 32767)
        plist.setShort("short2", -32768)
        plist.setFloat("float1", 9.8765)
        plist.setFloat("float2", -1.234)
        self.butler.put(plist, self.localTypeName, ccd=3)
        y = self.butler.get(self.localTypeName, ccd=3, immediate=True)
        self.assertEqual(plist.names(False), y.names(False))
        for i in plist.names(False):
            self.assertEqual(plist.get(i), y.get(i))
            self.assertEqual(plist.typeOf(i), y.typeOf(i))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
