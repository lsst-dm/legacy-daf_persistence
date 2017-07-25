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


import os
import pickle
import shutil
import unittest
import lsst.utils.tests

import lsst.daf.persistence as dafPersist
import pickleMapper

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


class MapperImportTestCase(unittest.TestCase):
    """A test case for the data butler finding a Mapper in a root"""

    def setUp(self):
        if os.path.exists(os.path.join(ROOT, 'root/out')):
            shutil.rmtree(os.path.join(ROOT, 'root/out'))

        self.butler = dafPersist.Butler(os.path.join(ROOT, "root"), outPath="out")

    def tearDown(self):
        del self.butler
        if os.path.exists(os.path.join(ROOT, 'root/out')):
            shutil.rmtree(os.path.join(ROOT, 'root/out'))

    def testMapperClass(self):
        repository = self.butler._repos.outputs()[0].repo
        self.assertTrue(isinstance(repository._mapper, pickleMapper.PickleMapper))

    def checkIO(self, butler, bbox, ccd):
        butler.put(bbox, "x", ccd=ccd)
        y = butler.get("x", ccd=ccd, immediate=True)
        self.assertEqual(bbox, y)
        self.assert_(os.path.exists(
            os.path.join(ROOT, "root", "out", "foo%d.pickle" % ccd)))

    def testIO(self):
        bbox = [[3, 4], [5, 6]]
        self.checkIO(self.butler, bbox, 3)

    def testPickle(self):
        butler = pickle.loads(pickle.dumps(self.butler))
        bbox = [[1, 2], [8, 9]]
        self.checkIO(butler, bbox, 1)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
