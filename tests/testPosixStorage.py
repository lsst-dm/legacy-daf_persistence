#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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
import unittest
import lsst.daf.persistence as dafPersist
from lsst.utils import getPackageDir
import lsst.utils.tests
import shutil

packageDir = os.path.join(getPackageDir('daf_persistence'))

def setup_module(module):
    lsst.utils.tests.init()

class GetParentFromSymlink(unittest.TestCase):
    """A test case for getting the relative path to parent from a symlink in PosixStorage."""

    testDir = os.path.join(packageDir, 'tests', 'GetParentFromSymlink')

    def setUp(self):
        self.tearDown()
        self.parentFolderPath = os.path.join(GetParentFromSymlink.testDir, "theParent")
        self.childFolderPath = os.path.join(GetParentFromSymlink.testDir, "theChild")
        self.parentlessFolderPath = os.path.join(GetParentFromSymlink.testDir, "parentlessRepo")
        for p in (self.parentFolderPath, self.childFolderPath, self.parentlessFolderPath):
            os.makedirs(p)
        relpath = os.path.relpath(self.parentFolderPath, self.childFolderPath)
        os.symlink(relpath, os.path.join(self.childFolderPath, '_parent'))

    def tearDown(self):
        if os.path.exists(GetParentFromSymlink.testDir):
            shutil.rmtree(GetParentFromSymlink.testDir)

    def testV1RepoWithParen(self):
        parentPath = dafPersist.PosixStorage.getParentSymlinkPath(self.childFolderPath)
        self.assertEqual(parentPath, os.path.relpath(self.parentFolderPath, self.childFolderPath))

    def testV1RepoWithoutParent(self):
        parentPath = dafPersist.PosixStorage.getParentSymlinkPath(self.parentlessFolderPath)
        self.assertEqual(parentPath, None)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
