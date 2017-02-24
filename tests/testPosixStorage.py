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
import lsst.daf.persistence as dp
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
        parentPath = dp.PosixStorage.getParentSymlinkPath(self.childFolderPath)
        self.assertEqual(parentPath, os.path.relpath(self.parentFolderPath, self.childFolderPath))

    def testV1RepoWithoutParent(self):
        parentPath = dp.PosixStorage.getParentSymlinkPath(self.parentlessFolderPath)
        self.assertEqual(parentPath, None)


class TestRelativePath(unittest.TestCase):
    """A test case for the PosixStorage.relativePath function."""

    testDir = os.path.join(packageDir, 'tests', 'TestRelativePath')

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testRelativePath(self):
        """Test that a relative path returns the correct relative path for
        1. relative inputs, 2. absolute inputs, 3. URI inputs (starts with
        file:///...)"""
        abspathA = os.path.join(self.testDir, 'a')
        abspathB = os.path.join(self.testDir, 'b')
        os.makedirs(abspathA)
        os.makedirs(abspathB)
        # 1.
        relpathA = os.path.relpath(abspathA)
        relpathB = os.path.relpath(abspathB)
        relpathAtoB = dp.PosixStorage.relativePath(relpathA, relpathB)
        self.assertEqual('../b', relpathAtoB)
        # 2.
        relpathAtoB = dp.PosixStorage.relativePath(abspathA, abspathB)
        self.assertEqual('../b', relpathAtoB)
        # 3.
        for prefix in ("file://", "file:///"):
            uriA = prefix + abspathA
            uriB = prefix + abspathB
            relpathAtoB = dp.PosixStorage.relativePath(uriA, uriB)
            self.assertEqual('../b', relpathAtoB)


class TestAbsolutePath(unittest.TestCase):
    """A test case for the PosixStorage.absolutePath function."""

    testDir = os.path.join(packageDir, 'tests', 'TestAbsolutePath')

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testAbsolutePath(self):
        """Tests that given a path and a relative path, the correct aboslute
        path to the relative path is returned."""
        abspathA = os.path.join(self.testDir, 'a')
        abspathB = os.path.join(self.testDir, 'b')
        os.makedirs(abspathA)
        os.makedirs(abspathB)
        relpathA = os.path.relpath(abspathA)
        self.assertEqual(abspathB,
                         dp.PosixStorage.absolutePath(abspathA, '../b'))
        self.assertEqual(abspathB,
                         dp.PosixStorage.absolutePath(relpathA, '../b'))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
