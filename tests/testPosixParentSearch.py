#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
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


def setup_module(module):
    lsst.utils.tests.init()


class PosixParentSearch(unittest.TestCase):
    """A test case for parentSearch in PosixStorage."""

    testDir = os.path.relpath(os.path.join(getPackageDir('daf_persistence'), 'tests', 'PosixParentSearch'))

    def setUp(self):
        self.tearDown()
        os.makedirs(PosixParentSearch.testDir)

    def tearDown(self):
        if os.path.exists(PosixParentSearch.testDir):
            shutil.rmtree(PosixParentSearch.testDir)

    def testFilePath(self):
        """Test that a file can be found; when root is part of the path then root is returned with the path
        result. When root is not part of the path then root is not returned with the path result."""
        with open(os.path.join(PosixParentSearch.testDir, 'foo.txt'), 'w') as f:
            f.write('abc')
        storage = dafPersist.PosixStorage(uri=PosixParentSearch.testDir)
        foundName = storage.search(storage.root, 'foo.txt', searchParents=True)
        self.assertEqual(foundName, ['foo.txt'])

        searchFor = os.path.join(PosixParentSearch.testDir, 'foo.txt')
        foundName = storage.search(storage.root, searchFor, searchParents=True)
        self.assertEqual(foundName, [searchFor])

    def testFilePathWithHeaderExt(self):
        """Find a file with a search string that includes a FITS-style header extension."""
        with open(os.path.join(PosixParentSearch.testDir, 'foo.txt'), 'w') as f:
            f.write('abc')
        storage = dafPersist.PosixStorage(uri=PosixParentSearch.testDir)
        foundName = storage.search(storage.root, 'foo.txt[0]', searchParents=True)
        self.assertEqual(foundName, ['foo.txt[0]'])

        searchFor = os.path.join(PosixParentSearch.testDir, 'foo.txt[0]')
        foundName = storage.search(storage.root, searchFor, searchParents=True)
        self.assertEqual(foundName, [searchFor])

    def testFilePathInParent(self):
        """Find a file in a repo that is a grandchild of the repo that has the file"""
        parentDir = os.path.join(PosixParentSearch.testDir, 'a')
        childDir = os.path.join(PosixParentSearch.testDir, 'b')
        for d in (parentDir, childDir):
            os.makedirs(d)
        with open(os.path.join(parentDir, 'foo.txt'), 'w') as f:
            f.write('abc')
        os.symlink('../a', os.path.join(childDir, '_parent'))
        storage = dafPersist.PosixStorage(uri=childDir)

        foundName = storage.search(storage.root, 'foo.txt', searchParents=True)
        self.assertEqual(storage.getRoot(), childDir)
        self.assertEqual(foundName, ['_parent/foo.txt'])

        searchFor = os.path.join(childDir, 'foo.txt')
        foundName = storage.search(storage.root, searchFor, searchParents=True)
        self.assertEqual(storage.getRoot(), childDir)
        self.assertEqual(foundName, [os.path.join(childDir, '_parent/foo.txt')])

    def testFilePathIn2ndParentParent(self):
        """Find a file in a repo that is the parent of a parent of the root repo."""
        grandParentDir = os.path.join(PosixParentSearch.testDir, 'a')
        parentDir = os.path.join(PosixParentSearch.testDir, 'b')
        childDir = os.path.join(PosixParentSearch.testDir, 'c')
        for d in (grandParentDir, parentDir, childDir):
            os.makedirs(d)
        for name in ('foo.txt', 'bar.txt'):
            with open(os.path.join(grandParentDir, name), 'w') as f:
                f.write('abc')
        os.symlink('../a', os.path.join(parentDir, '_parent'))
        os.symlink('../b', os.path.join(childDir, '_parent'))
        storage = dafPersist.PosixStorage(uri=childDir)

        for name in ('foo.txt', 'bar.txt[0]'):
            foundName = storage.search(storage.root, name, searchParents=True)
            self.assertEqual(storage.getRoot(), childDir)
            self.assertEqual(foundName, [os.path.join('_parent/_parent/', name)])

        for name in ('foo.txt', 'bar.txt[0]'):
            searchFor = os.path.join(childDir, name)
            foundName = storage.search(storage.root, searchFor, searchParents=True)
            self.assertEqual(storage.getRoot(), childDir)
            self.assertEqual(foundName, [os.path.join(childDir, '_parent/_parent/', name)])

    def testDoSearchParentFlag(self):
        """Test that parent search can be told to follow _parent symlink (or not) when searching."""
        parentDir = os.path.join(PosixParentSearch.testDir, 'a')
        childDir = os.path.join(PosixParentSearch.testDir, 'b')
        for d in (parentDir, childDir):
            os.makedirs(d)
        with open(os.path.join(parentDir, 'foo.txt'), 'w') as f:
            f.write('abc')
        os.symlink('../a', os.path.join(childDir, '_parent'))
        storage = dafPersist.PosixStorage(uri=childDir)
        self.assertEquals(storage.search(storage.root, 'foo.txt', searchParents=True), ['_parent/foo.txt'])
        self.assertEquals(storage.search(storage.root, 'foo.txt', searchParents=False), None)

    def testNoResults(self):
        storage = dafPersist.PosixStorage(uri=PosixParentSearch.testDir)
        self.assertIsNone(storage.search(storage.root, 'fileThatDoesNotExist.txt', searchParents=True))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
