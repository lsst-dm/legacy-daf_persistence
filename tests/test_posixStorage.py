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
import lsst.utils.tests
import shutil
import tempfile

try:
    FileType = file
except NameError:
    from io import IOBase
    FileType = IOBase


ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class GetParentFromSymlink(unittest.TestCase):
    """A test case for getting the relative path to parent from a symlink in PosixStorage."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='GetParentFromSymlink-')
        self.parentFolderPath = os.path.join(self.testDir, "theParent")
        self.childFolderPath = os.path.join(self.testDir, "theChild")
        self.parentlessFolderPath = os.path.join(self.testDir, "parentlessRepo")
        for p in (self.parentFolderPath, self.childFolderPath, self.parentlessFolderPath):
            os.makedirs(p)
        relpath = os.path.relpath(self.parentFolderPath, self.childFolderPath)
        os.symlink(relpath, os.path.join(self.childFolderPath, '_parent'))

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testV1RepoWithParen(self):
        parentPath = dp.PosixStorage.getParentSymlinkPath(self.childFolderPath)
        self.assertEqual(parentPath, os.path.relpath(self.parentFolderPath, self.childFolderPath))

    def testV1RepoWithoutParent(self):
        parentPath = dp.PosixStorage.getParentSymlinkPath(self.parentlessFolderPath)
        self.assertEqual(parentPath, None)


class TestRelativePath(unittest.TestCase):
    """A test case for the PosixStorage.relativePath function."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='TestRelativePath-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testRelativePath(self):
        """Test that a relative path returns the correct relative path for
        1. relative inputs, 2. absolute inputs."""
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

    @unittest.expectedFailure
    def testRelativeSymlinkPath(self):
        """Test that a relative path returns the correct relative path for
        1. relative inputs, 2. absolute inputs."""
        repoDir = os.path.join(self.testDir, 'repo')
        symlinkDir = os.path.join(self.testDir, 'symlink')

        abspathA = os.path.join(repoDir, 'a')
        abspathB = os.path.join(repoDir, 'b')
        absSymlinkPathA = os.path.join(symlinkDir, 'a')
        absSymlinkPathB = os.path.join(symlinkDir, 'b')

        os.makedirs(repoDir)
        os.makedirs(symlinkDir)
        os.makedirs(abspathA)
        os.makedirs(abspathB)

        os.symlink(abspathA, absSymlinkPathA)
        os.symlink(abspathB, absSymlinkPathB)
        # 1.
        relpathA = os.path.relpath(abspathA)
        relpathB = os.path.relpath(abspathB)
        relpathAtoB = dp.PosixStorage.relativePath(relpathA, relpathB)
        self.assertEqual('../b', relpathAtoB)
        # 2.
        relpathAtoB = dp.PosixStorage.relativePath(abspathA, abspathB)
        self.assertEqual('../b', relpathAtoB)

        relpathSymlinkAtoB = dp.PosixStorage.relativePath(absSymlinkPathA, absSymlinkPathB)
        self.assertEqual('../b', relpathSymlinkAtoB)


class TestAbsolutePath(unittest.TestCase):
    """A test case for the PosixStorage.absolutePath function."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='TestAbsolutePath-')

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


class TestGetLocalFile(unittest.TestCase):
    """A test case for the PosixStorage.getLocalFile function."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='TestGetLocalFile-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testAbsolutePath(self):
        """Tests that GetLocalFile returns a file when it exists and returns
        None when it does not exist."""
        storage = dp.PosixStorage(self.testDir, create=True)
        self.assertIsNone(storage.getLocalFile('foo.txt'))
        with open(os.path.join(self.testDir, 'foo.txt'), 'w') as f:
            f.write('foobarbaz')
        del f
        f = storage.getLocalFile('foo.txt')
        self.assertIsInstance(f, FileType)
        self.assertEqual(f.read(), 'foobarbaz')
        f.close()


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
