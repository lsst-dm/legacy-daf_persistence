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

import multiprocessing
import os
import shutil
import stat
import time
import unittest
import tempfile

import lsst.daf.persistence as dp
import lsst.utils.tests
from lsst.log import Log


# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class WriteOnceCompareSameTest(unittest.TestCase):

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='WriteOnceCompareSameTest-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testCompareSame(self):

        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(self.testDir, 'test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(self.testDir, 'test.txt')))
        self.assertEqual(len(os.listdir(self.testDir)), 1)

        # write the same file, verify the dir & file stay the same
        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(self.testDir, 'test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(self.testDir, 'test.txt')))
        self.assertEqual(len(os.listdir(self.testDir)), 1)

    def testCompareDifferent(self):
        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(self.testDir, 'test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(self.testDir, 'test.txt')))
        self.assertEqual(len(os.listdir(self.testDir)), 1)

        # write the same file, verify the dir & file stay the same
        def writeNonMatchingFile():
            with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(self.testDir, 'test.txt')) as f:
                f.write('boo\n')
                f.write('fop\n')
        self.assertRaises(RuntimeError, writeNonMatchingFile)

    def testPermissions(self):
        """Check that the file is created with the current umask."""
        # The only way to get the umask is to set it.
        umask = os.umask(0)
        os.umask(umask)

        fileName = os.path.join(self.testDir, 'test.txt')
        with dp.safeFileIo.FileForWriteOnceCompareSame(fileName) as f:
            f.write('bar\n')
            f.write('baz\n')

        filePerms = stat.S_IMODE(os.lstat(fileName).st_mode)
        self.assertEqual(~umask & 0o666, filePerms)


def readFile(filename, readQueue):
    readQueue.put("waiting")
    readQueue.get()
    with dp.safeFileIo.SafeLockedFileForRead(filename) as f:
        readQueue.put(f.read())


class TestFileLocking(unittest.TestCase):
    """A test case for safeFileIo file read and write locking"""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='TestFileLocking-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testWriteLock(self):
        """Test SafeLockedFileForWrite by
        1. open a file for write
        2. spawn a second process that tries to read the file but should be blocked by the file lock
        3. then write the file it and closing it (in the first process)
        4. the second process should then be unblocked
        5. read the file in the second process and return the result to the first process
        6. compare what was written and read
        """
        readQueue = multiprocessing.Queue()
        fileName = os.path.join(self.testDir, "testfile.txt")
        proc = multiprocessing.Process(target=readFile, args=(fileName, readQueue))
        testStr = "foobarbaz"
        proc.start()
        self.assertEqual(readQueue.get(), "waiting")
        with dp.safeFileIo.SafeLockedFileForWrite(fileName) as f:
            readQueue.put("go")
            time.sleep(1)
            f.write(testStr)
        self.assertEqual(readQueue.get(), testStr)
        proc.join()

    def testNoChange(self):
        """Test that if a file is opened and not changed that the file does not get changed"""
        fileName = os.path.join(self.testDir, "testfile.txt")
        # create the file with some contents
        with dp.safeFileIo.SafeLockedFileForWrite(fileName) as f:
            f.write("some test string")
        # open the file but do not change it
        with dp.safeFileIo.SafeLockedFileForWrite(fileName) as f:
            pass
        # open the file for read and test that it still contains the original test contents
        with dp.safeFileIo.SafeLockedFileForRead(fileName) as f:
            self.assertEqual(f.read(), "some test string")


class TestMultipleWriters(unittest.TestCase):
    """Test for efficient file updating with shared & exclusive locks by
    serializing a RepositoryCfg to a location several times in parallel."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='TestMultipleWriters-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    @staticmethod
    def writeCfg(cfg, go):
        """Write a configuration file after waiting on a condition variable."""
        while go is False:
            pass
        dp.PosixStorage.putRepositoryCfg(cfg)

    def testWriteCfg(self):
        """Test parallel writes to a configuration file.

        multiprocessing is used to spawn several writer function executions,
        all of which wait to be released by the condition variable "go".

        There are no asserts here, so success is measured solely by not
        failing with an exception, but the time it took to do the writes can
        be logged as a potential performance metric.
        """
        numWriters = 3
        startTime = time.time()
        go = multiprocessing.Value('b', False)
        cfg = dp.RepositoryCfg(root=os.path.join(self.testDir), mapper='bar', mapperArgs={},
                               parents=None, policy=None)
        procs = [multiprocessing.Process(target=TestMultipleWriters.writeCfg, args=(cfg, go))
                 for x in range(numWriters)]
        for proc in procs:
            proc.start()
        go = True
        for proc in procs:
            proc.join()
        endTime = time.time()
        log = Log.getLogger("daf.persistence")
        log.trace("TestMultipleWriters took {} seconds.".format(endTime-startTime))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
