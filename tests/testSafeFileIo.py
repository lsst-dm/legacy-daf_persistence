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
import multiprocessing
import shutil
import time

import lsst.daf.persistence as dp
from lsst.utils import getPackageDir
import lsst.utils.tests
from lsst.log import Log

packageDir = os.path.join(getPackageDir('daf_persistence'))


def setup_module(module):
    lsst.utils.tests.init()


def readFile(filename, readQueue):
    readQueue.put("waiting")
    readQueue.get()
    with dp.safeFileIo.SafeLockedFileForRead(filename) as f:
        readQueue.put(f.read())


class TestFileLocking(unittest.TestCase):
    """A test case for safeFileIo file read and write locking"""

    testDir = os.path.join(packageDir, 'tests', 'TestFileLocking')

    def setUp(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

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


class TestOneThousandWriters(unittest.TestCase):
    """Test for efficient file updating with shared & exclusive locks by serializing a RepositoryCfg to a
    location 1000 times. When this was tested on a 2.8 GHz Intel Core i7 macbook pro it took about 1.3 seconds
    to run. When the squash performance monitoring framework is done, this test could be monitored in that
    system."""

    testDir = os.path.join(packageDir, 'tests', 'TestOneThousandWriters')

    def setUp(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    @staticmethod
    def writeCfg(cfg, go):
        while go is False:
            pass
        dp.PosixStorage.putRepositoryCfg(cfg)

    def testWriteCfg(self):
        # The number of writers to use can result in too many open files
        # We calculate this as the 80% of the maximum allowed number for this
        # process, or 1000, whichever is smaller.
        numWriters = 1000
        try:
            import resource
            limit = resource.getrlimit(resource.RLIMIT_NOFILE)
            allowedOpen = int(limit[0] * 0.8)
            if allowedOpen < numWriters:
                numWriters = allowedOpen
        except:
            # Use the default number if we had trouble obtaining resources
            pass
        startTime = time.time()
        go = multiprocessing.Value('b', False)
        cfg = dp.RepositoryCfg(root=os.path.join(TestOneThousandWriters.testDir), mapper='bar', mapperArgs={},
                               parents=None, policy=None)
        procs = [multiprocessing.Process(target=TestOneThousandWriters.writeCfg, args=(cfg, go))
                 for x in range(numWriters)]
        for proc in procs:
            proc.start()
        go = True
        for proc in procs:
            proc.join()
        endTime = time.time()
        log = Log.getLogger("daf.persistence")
        log.trace("TestOneThousandWriters took {} seconds.".format(endTime-startTime))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
