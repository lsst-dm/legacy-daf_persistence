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
import shutil
import unittest

import lsst.daf.persistence as dp
import lsst.utils.tests

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class WriteOnceCompareSameTest(unittest.TestCase):

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(os.path.join(ROOT, 'safeFileIo')):
            shutil.rmtree(os.path.join(ROOT, 'safeFileIo'))

    def testCompareSame(self):

        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(ROOT, 'safeFileIo/test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(ROOT, 'safeFileIo/test.txt')))
        self.assertEqual(len(os.listdir(os.path.join(ROOT, 'safeFileIo'))), 1)

        # write the same file, verify the dir & file stay the same
        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(ROOT, 'safeFileIo/test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(ROOT, 'safeFileIo/test.txt')))
        self.assertEqual(len(os.listdir(os.path.join(ROOT, 'safeFileIo'))), 1)

    def testCompareDifferent(self):
        with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(ROOT, 'safeFileIo/test.txt')) as f:
            f.write('bar\n')
            f.write('baz\n')
        self.assertTrue(os.path.exists(os.path.join(ROOT, 'safeFileIo/test.txt')))
        self.assertEqual(len(os.listdir(os.path.join(ROOT, 'safeFileIo'))), 1)

        # write the same file, verify the dir & file stay the same
        def writeNonMatchingFile():
            with dp.safeFileIo.FileForWriteOnceCompareSame(os.path.join(ROOT, 'safeFileIo/test.txt')) as f:
                f.write('boo\n')
                f.write('fop\n')
        self.assertRaises(RuntimeError, writeNonMatchingFile)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
