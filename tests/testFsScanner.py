#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2015 LSST Corporation.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import unittest
import os
import lsst.utils.tests
from lsst.daf.persistence import FsScanner
import shutil

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))
TEMP_ROOT = os.path.join(os.path.join(ROOT, 'temp'))


class FsScannerTestCase(unittest.TestCase):

    def setUp(self):
        if os.makedirs(TEMP_ROOT):
            os.makedirs(TEMP_ROOT)

    def tearDown(self):
        if os.path.exists(TEMP_ROOT):
            shutil.rmtree(TEMP_ROOT)

    def test1(self):
        template = '%(visit)d%(state)1s.fits.fz[%(extension)d]'
        scanner = FsScanner(template)
        res = scanner.processPath(os.path.join(ROOT, 'testFsScanner'))
        self.assertEqual(res, {'1038843o.fits.fz': {'state': 'o', 'visit': 1038843}})

    def test2(self):
        template = 'raw_v%(visit)d_f%(filter)s.fits.gz'
        scanner = FsScanner(template)
        res = scanner.processPath(os.path.join(ROOT, 'testFsScanner'))
        self.assertEqual(res, {'raw_v1_fg.fits.gz': {'visit': 1, 'filter': 'g'}})

    def test3(self):
        folder = os.path.join(TEMP_ROOT, "2015-12-22")
        os.makedirs(folder)
        with open(os.path.join(folder, "PFSA000004r2.fits"), 'w') as f:
            pass
        template = "%(dateObs)s/PF%(site)1s%(category)1s%(visit)06d%(arm)s%(spectrograph)1d.fits"
        scanner = FsScanner(template)
        res = scanner.processPath(TEMP_ROOT)
        expected = {'2015-12-22/PFSA000004r2.fits' : {'category': 'A', 'visit': 4, 'dateObs': '2015-12-22',
            'site': 'S', 'spectrograph': 2, 'arm': 'r'}}
        self.assertEqual(expected, res)

    def test4(self):
        with open(os.path.join(TEMP_ROOT, "001004.fits"), 'w') as f:
            pass
        template = "%(a)3d%(b)3d.fits"
        scanner = FsScanner(template)
        res = scanner.processPath(TEMP_ROOT)
        expected = {'001004.fits' : {'a': 1, 'b': 4}}
        self.assertEqual(expected, res)

class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
