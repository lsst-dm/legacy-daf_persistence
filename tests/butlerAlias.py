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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#


import os
import pickle
import types
import unittest
import lsst.afw.fits.fitsLib as fitsLib
import lsst.daf.persistence as dafPersist
import lsst.utils.tests as utilsTests
import pickleMapper

class ButlerTestCase(unittest.TestCase):
    """A test case for the basic butler api"""

    datasetType = '@foo'

    def setUp(self):
        repo_dir = "tests/butlerAlias/data/input"
        self.butler = dafPersist.Butler(repo_dir)
        self.butler.defineAlias(self.datasetType, 'raw')

    def tearDown(self):
        del self.butler

    def testGet(self):
        raw_image = self.butler.get(self.datasetType, visit="2") # (filter is looked up, and found to be unique)
        # in this case the width is known to be 1026:
        self.assertEqual(raw_image.getWidth(), 1026) # raw_image is an lsst.afw.ExposureU

    def testSubset(self):
        subset = self.butler.subset(self.datasetType)
        self.assertEqual(len(subset), 3)

    def testGetKeys(self):
        keys = self.butler.getKeys(self.datasetType)
        self.assertEqual('filter' in keys, True)
        self.assertEqual('visit' in keys, True)
        self.assertEqual(keys['filter'], type("")) # todo how to define a string type?
        self.assertEqual(keys['visit'], type(1)) # todo how to define an int type?

    def testQueryMetadata(self):
        keys = self.butler.getKeys(self.datasetType)
        expectedKeyValues = {'filter':['g', 'r'], 'visit':[1, 2, 3]}
        for key in keys:
            val = self.butler.queryMetadata(self.datasetType, key)
            self.assertEqual(val, expectedKeyValues[key])

    def testDatasetExists(self):
        # I was expecting to be able to iterate over the files as they are named and see
        # that the datasets exist in the files as they are named (for example, a file named
        # raw_v1_fg.fits.gz contains visit 1 and filters f & g. However, the test failed and
        # after inspecting the file I see that the header says it contians filter g. So, I'm
        # not totally clear on the coralation of filename and header data here.
        # instead of iterating the names, I've just inspected the files for expected filters.
        
        # test the valeus that are expected to be true:
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':1}), True)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':2}), True)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'r', 'visit':3}), True)
        
        # test a few values that are expected to be false:
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'f', 'visit':1}), False)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'r', 'visit':1}), False)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':3}), False)

    def testGet(self):
        # todo figure out what needs to be done to get a butler than can put, or make sure
        # to run tests that cover put (like butlerPickle)
        print "WARNING butler::get not tested, run butlerPickle.py to test put"

    def testSubset(self):
        print "WARNING: butler::subset not tested, run butlerSubset.py to test subset"

    def testDataRef(self):
        print self.butler.dataRef(self.datasetType, dataId={'filter':'g', 'visit':1})

    def testUnregisteredAlias(self):
        with self.assertRaises(RuntimeError):
            self.butler.getKeys('@bar')

    def testOverlappingAlias(self):
        repo_dir = 'tests/butlerAlias/data/input'
        self.butler = dafPersist.Butler(repo_dir)
        self.butler.defineAlias('foo', 'raw')
        with self.assertRaises(RuntimeError):
            self.butler.defineAlias('foobar', 'calexp')
        self.butler.defineAlias('barbaz', 'qux')
        with self.assertRaises(RuntimeError):
            self.butler.defineAlias('bar', 'quux')

    def testBadlyFormattedAlias(self):
        with self.assertRaises(RuntimeError):
            self.butler.defineAlias('abc@xyz', 'calexp')

def suite():
    utilsTests.init()
    
    suites = []
    suites += unittest.makeSuite(ButlerTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
