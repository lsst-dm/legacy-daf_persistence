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

from __future__ import print_function
import lsst.daf.persistence as dafPersist
import lsst.utils.tests as utilsTests
import os
import pickle
import pyfits
import types
import unittest

class MinMapper(dafPersist.Mapper):
    def map_raw(self, dataId, write):
        python = 'pyfits.HDUList'
        persistable = None
        storage = 'FitsStorage'
        path = 'tests/butlerAlias/data/input/raw/raw_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz'
        return dafPersist.ButlerLocation(python, persistable, storage, path, dataId, self)

    def bypass_raw(self, datasetType, pythonType, location, dataId):
        return pyfits.open(location.getLocations()[0])

    def query_raw(self, key, format, dataId):
        values = [{'visit':1, 'filter':'g'}, {'visit':2, 'filter':'g'}, {'visit':3, 'filter':'r'}]
        matches = []
        for value in values:
            match = True
            for item in dataId:
                if value[item] != dataId[item]:
                    match = False
                    break
            if match:
                matches.append(value)
        results = set()
        for match in matches:
            tempTup = []
            for word in format:
                tempTup.append(match[word])
            results.add(tuple(tempTup))
        return results

    def getDefaultLevel(self):
        return 'visit'

    def getKeys(self, datasetType, level):
        return {'filter': types.StringType, 'visit': types.IntType}


class ButlerTestCase(unittest.TestCase):
    """A test case for the basic butler api"""

    datasetType = '@foo'

    def setUp(self):
        self.butler = dafPersist.Butler('tests/butlerAlias/data/input', MinMapper())
        self.butler.defineAlias(self.datasetType, 'raw')

    def tearDown(self):
        del self.butler

    def testGet(self):
        raw_image = self.butler.get(self.datasetType, {'visit':'2', 'filter':'g'})
        # in this case the width is known to be 1026:
        self.assertEqual(raw_image[1].header["NAXIS1"], 1026) # raw_image is an lsst.afw.ExposureU

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
            self.assertEqual(val.sort(), expectedKeyValues[key].sort())

    def testDatasetExists(self):
        # test the valeus that are expected to be true:
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':1}), True)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':2}), True)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'r', 'visit':3}), True)
        
        # test a few values that are expected to be false:
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'f', 'visit':1}), False)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'r', 'visit':1}), False)
        self.assertEqual(self.butler.datasetExists(self.datasetType, {'filter':'g', 'visit':3}), False)

    def testDataRef(self):
        print(self.butler.dataRef(self.datasetType, dataId={'filter':'g', 'visit':1}))

    def testUnregisteredAlias(self):
        with self.assertRaises(RuntimeError):
            self.butler.getKeys('@bar')

    def testOverlappingAlias(self):
        storageCfg = dafPersist.PosixStorage.cfg(root='tests/butlerAlias/data/input')
        accessCfg = dafPersist.Access.cfg(storageCfg=storageCfg)
        repoCfg = dafPersist.Repository.cfg(accessCfg=accessCfg, mapper=MinMapper())
        butlerCfg = dafPersist.Butler.cfg(repoCfg=repoCfg)
        self.butler = dafPersist.Butler(butlerCfg)

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
