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

import collections
import copy
import os
import pyfits
import shutil
import types
import unittest
import uuid

import lsst.utils.tests as utilsTests
import lsst.daf.persistence as dp
from lsst.daf.persistence import posixRepoCfg

def repoCfg(*args, **kwargs):
    """short hack to work around the issue that the root must be in mapperArgs in some cases"""
    kwargs['mapperArgs'] = {'root':kwargs['root']} if 'root' in kwargs else None
    return posixRepoCfg(*args, **kwargs)

class ParentMapper(dp.Mapper):

    def __init__(self, root):
        self.root = root

    def map_raw(self, dataId, write):
        python = 'pyfits.HDUList'
        persistable = None
        storage = 'PickleStorage'
        path = os.path.join(self.root, 'data/input/raw')
        path = os.path.join(path, 'raw_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz')
        if os.path.exists(path):
            return dp.ButlerLocation(python, persistable, storage, path, dataId, self)
        return None

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


class ChildrenMapper(dp.Mapper):

    def __init__(self, root):
        self.root = root

    def map_raw(self, dataId, write):
        python = 'pyfits.HDUList'
        persistable = None
        storage = 'FitsStorage'
        path = os.path.join(self.root, 'data/input/raw')
        path = os.path.join(path, 'raw_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz')
        if write or os.path.exists(path):
            return dp.ButlerLocation(python, persistable, storage, path, dataId, self)
        return None

    def bypass_raw(self, datasetType, pythonType, location, dataId):
        return pyfits.open(location.getLocations()[0])

    def query_raw(self, key, format, dataId):
        return None
        # results = set()
        # return results

    def getDefaultLevel(self):
        return 'visit'

    def getKeys(self, datasetType, level):
        return {'filter': types.StringType, 'visit': types.IntType}


class TestBasics(unittest.TestCase):
    """Test case for basic functions of the repository classes."""

    def setUp(self):
        # todo
        # on the one hand all this config is a masstive PITA. On the other hand, most existing repos
        # should already contain their parent info. Maybe there is a more elegant way to set up cfg.
        inputRoot = 'tests/butlerAlias'
        outputRootA = 'tests/repository/repoA'
        outputRootB = 'tests/repository/repoB'

        storageCfg = dp.PosixStorage.cfg(root=inputRoot)
        accessCfg = dp.Access.cfg(storageCfg=storageCfg)
        inputRepoCfg = dp.Repository.cfg(accessCfg=accessCfg, mapper=ParentMapper(root=inputRoot))

        storageCfg = dp.PosixStorage.cfg(root=outputRootB)
        accessCfg = dp.Access.cfg(storageCfg=storageCfg)
        repoBCfg = dp.Repository.cfg(accessCfg=accessCfg, parentCfgs=[inputRepoCfg],
                                     mapper=ChildrenMapper(root=outputRootB))

        storageCfg = dp.PosixStorage.cfg(root=outputRootA)
        accessCfg = dp.Access.cfg(storageCfg=storageCfg)
        repoACfg = dp.Repository.cfg(accessCfg=accessCfg, mapper=ChildrenMapper(root=outputRootA),
                                     peerCfgs=[repoBCfg], parentCfgs=[inputRepoCfg])

        butlerCfg = dp.Butler.cfg(repoCfg=repoACfg)
        self.butler = dp.Butler(butlerCfg)

        self.datasetType = 'raw'

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')
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
        print self.butler.dataRef(self.datasetType, dataId={'filter':'g', 'visit':1})

##############################################################################################################
##############################################################################################################
##############################################################################################################

class MapperForTestWriting(dp.Mapper):
    def __init__(self, root):
        self.root = root

    def map_foo(self, dataId, write):
        python = TestObject
        persistable = None
        storage = 'PickleStorage'
        fileName = 'filename'
        for key, value in dataId.iteritems():
            fileName += '_' + key + str(value)
        fileName += '.txt'
        path = os.path.join(self.root, fileName)
        if not write and not os.path.exists(path):
            return None
        return dp.ButlerLocation(python, persistable, storage, path, dataId, self)


class TestObject(object):
    def __init__(self, data):
        self.data = data

    def __eq__(self, other):
        return self.data == other.data


class TestWriting(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    1. creates repo with a peer repo, writes to those repos.
    2. reloads those output repos as a parents of new repos
       * does a read from from the repo (verifies parent search)
    3. writes to the new output repo and reloads it as a parent of a new repo
       * verifies masking
    4. reloads the repo from its persisted cfg
       * verifies reload from cfg
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')
        # del self.butler

    def testCreateAggregateAndLoadingAChild(self):
        """Tests putting a very basic pickled object in a variety of Repository configuration settings
        :return:
        """

        outputRootA = 'tests/repository/repoA'
        storageCfgA = dp.PosixStorage.cfg(root=outputRootA)
        accessCfgA = dp.Access.cfg(storageCfg=storageCfgA)
        repoACfg = dp.Repository.cfg(id='repoA', accessCfg=accessCfgA,
                                     mapper=MapperForTestWriting(root=outputRootA))

        outputRootB = 'tests/repository/repoB'
        storageCfg = dp.PosixStorage.cfg(root=outputRootB)
        accessCfg = dp.Access.cfg(storageCfg=storageCfg)
        repoBCfg = dp.Repository.cfg(id='repoB', accessCfg=accessCfg, peerCfgs=[repoACfg],
                                     mapper=MapperForTestWriting(root=outputRootB))

        butlerCfg = dp.Butler.cfg(repoCfg=repoBCfg)
        butlerAB = dp.Butler(butlerCfg)

        objA = TestObject('abc')
        butlerAB.put(objA, 'foo', {'val':1})
        objB = TestObject('def')
        butlerAB.put(objB, 'foo', {'val':2})

        # create butlers where the output repos are now input repos

        repoCfg = dp.Repository.cfg(id='repoC',
            accessCfg=dp.Access.cfg(storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoC')),
            parentCfgs=[dp.Access(dp.Access.cfg(storageCfg=dp.PosixStorage.cfg('tests/repository/repoA'))).loadCfg()],
            mapper=MapperForTestWriting(root='tests/repository/repoC'))
        butlerC = dp.Butler(dp.Butler.cfg(repoCfg=repoCfg))

        repoCfg = dp.Repository.cfg(id='repoD',
            accessCfg=dp.Access.cfg(storageCfg=dp.PosixStorage.cfg('tests/repository/repoD')),
            parentCfgs=[dp.Access(dp.Access.cfg(storageCfg=dp.PosixStorage.cfg('tests/repository/repoB'))).loadCfg()],
            mapper=MapperForTestWriting(root='tests/repository/repoD'))
        butlerD = dp.Butler(dp.Butler.cfg(repoCfg=repoCfg))

        # # verify the objects exist by getting them
        self.assertEqual(objA, butlerC.get('foo', {'val':1}))
        self.assertEqual(objA, butlerC.get('foo', {'val':1}))
        self.assertEqual(objB, butlerD.get('foo', {'val':2}))
        self.assertEqual(objB, butlerD.get('foo', {'val':2}))


class TestParentMasking(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    1. creates an aggregate repo with 2 output repos, writes to those repos:
       * verifies writing 2 puts (to each repo) and verfied get from each repo
    2. reloads one of those output repos as a parent of another repo
       * does a read from from the repo (verifies parent search)
    3. writes to and reads from the repo
       * verifies masking
    4. reloads the repo from its persisted cfg
       * verifies reload from cfg
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')

    def test(self):
        repoACfg = repoCfg(root='tests/repository/repoA', mapper=MapperForTestWriting)
        butler = dp.Butler(dp.Butler.cfg(repoCfg=repoACfg))
        obj0 = TestObject('abc')
        butler.put(obj0, 'foo', {'bar':1})
        del butler

        repoBCfg = repoCfg(root='tests/repository/repoB', parentRepoCfgs=(repoACfg,),
                               mapper=MapperForTestWriting)
        butler = dp.Butler(dp.Butler.cfg(repoCfg=repoBCfg))
        obj1 = butler.get('foo', {'bar':1})
        self.assertEqual(obj0, obj1)
        obj1.data = "def"
        butler.put(obj1, 'foo', {'bar':1})

        repoCCfg = repoCfg(root='tests/repository/repoB', parentRepoCfgs=(repoBCfg,),
                               mapper=MapperForTestWriting)
        butler = dp.Butler(dp.Butler.cfg(repoCfg=repoCCfg))
        obj2 = butler.get('foo', {'bar':1})
        self.assertEqual(obj1, obj2)


class TestPeerPut(unittest.TestCase):
    """A test case for the repository classes.

    A test that
        1. creates 3 peer repositories and readers for them
        2. does a single put
        3. verifies that all repos received the put
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')

    def test(self):
        repoACfg = repoCfg(root='tests/repository/repoA', mapper=MapperForTestWriting)
        repoBCfg = repoCfg(root='tests/repository/repoB', mapper=MapperForTestWriting)
        repoCCfg = repoCfg(root='tests/repository/repoC', mapper=MapperForTestWriting,
                               peerCfgs=[repoACfg, repoBCfg])


        butler = dp.Butler(dp.Butler.cfg(repoCfg=repoCCfg))
        obj0 = TestObject('abc')
        butler.put(obj0, 'foo', {'bar':1})

        for cfg in (repoACfg, repoBCfg, repoCCfg):
            butler = dp.Butler(dp.Butler.cfg(repoCfg=repoCfg(parentRepoCfgs=(cfg,))))
            self.assertEqual(butler.get('foo', {'bar':1}), obj0)


class TestAggregateParent(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    - create 2 repos & close them
    - create an aggregate repo, use the 2 repos as parents.
    - test reads from parents
    - test write to aggregate child
    - test reloading from aggregate
    - test reloading from child
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')


    def test(self):
        repoACfg = repoCfg(root='tests/repository/repoA', mapper=MapperForTestWriting)
        repoBCfg = repoCfg(root='tests/repository/repoB', mapper=MapperForTestWriting)
        butlerA = dp.Butler(dp.Butler.cfg(repoACfg))
        butlerB = dp.Butler(dp.Butler.cfg(repoBCfg))
        readerA = dp.Butler(dp.Butler.cfg(repoCfg=repoCfg(parentRepoCfgs=(repoACfg,))))
        readerB = dp.Butler(dp.Butler.cfg(repoCfg=repoCfg(parentRepoCfgs=(repoBCfg,))))

        # identical overlapping contents
        obj0 = TestObject('abc')
        butlerA.put(obj0, 'foo', {'bar':1})
        butlerB.put(obj0, 'foo', {'bar':1})
        self.assertEqual(readerA.get('foo', {'bar':1}), obj0)
        self.assertEqual(readerB.get('foo', {'bar':1}), obj0)

        # overlapping dataId with different values
        obj1 = TestObject('abc')
        obj2 = TestObject('def')
        butlerA.put(obj1, 'foo', {'bar':2})
        butlerB.put(obj2, 'foo', {'bar':2})
        self.assertEqual(readerA.get('foo', {'bar':2}), obj1)
        self.assertEqual(readerB.get('foo', {'bar':2}), obj2)

        # entirely different dataId & values
        obj3 = TestObject('abc')
        obj4 = TestObject('def')
        butlerA.put(obj3, 'foo', {'bar':3})
        butlerB.put(obj4, 'foo', {'bar':4})
        self.assertEqual(readerA.get('foo', {'bar':3}), obj3)
        self.assertEqual(readerB.get('foo', {'bar':4}), obj4)

        del butlerA
        del butlerB
        del readerA
        del readerB

        # test first-found get behavior
        repoACfg = repoCfg(root='tests/repository/repoA', mapper=MapperForTestWriting)
        repoBCfg = repoCfg(root='tests/repository/repoB', mapper=MapperForTestWriting)
        repoABCfg = repoCfg(root='tests/repository/repoAB', mapper=MapperForTestWriting,
                                parentRepoCfgs=(repoACfg, repoBCfg))
        butlerAB = dp.Butler(dp.Butler.cfg(repoCfg=repoABCfg))
        res = butlerAB.get('foo', {'bar':1})
        self.assertEqual(res, (obj0))

        # test first-found get behavior
        repoACfg = repoCfg(root='tests/repository/repoA', mapper=MapperForTestWriting)
        repoBCfg = repoCfg(root='tests/repository/repoB', mapper=MapperForTestWriting)
        repoABCfg = repoCfg(root='tests/repository/repoAB', mapper=MapperForTestWriting,
                                parentRepoCfgs=(repoACfg, repoBCfg), parentJoin='outer')
        butlerAB = dp.Butler(dp.Butler.cfg(repoCfg=repoABCfg))
        try:
            res = butlerAB.get('foo', {'bar':1})
            self.assertTrue(False, "butlerAB.get should have thrown")
        except dp.butlerExceptions.MultipleResults as e:
            # the butler should have found 2 results
            self.assertEqual(len(e.locations), 2)

def suite():
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(TestBasics)
    suites += unittest.makeSuite(TestWriting)
    suites += unittest.makeSuite(TestParentMasking)
    suites += unittest.makeSuite(TestAggregateParent)
    suites += unittest.makeSuite(TestPeerPut)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
