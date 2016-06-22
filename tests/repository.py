# -*- coding: UTF-8 -*-
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

import yaml

import lsst.utils.tests as utilsTests
import lsst.daf.persistence as dp
from lsst.daf.persistence import Policy

class ParentMapper(dp.Mapper):

    @classmethod
    def cfg(cls, root=None):
        return Policy({'cls':cls, 'root':root})

    def __init__(self, cfg):
        try:
            self.root = cfg['root']
            self.cfg = copy.deepcopy(cfg)
        except TypeError:
            # handle the case where cfg is just a root, not a proper cfg
            self.root = cfg
            self.cfg = ParentMapper.cfg(root=cfg)

    def __repr__(self):
        return 'ParentMapper(cfg=%s)' % self.cfg

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

    def query_raw(self, format, dataId):
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

    def map_str(self, dataId, write):
        path = os.path.join(self.root, 'data/input/raw')
        path = os.path.join(path, 'raw_v' + str(dataId['str']) + '_f' + dataId['filter'] + '.fits.gz')
        if os.path.exists(path):
            return dp.ButlerLocation(str, None, 'PickleStorage', path, dataId, self)
        return None


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
        inputRoot = 'tests/butlerAlias'
        outputRootA = 'tests/repository/repoA'
        outputRootB = 'tests/repository/repoB'

        inputRepoCfg = dp.Repository.cfg(mode='r',
                                         storageCfg=dp.PosixStorage.cfg(root=inputRoot),
                                         mapper=ParentMapper(ParentMapper.cfg(root=inputRoot)))

        repoBCfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root=outputRootB), 
                                     mapper=ChildrenMapper(root=outputRootB))

        repoACfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root=outputRootA), 
                                     mapper=ChildrenMapper(root=outputRootA))
        self.butler = dp.Butler(inputs=inputRepoCfg, outputs=repoACfg)

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
            format = (key, )
            val = self.butler.queryMetadata(self.datasetType, format)
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
        repoACfg = dp.Repository.cfg(mode='w', 
                                     storageCfg=dp.PosixStorage.cfg(root=outputRootA),
                                     mapper=MapperForTestWriting(root=outputRootA))
        outputRootB = 'tests/repository/repoB'
        repoBCfg = dp.Repository.cfg(mode='w', 
                                     storageCfg=dp.PosixStorage.cfg(root=outputRootB), 
                                     mapper=MapperForTestWriting(root=outputRootB))
        butlerAB = dp.Butler(outputs=[repoACfg, repoBCfg])

        objA = TestObject('abc')
        butlerAB.put(objA, 'foo', {'val':1})
        objB = TestObject('def')
        butlerAB.put(objB, 'foo', {'val':2})

        # create butlers where the output repos are now input repos

        repoACfg['mode'] = 'r'
        butlerC = dp.Butler(inputs=repoACfg)

        repoBCfg['mode'] = 'r'
        butlerD = dp.Butler(inputs=repoBCfg)

        # # verify the objects exist by getting them
        self.assertEqual(objA, butlerC.get('foo', {'val':1}))
        self.assertEqual(objA, butlerC.get('foo', {'val':1}))
        self.assertEqual(objB, butlerD.get('foo', {'val':2}))
        self.assertEqual(objB, butlerD.get('foo', {'val':2}))


class TestMasking(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    1. creates a repo, does a put
    2. creates a new butler, uses that repo as an input and creates a new read-write output repo
    3. gets from the input repo, modifies the dataset, and puts into the output repo
    4. does a get and verifies that the changed dataset is returned.
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')

    def test(self):
        repoACfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoACfg)
        obj0 = TestObject('abc')
        butler.put(obj0, 'foo', {'bar':1})
        del butler

        repoACfg['mode'] = 'r'
        repoBCfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(inputs=repoACfg, outputs=repoBCfg)
        obj1 = butler.get('foo', {'bar':1})
        self.assertEqual(obj0, obj1)
        obj1.data = "def"
        butler.put(obj1, 'foo', {'bar':1})
        obj2 = butler.get('foo', {'bar':1})
        self.assertEqual(obj1, obj2)


class TestMultipleOutputsPut(unittest.TestCase):
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
        repoACfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'),
                                     mapper=MapperForTestWriting)
        repoBCfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'),
                                     mapper=MapperForTestWriting)

        butler = dp.Butler(outputs=(repoACfg, repoBCfg))
        obj0 = TestObject('abc')
        butler.put(obj0, 'foo', {'bar':1})

        for cfg in (repoACfg, repoBCfg):
            cfg['mode'] = 'r'
            butler = dp.Butler(inputs=cfg)
            self.assertEqual(butler.get('foo', {'bar':1}), obj0)


class TestMultipleInputs(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    - create output 2 repos, write same & different data to them & close them
    - create a new butler with those repos as inputs
    - read data that was written to both repos:
        - verify data that was written to only one of each repo
        - verify dissimilar datasets with same id that were written to the repos

    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')


    def test(self):
        objAbc = TestObject('abc')
        objDef = TestObject('def')
        objGhi = TestObject('ghi')
        objJkl = TestObject('jkl')

        repoACfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoACfg)
        butler.put(objAbc, 'foo', {'bar':1})
        butler.put(objDef, 'foo', {'bar':2})

        repoBCfg = dp.Repository.cfg(mode='w',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoBCfg)
        butler.put(objGhi, 'foo', {'bar':1}) # note different object with overlapping dataId with repoA
        butler.put(objJkl, 'foo', {'bar':3})

        repoACfg['mode'] = 'r'
        repoBCfg['mode'] = 'r'
        butler = dp.Butler(inputs=(repoACfg, repoBCfg))
        self.assertEqual(butler.get('foo', {'bar':1}), objAbc)
        self.assertEqual(butler.get('foo', {'bar':2}), objDef)
        self.assertEqual(butler.get('foo', {'bar':3}), objJkl)

        repoACfg['mode'] = 'r'
        repoBCfg['mode'] = 'r'
        butler = dp.Butler(inputs=(repoBCfg, repoACfg))
        self.assertEqual(butler.get('foo', {'bar':1}), objGhi)
        self.assertEqual(butler.get('foo', {'bar':2}), objDef)
        self.assertEqual(butler.get('foo', {'bar':3}), objJkl)


class TestTagging(unittest.TestCase):
    """A test case for the tagging of repository classes.
    """

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')


    def testOneLevelInputs(self):
        """
        1. put an object with the same ID but slightly different value into 2 repositories.
        2. use those repositories as inputs to a butler, and tag them
        3. make sure that the correct object is gotten for each of
            a. one tag
            b. the other tag
            c. no tag
        4. repeat step 3 but reverse the order of input cfgs to a new butler.
        5. use the butler from step 4 and write an output. The inputs will get recorded as parents of the 
           output repo.
        6. create a new butler with a new overlapping repo, and verify that objects can be gotten from the 
           other's parent repos via tagging.
        """
        objA = TestObject('a')
        objB = TestObject('b')

        # put objA in repo1:
        repo1Cfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repo1'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repo1Cfg)
        butler.put(objA, 'foo', {'bar':1})
        # TODO butler should persist the cfg
        del butler

        # put objB in repo2:
        repo2Cfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repo2'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repo2Cfg)
        butler.put(objB, 'foo', {'bar':1})
        # TODO butler should persist the cfg
        del butler

        repo1Cfg.tag('one')
        repo2Cfg.tag('two')

        # make the objects inputs of repos
        # and verify the correct object can ge fetched using the tag and not using the tag

        # todo should get the cfg from the repo
        butler = dp.Butler(inputs=(repo1Cfg, repo2Cfg))
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='two')), objB)
        self.assertEqual(butler.get('foo', {'bar':1}), objA)

        butler = dp.Butler(inputs=(repo2Cfg, repo1Cfg))
        self.assertEqual(butler.get('foo', dp.DataId(bar=1, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId(bar=1, tag='two')), objB)
        self.assertEqual(butler.get('foo', dp.DataId(bar=1)), objB)

        # create butler with repo1 and repo2 as parents, and an output repo3.
        repo3Cfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repo3'), 
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(inputs=(repo1Cfg, repo2Cfg), outputs=repo3Cfg)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='two')), objB)
        self.assertEqual(butler.get('foo', {'bar':1}), objA)
        # add an object to the output repo. note since the output repo mode is 'rw' that object is gettable
        # and it has first priority in search order. Other repos should be searchable by tagging.
        objC = TestObject('c')
        butler.put(objC, 'foo', {'bar':1})
        self.assertEqual(butler.get('foo', {'bar':1}), objC)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='two')), objB)
        del butler

        # expand the structure to look like this:
        # ┌────────────────────────┐ ┌────────────────────────┐
        # │repo1                   │ │repo2                   │
        # │ tag:"one"              │ │ tag:"two"              │
        # │ TestObject('a')        │ │ TestObject('b')        │
        # │   at ('foo', {'bar:1'})│ │   at ('foo', {'bar:1'})│
        # └───────────┬────────────┘ └───────────┬────────────┘
        #             └─────────────┬────────────┘
        #              ┌────────────┴───────────┐ ┌────────────────────────┐
        #              │repo4                   │ │repo5                   │
        #              │ tag:"four"             │ │ tag:"five"             │
        #              │ TestObject('d')        │ │ TestObject('e')        │
        #              │   at ('foo', {'bar:2'})│ │   at ('foo', {'bar:1'})│
        #              └───────────┬────────────┘ └───────────┬────────────┘
        #                          └─────────────┬────────────┘
        #                                     ┌──┴───┐ 
        #                                     │butler│ 
        #                                     └──────┘ 


        repo4Cfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repo4'), 
                                     mapper=MapperForTestWriting)
        repo4Cfg.tag('four')
        butler = dp.Butler(inputs=(repo1Cfg, repo2Cfg), outputs=repo4Cfg)
        objD = TestObject('d')
        butler.put(objD, 'foo', {'bar':2})
        del butler

        repo5Cfg = dp.Repository.cfg(mode='rw',
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repo5'), 
                                     mapper=MapperForTestWriting)
        repo5Cfg.tag('five')
        butler = dp.Butler(outputs=repo5Cfg)
        objE = TestObject('e')
        butler.put(objE, 'foo', {'bar':1})
        del butler

        butler = dp.Butler(inputs=(repo4Cfg, repo5Cfg))
        self.assertEqual(butler.get('foo', {'bar':1}), objA)
        self.assertEqual(butler.get('foo', {'bar':2}), objD)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='four')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='five')), objE)
        del butler

        butler = dp.Butler(inputs=(repo5Cfg, repo4Cfg))
        self.assertEqual(butler.get('foo', {'bar':1}), objE)
        self.assertEqual(butler.get('foo', {'bar':2}), objD)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='four')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar':1}, tag='five')), objE)
        del butler

class TestMapperInference(unittest.TestCase):
    """A test for inferring mapper in the cfg from parent cfgs"""

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')
        # del self.butler

    def testSingleParent(self):
        """ 
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has a parent that does have a mapper specified in the cfg
        verifies that the child repo inherits the parent's mapper via inference.
        """
        repoACfg = dp.Repository.cfg(mode='r', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'),
                                     mapper=MapperForTestWriting)
        repoBCfg = dp.Repository.cfg(mode='rw', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'), 
                                     parentCfgs=repoACfg)
        butler = dp.Butler(outputs=repoBCfg)
        self.assertTrue(isinstance(butler.outputs[0].repo._mapper, MapperForTestWriting))


    def testMultipleParentsSameMapper(self):
        """
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has 2 parents that do have the same mapper specified in the cfg
        verifies that the child repo inherits the parent's mapper via inference.

        """
        repoACfg = dp.Repository.cfg(mode='r', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'),
                                     mapper=MapperForTestWriting)
        repoBCfg = dp.Repository.cfg(mode='r', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'), 
                                     mapper=MapperForTestWriting)
        repoCCfg = dp.Repository.cfg(mode='w', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoC'), 
                                     parentCfgs=(repoACfg, repoBCfg))
        butler = dp.Butler(outputs=repoCCfg)
        self.assertTrue(isinstance(butler.outputs[0].repo._mapper, MapperForTestWriting))


    class AlternateMapper(object) :
        pass


    def testMultipleParentsDifferentMappers(self):
        """
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has 2 parent repos that have different mappers
        verifies that the constructor raises a RuntimeError because the mapper can not be inferred.
        """
        repoACfg = dp.Repository.cfg(mode='r', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoA'),
                                     mapper=MapperForTestWriting)
        repoBCfg = dp.Repository.cfg(mode='r', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoB'), 
                                     mapper=TestMapperInference.AlternateMapper)
        repoCCfg = dp.Repository.cfg(mode='w', 
                                     storageCfg=dp.PosixStorage.cfg(root='tests/repository/repoC'), 
                                     parentCfgs=(repoACfg, repoBCfg))
        self.assertRaises(RuntimeError, dp.Butler, outputs=repoCCfg)

if __name__ == '__main__':
    unittest.main()
