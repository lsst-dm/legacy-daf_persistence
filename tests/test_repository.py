# -*- coding: UTF-8 -*-

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
# (at your option) any later version.1n
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

from builtins import str
from builtins import range

import os
import astropy.io.fits
import shutil
import sqlite3
import unittest
import tempfile

import lsst.daf.persistence as dp
# can't use name TestObject, becuase it messes Pytest up. Alias it to tstObj
from lsst.daf.persistence.test import TestObject as tstObj
from lsst.daf.persistence.test import MapperForTestWriting
import lsst.utils.tests

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class ParentMapper(dp.Mapper):

    def __init__(self, root, **kwargs):
        self.root = root
        self.storage = dp.Storage.makeFromURI(self.root)

    def __repr__(self):
        return 'ParentMapper(root=%s)' % self.root

    def map_raw(self, dataId, write):
        python = 'astropy.io.fits.HDUList'
        persistable = None
        storage = 'PickleStorage'
        path = os.path.join(self.root, 'raw')
        path = os.path.join(path, 'raw_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz')
        if os.path.exists(path):
            return dp.ButlerLocation(python, persistable, storage, path,
                                     dataId, self, self.storage)
        return None

    def bypass_raw(self, datasetType, pythonType, location, dataId):
        return astropy.io.fits.open(location.getLocations()[0])

    def query_raw(self, format, dataId):
        values = [{'visit': 1, 'filter': 'g'}, {'visit': 2, 'filter': 'g'}, {'visit': 3, 'filter': 'r'}]
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
        return {'filter': str, 'visit': int}

    def map_str(self, dataId, write):
        path = os.path.join(self.root, 'raw')
        path = os.path.join(path, 'raw_v' + str(dataId['str']) + '_f' + dataId['filter'] + '.fits.gz')
        if os.path.exists(path):
            return dp.ButlerLocation(str, None, 'PickleStorage', path, dataId,
                                     self, self.storage)
        return None


class ChildrenMapper(dp.Mapper):

    def __init__(self, root, **kwargs):
        self.root = root
        self.storage = dp.Storage.makeFromURI(self.root)

    def map_raw(self, dataId, write):
        python = 'astropy.io.fits.HDUList'
        persistable = None
        storage = 'FitsStorage'
        path = os.path.join(self.root, 'raw')
        path = os.path.join(path, 'raw_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz')
        if write or os.path.exists(path):
            return dp.ButlerLocation(python, persistable, storage, path,
                                     dataId, self, self.storage)
        return None

    def map_pickled(self, dataId, write):
        python = 'dict'
        persistable = None
        storage = 'PickleStorage'
        path = os.path.join(self.root, 'raw')
        path = os.path.join(path, 'pickled_v' + str(dataId['visit']) + '_f' + dataId['filter'] + '.fits.gz')
        if write or os.path.exists(path):
            return dp.ButlerLocation(python, persistable, storage, path,
                                     dataId, self, self.storage)

    def bypass_raw(self, datasetType, pythonType, location, dataId):
        return astropy.io.fits.open(location.getLocations()[0])

    def query_raw(self, format, dataId):
        return None
        # results = set()
        # return results

    def getDefaultLevel(self):
        return 'visit'

    def getKeys(self, datasetType, level):
        return {'filter': str, 'visit': int}


class TestBasics(unittest.TestCase):
    """Test case for basic functions of the repository classes."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestBasics-")
        inputRepoArgs = dp.RepositoryArgs(root=os.path.join(ROOT, 'butlerAlias', 'data', 'input'),
                                          mapper=ParentMapper,
                                          tags='baArgs')
        # mode of output repos is write-only by default
        outputRepoArgs = dp.RepositoryArgs(root=os.path.join(self.testDir, 'repoA'),
                                           mapper=ChildrenMapper,
                                           mode='rw')
        self.butler = dp.Butler(inputs=inputRepoArgs, outputs=outputRepoArgs)

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        if os.path.exists(os.path.join(ROOT, 'butlerAlias/repositoryCfg.yaml')):
            os.remove(os.path.join(ROOT, 'butlerAlias/repositoryCfg.yaml'))
        if hasattr(self, "butler"):
            del self.butler

    def testGet(self):
        raw_image = self.butler.get('raw', {'visit': '2', 'filter': 'g'})
        # in this case the width is known to be 1026:
        self.assertEqual(raw_image[1].header["NAXIS1"], 1026)

    def testGetUri(self):
        raw_uri = self.butler.getUri('raw', {'visit': '2', 'filter': 'g'})
        self.assertEqual(raw_uri,
                         os.path.join(ROOT, 'butlerAlias', 'data', 'input', 'raw', 'raw_v2_fg.fits.gz'))
        self.assertEqual(os.path.isfile(raw_uri), True)

    def testSubset(self):
        subset = self.butler.subset('raw')
        self.assertEqual(len(subset), 3)

    def testGetKeys(self):
        keys = self.butler.getKeys('raw')
        self.assertIn('filter', keys)
        self.assertIn('visit', keys)
        self.assertEqual(keys['filter'], str)
        self.assertEqual(keys['visit'], int)

        keys = self.butler.getKeys('raw', tag='baArgs')
        self.assertIn('filter', keys)
        self.assertIn('visit', keys)
        self.assertEqual(keys['filter'], str)
        self.assertEqual(keys['visit'], int)

        keys = self.butler.getKeys('raw', tag=('baArgs', 'foo'))
        self.assertIn('filter', keys)
        self.assertIn('visit', keys)
        self.assertEqual(keys['filter'], str)
        self.assertEqual(keys['visit'], int)

        keys = self.butler.getKeys('raw', tag='foo')
        self.assertIsNone(keys)

        keys = self.butler.getKeys('raw', tag=set(['baArgs', 'foo']))
        self.assertIn('filter', keys)
        self.assertIn('visit', keys)
        self.assertEqual(keys['filter'], str)
        self.assertEqual(keys['visit'], int)

    def testQueryMetadata(self):
        val = self.butler.queryMetadata('raw', ('filter',))
        val.sort()
        self.assertEqual(val, ['g', 'r'])

        val = self.butler.queryMetadata('raw', ('visit',))
        val.sort()
        self.assertEqual(val, [1, 2, 3])

        val = self.butler.queryMetadata('raw', ('visit',), dataId={'filter': 'g'})
        val.sort()
        self.assertEqual(val, [1, 2])

        val = self.butler.queryMetadata('raw', ('visit',), dataId={'filter': 'r'})
        self.assertEqual(val, [3])

        val = self.butler.queryMetadata('raw', ('filter',), dataId={'visit': 1})
        self.assertEqual(val, ['g'])

        val = self.butler.queryMetadata('raw', ('filter',), dataId={'visit': 2})
        self.assertEqual(val, ['g'])

        val = self.butler.queryMetadata('raw', ('filter',), dataId={'visit': 3})
        self.assertEqual(val, ['r'])

        val = self.butler.queryMetadata('raw', ('visit',), dataId={'filter': 'h'})
        self.assertEqual(val, [])

        dataId = dp.DataId({'filter': 'g'}, tag='baArgs')
        val = self.butler.queryMetadata('raw', ('visit',), dataId={'filter': 'g'})
        val.sort()
        self.assertEqual(val, [1, 2])

        dataId = dp.DataId({'filter': 'g'}, tag='foo')
        val = self.butler.queryMetadata('raw', ('visit',), dataId=dataId)
        self.assertEqual(val, [])

    def testDatasetExists(self):
        # test the values that are expected to be true:
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'g', 'visit': 1}), True)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'g', 'visit': 2}), True)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'r', 'visit': 3}), True)

        # test a few values that are expected to be false:
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'f', 'visit': 1}), False)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'r', 'visit': 1}), False)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'g', 'visit': 3}), False)

        # test that we don't see datasets in the input repo as being in the output repo
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'g', 'visit': 1}, write=True), False)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'g', 'visit': 2}, write=True), False)
        self.assertEqual(self.butler.datasetExists('raw', {'filter': 'r', 'visit': 3}, write=True), False)

        # write a dataset to the output repo and check that we see it with both write=True and write=False
        dataId = {'visit': '2', 'filter': 'g'}
        self.butler.put({"number": 3}, 'pickled', dataId)
        self.assertEqual(self.butler.datasetExists('pickled', dataId, write=True), True)
        self.assertEqual(self.butler.datasetExists('pickled', dataId, write=False), True)


##############################################################################################################
##############################################################################################################
##############################################################################################################


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
    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestWriting-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        # del self.butler

    def testCreateAggregateAndLoadingAChild(self):
        """Tests putting a very basic pickled object in a variety of Repository configuration settings
        :return:
        """

        repoAArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoA'),
                                      mapper=MapperForTestWriting)
        repoBArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoB'),
                                      mapper=MapperForTestWriting)
        butlerAB = dp.Butler(outputs=[repoAArgs, repoBArgs])

        objA = tstObj('abc')
        butlerAB.put(objA, 'foo', {'val': 1})
        objB = tstObj('def')
        butlerAB.put(objB, 'foo', {'val': 2})

        # create butlers where the output repos are now input repos
        butlerC = dp.Butler(inputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'repoA')))
        butlerD = dp.Butler(inputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'repoB')))

        # # verify the objects exist by getting them
        self.assertEqual(objA, butlerC.get('foo', {'val': 1}))
        self.assertEqual(objA, butlerC.get('foo', {'val': 1}))
        self.assertEqual(objB, butlerD.get('foo', {'val': 2}))
        self.assertEqual(objB, butlerD.get('foo', {'val': 2}))


class TestMasking(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    1. creates a repo, does a put
    2. creates a new butler, uses that repo as an input and creates a new read-write output repo
    3. gets from the input repo, modifies the dataset, and puts into the output repo
    4. does a get and verifies that the changed dataset is returned.
    """
    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestMasking-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test(self):
        repoAArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoA'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoAArgs)
        obj0 = tstObj('abc')
        butler.put(obj0, 'foo', {'bar': 1})
        del butler
        del repoAArgs

        repoBArgs = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repoB'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(inputs=os.path.join(self.testDir, 'repoA'), outputs=repoBArgs)
        obj1 = butler.get('foo', {'bar': 1})
        self.assertEqual(obj0, obj1)
        obj1.data = "def"
        butler.put(obj1, 'foo', {'bar': 1})
        obj2 = butler.get('foo', {'bar': 1})
        self.assertEqual(obj1, obj2)


class TestMultipleOutputsPut(unittest.TestCase):
    """A test case for the repository classes.

    A test that
        1. creates 3 peer repositories and readers for them
        2. does a single put
        3. verifies that all repos received the put
    """
    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestMultipleOutputsPut-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test(self):
        repoAArgs = dp.RepositoryArgs(root=os.path.join(self.testDir, 'repoA'),
                                      mapper=MapperForTestWriting)
        repoBArgs = dp.RepositoryArgs(root=os.path.join(self.testDir, 'repoB'),
                                      mapper=MapperForTestWriting)

        butler = dp.Butler(outputs=(repoAArgs, repoBArgs))
        obj0 = tstObj('abc')
        butler.put(obj0, 'foo', {'bar': 1})

        for root in (os.path.join(self.testDir, 'repoA'),
                     os.path.join(self.testDir, 'repoB')):
            butler = dp.Butler(inputs=root)
            self.assertEqual(butler.get('foo', {'bar': 1}), obj0)


class TestMultipleInputs(unittest.TestCase):
    """A test case for the repository classes.

    A test that
    - create output 2 repos, write same & different data to them & close them
    - create a new butler with those repos as inputs
    - read data that was written to both repos:
        - verify data that was written to only one of each repo
        - verify dissimilar datasets with same id that were written to the repos

    """
    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestMapperInference-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test(self):
        objAbc = tstObj('abc')
        objDef = tstObj('def')
        objGhi = tstObj('ghi')
        objJkl = tstObj('jkl')

        repoAArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoA'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoAArgs)
        butler.put(objAbc, 'foo', {'bar': 1})
        butler.put(objDef, 'foo', {'bar': 2})

        repoBArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoB'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repoBArgs)
        butler.put(objGhi, 'foo', {'bar': 1})  # note different object with overlapping dataId with repoA
        butler.put(objJkl, 'foo', {'bar': 3})

        # note repo order: A, B
        butler = dp.Butler(inputs=(os.path.join(self.testDir, 'repoA'),
                                   os.path.join(self.testDir, 'repoB')))
        self.assertEqual(butler.get('foo', {'bar': 1}), objAbc)
        self.assertEqual(butler.get('foo', {'bar': 2}), objDef)
        self.assertEqual(butler.get('foo', {'bar': 3}), objJkl)

        # note reversed repo order: B, A
        butler = dp.Butler(inputs=(os.path.join(self.testDir, 'repoB'),
                                   os.path.join(self.testDir, 'repoA')))
        self.assertEqual(butler.get('foo', {'bar': 1}), objGhi)
        self.assertEqual(butler.get('foo', {'bar': 2}), objDef)
        self.assertEqual(butler.get('foo', {'bar': 3}), objJkl)


class TestTagging(unittest.TestCase):
    """A test case for the tagging of repository classes.
    """

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestTagging-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

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
        objA = tstObj('a')
        objB = tstObj('b')

        # put objA in repo1:
        repo1Args = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repo1'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repo1Args)
        butler.put(objA, 'foo', {'bar': 1})
        del butler

        # put objB in repo2:
        repo2Args = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repo2'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repo2Args)
        butler.put(objB, 'foo', {'bar': 1})
        del butler
        del repo1Args
        del repo2Args

        # make the objects inputs of repos
        # and verify the correct object can ge fetched using the tag and not using the tag

        repo1Args = dp.RepositoryArgs(root=os.path.join(self.testDir, 'repo1'), tags='one')
        repo2Args = dp.RepositoryArgs(root=os.path.join(self.testDir, 'repo2'), tags='two')

        butler = dp.Butler(inputs=(repo1Args, repo2Args))
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='two')), objB)
        self.assertEqual(butler.get('foo', {'bar': 1}), objA)

        butler = dp.Butler(inputs=(repo2Args, repo1Args))
        self.assertEqual(butler.get('foo', dp.DataId(bar=1, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId(bar=1, tag='two')), objB)
        self.assertEqual(butler.get('foo', dp.DataId(bar=1)), objB)

        # create butler with repo1 and repo2 as parents, and an output repo3.
        repo3Args = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repo3'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(inputs=(repo1Args, repo2Args), outputs=repo3Args)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='two')), objB)
        self.assertEqual(butler.get('foo', {'bar': 1}), objA)
        # add an object to the output repo. note since the output repo mode is 'rw' that object is gettable
        # and it has first priority in search order. Other repos should be searchable by tagging.
        objC = tstObj('c')
        butler.put(objC, 'foo', {'bar': 1})
        self.assertEqual(butler.get('foo', {'bar': 1}), objC)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='one')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='two')), objB)
        del butler

        repo3Cfg = dp.Storage().getRepositoryCfg(os.path.join(self.testDir, 'repo3'))
        self.assertEqual(repo3Cfg.parents, [os.path.join(self.testDir, 'repo1'),
                                            os.path.join(self.testDir, 'repo2')])

        # expand the structure to look like this:
        # ┌────────────────────────┐ ┌────────────────────────┐
        # │repo1                   │ │repo2                   │
        # │ tag:"one"              │ │ tag:"two"              │
        # │ tstObj('a')            │ │ tstObj('b')            │
        # │   at ('foo', {'bar:1'})│ │   at ('foo', {'bar:1'})│
        # └───────────┬────────────┘ └───────────┬────────────┘
        #             └─────────────┬────────────┘
        #              ┌────────────┴───────────┐ ┌────────────────────────┐
        #              │repo4                   │ │repo5                   │
        #              │ tag:"four"             │ │ tag:"five"             │
        #              │ tstObj('d')            │ │ tstObj('e')            │
        #              │   at ('foo', {'bar:2'})│ │   at ('foo', {'bar:1'})│
        #              └───────────┬────────────┘ └───────────┬────────────┘
        #                          └─────────────┬────────────┘
        #                                     ┌──┴───┐
        #                                     │butler│
        #                                     └──────┘

        repo4Args = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repo4'),
                                      mapper=MapperForTestWriting)
        butler = dp.Butler(inputs=(os.path.join(self.testDir, 'repo1'),
                                   os.path.join(self.testDir, 'repo2')), outputs=repo4Args)
        objD = tstObj('d')
        butler.put(objD, 'foo', {'bar': 2})
        del butler

        repo5Cfg = dp.RepositoryArgs(mode='rw',
                                     root=os.path.join(self.testDir, 'repo5'),
                                     mapper=MapperForTestWriting)
        butler = dp.Butler(outputs=repo5Cfg)
        objE = tstObj('e')
        butler.put(objE, 'foo', {'bar': 1})
        del butler

        repo4Args = dp.RepositoryArgs(cfgRoot=os.path.join(self.testDir, 'repo4'), tags='four')
        repo5Args = dp.RepositoryArgs(cfgRoot=os.path.join(self.testDir, 'repo5'), tags='five')
        butler = dp.Butler(inputs=(repo4Args, repo5Args))
        self.assertEqual(butler.get('foo', {'bar': 1}), objA)
        self.assertEqual(butler.get('foo', {'bar': 2}), objD)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='four')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='five')), objE)
        del butler

        butler = dp.Butler(inputs=(repo5Args, repo4Args))
        self.assertEqual(butler.get('foo', {'bar': 1}), objE)
        self.assertEqual(butler.get('foo', {'bar': 2}), objD)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='four')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='five')), objE)
        del butler


class TestMapperInference(unittest.TestCase):
    """A test for inferring mapper in the cfg from parent cfgs"""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestMapperInference-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testSingleParent(self):
        """
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has a parent that does have a mapper specified in the cfg
        verifies that the child repo inherits the parent's mapper via inference.
        """
        repoACfg = dp.RepositoryCfg(root=os.path.join(self.testDir, 'repoA'),
                                    mapper=MapperForTestWriting,
                                    mapperArgs=None,
                                    parents=None,
                                    policy=None)
        dp.Storage.putRepositoryCfg(repoACfg, os.path.join(self.testDir, 'repoA'))

        repoBArgs = dp.RepositoryArgs(mode='rw',
                                      root=os.path.join(self.testDir, 'repoB'))
        butler = dp.Butler(inputs=os.path.join(self.testDir, 'repoA'), outputs=repoBArgs)
        self.assertIsInstance(butler._repos.outputs()[0].repo._mapper, MapperForTestWriting)

    def testMultipleParentsSameMapper(self):
        """
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has 2 parents that do have the same mapper specified in the cfg
        verifies that the child repo inherits the parent's mapper via inference.

        """
        repoACfg = dp.RepositoryCfg(root=os.path.join(self.testDir, 'repoA'),
                                    mapper=MapperForTestWriting,
                                    mapperArgs=None,
                                    parents=None,
                                    policy=None,)
        repoBCfg = dp.RepositoryCfg(root=os.path.join(self.testDir, 'repoB'),
                                    mapper=MapperForTestWriting,
                                    mapperArgs=None,
                                    parents=None,
                                    policy=None)
        dp.Storage.putRepositoryCfg(repoACfg, os.path.join(self.testDir, 'repoA'))
        dp.Storage.putRepositoryCfg(repoBCfg, os.path.join(self.testDir, 'repoB'))

        repoCArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoC'))

        butler = dp.Butler(inputs=(os.path.join(self.testDir, 'repoA'),
                                   os.path.join(self.testDir, 'repoB')),
                           outputs=repoCArgs)
        self.assertIsInstance(butler._repos.outputs()[0].repo._mapper, MapperForTestWriting)

    def testMultipleParentsDifferentMappers(self):
        """
        creates a repo that:
          a. does not have a mapper specified in the cfg
          b. has 2 parent repos that have different mappers
        verifies that the constructor raises a RuntimeError because the mapper can not be inferred.
        """
        repoACfg = dp.RepositoryCfg(root=os.path.join(self.testDir, 'repoA'),
                                    mapper=MapperForTestWriting,
                                    mapperArgs=None,
                                    parents=None,
                                    policy=None)
        repoBCfg = dp.RepositoryCfg(root=os.path.join(self.testDir, 'repoB'),
                                    mapper='lsst.daf.persistence.test.EmptyTestMapper',
                                    mapperArgs=None,
                                    parents=None,
                                    policy=None)
        dp.Storage.putRepositoryCfg(repoACfg, os.path.join(self.testDir, 'repoA'))
        dp.Storage.putRepositoryCfg(repoBCfg, os.path.join(self.testDir, 'repoB'))

        repoCArgs = dp.RepositoryArgs(mode='w',
                                      root=os.path.join(self.testDir, 'repoC'))
        self.assertRaises(RuntimeError,
                          dp.Butler,
                          inputs=(os.path.join(self.testDir, 'repoA'),
                                  os.path.join(self.testDir, 'repoB')),
                          outputs=repoCArgs)


class TestMovedRepositoryCfg(unittest.TestCase):
    """Test if a repository cfg is in-place (resides at root of the repository) and the cfg is moved, the
    deserialized cfg root should be the new location if the repository is moved.
    """

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestMovedRepositoryCfg-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test(self):
        butler = dp.Butler(outputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'a'),
                                                     mapper=MapperForTestWriting))
        del butler
        os.makedirs(os.path.join(self.testDir, 'b'))
        os.rename(os.path.join(self.testDir, 'a/repositoryCfg.yaml'),
                  os.path.join(self.testDir, 'b/repositoryCfg.yaml'))
        butler = dp.Butler(inputs=os.path.join(self.testDir, 'b'))
        self.assertEqual(list(butler._repos.inputs())[0].cfg,
                         dp.RepositoryCfg(root=os.path.join(self.testDir, 'b'),
                                          mapper=MapperForTestWriting,
                                          mapperArgs=None,
                                          parents=None,
                                          policy=None))


class TestOutputAlreadyHasParent(unittest.TestCase):

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestOutputAlreadyHasParent-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test(self):
        # create a repo where repo 'a' is a parent of repo 'b'
        butler = dp.Butler(outputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'a'),
                                                     mapper=MapperForTestWriting))
        del butler
        butler = dp.Butler(inputs=os.path.join(self.testDir, 'a'),
                           outputs=os.path.join(self.testDir, 'b'))
        self.assertEqual(len(butler._repos.inputs()), 1)
        self.assertEqual(butler._repos.inputs()[0].cfg.root,
                         os.path.join(self.testDir, 'a'))
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.outputs()[0].cfg.root,
                         os.path.join(self.testDir, 'b'))
        del butler

        # load that repo a few times, include 'a' as an input.
        for i in range(4):
            butler = dp.Butler(inputs=os.path.join(self.testDir, 'a'),
                               outputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'b'),
                                                         mode='rw'))
            self.assertEqual(len(butler._repos.inputs()), 2)
            self.assertEqual(butler._repos.inputs()[0].cfg.root,
                             os.path.join(self.testDir, 'b'))
            self.assertEqual(butler._repos.inputs()[1].cfg.root,
                             os.path.join(self.testDir, 'a'))
            self.assertEqual(len(butler._repos.outputs()), 1)
            self.assertEqual(butler._repos.outputs()[0].cfg.root,
                             os.path.join(self.testDir, 'b'))
            cfg = dp.Storage().getRepositoryCfg(os.path.join(self.testDir, 'b'))
            self.assertEqual(cfg, dp.RepositoryCfg(root=os.path.join(self.testDir, 'b'),
                                                   mapper=MapperForTestWriting,
                                                   mapperArgs=None,
                                                   parents=[os.path.join(self.testDir, 'a')],
                                                   policy=None))

        # load the repo a few times and don't explicitly list 'a' as an input
        for i in range(4):
            butler = dp.Butler(outputs=dp.RepositoryArgs(root=os.path.join(self.testDir, 'b'),
                                                         mode='rw'))
            self.assertEqual(len(butler._repos.inputs()), 2)
            self.assertEqual(butler._repos.inputs()[0].cfg.root,
                             os.path.join(self.testDir, 'b'))
            self.assertEqual(butler._repos.inputs()[1].cfg.root,
                             os.path.join(self.testDir, 'a'))
            self.assertEqual(len(butler._repos.outputs()), 1)
            self.assertEqual(butler._repos.outputs()[0].cfg.root,
                             os.path.join(self.testDir, 'b'))
            cfg = dp.Storage().getRepositoryCfg(os.path.join(self.testDir, 'b'))
            self.assertEqual(cfg, dp.RepositoryCfg(root=os.path.join(self.testDir, 'b'),
                                                   mapper=MapperForTestWriting,
                                                   mapperArgs=None,
                                                   parents=[os.path.join(self.testDir, 'a')],
                                                   policy=None))

        # load 'b' as 'write only' and don't list 'a' as an input. This should raise, because inputs must
        # match readable outputs parents.
        with self.assertRaises(RuntimeError):
            butler = dp.Butler(outputs=os.path.join(self.testDir, 'b'))

        # load 'b' as 'write only' and explicitly list 'a' as an input.
        butler = dp.Butler(inputs=os.path.join(self.testDir, 'a'),
                           outputs=os.path.join(self.testDir, 'b'))
        self.assertEqual(len(butler._repos.inputs()), 1)
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.inputs()[0].cfg.root,
                         os.path.join(self.testDir, 'a'))
        self.assertEqual(butler._repos.outputs()[0].cfg.root,
                         os.path.join(self.testDir, 'b'))
        cfg = dp.Storage().getRepositoryCfg(os.path.join(self.testDir, 'b'))


class ParentRepoTestMapper(dp.Mapper):
    def __init__(self, parentRegistry, repositoryCfg, **kwargs):
        self.parentRegistry = parentRegistry
        root = repositoryCfg.root
        if os.path.exists(os.path.join(root, 'registry.sqlite3')):
            self.registry = dp.Registry.create(os.path.join(root, 'registry.sqlite3'))
        else:
            self.registry = None

    def getRegistry(self):
        return self.registry


class TestParentRepository(unittest.TestCase):
    """A test to verify that when a parent repository is used the correct one is found."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestParentRepository-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir, True)

    def test(self):

        """Tests 1. That an sqlite registry in a parent repo is used as the
        registry in a child repo that does not have its own sqlite registry.
        2. That when a repo has a parent and grandparent and only the
        grandparent has an sqlite registry that the grandparent's registry is
        used.
        3. That when the parent and grandparent both have sqlite registries
        that the parent's sqlite registry is used."""
        # setup; create the parent repo
        def makeRegistry(location, name):
            conn = sqlite3.connect(location)
            conn.execute("CREATE TABLE {name} (real)".format(name=name))
            conn.commit()
            conn.close()

        repoADir = os.path.join(self.testDir, 'repoA')
        repoBDir = os.path.join(self.testDir, 'repoB')
        repoCDir = os.path.join(self.testDir, 'repoC')
        butler = dp.Butler(outputs={'root': repoADir, 'mapper': ParentRepoTestMapper})
        del butler
        makeRegistry(os.path.join(repoADir, 'registry.sqlite3'), 'repoA')
        # test 1:
        butler = dp.Butler(inputs=repoADir, outputs=repoBDir)
        registry = butler._repos.outputs()[0].repo._mapper.parentRegistry
        self.assertIsInstance(registry, dp.SqliteRegistry)
        tables = registry.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [('repoA', )])
        del butler
        # test 2:
        butler = dp.Butler(inputs=repoBDir, outputs=repoCDir)
        registry = butler._repos.outputs()[0].repo._mapper.parentRegistry
        self.assertIsInstance(registry, dp.SqliteRegistry)
        tables = registry.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [('repoA', )])
        del butler
        # test 3:
        makeRegistry(os.path.join(repoBDir, 'registry.sqlite3'), 'repoB')
        butler = dp.Butler(inputs=repoBDir, outputs=repoCDir)
        registry = butler._repos.outputs()[0].repo._mapper.parentRegistry
        self.assertIsInstance(registry, dp.SqliteRegistry)
        tables = registry.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [('repoB', )])


class TestOldButlerParent(unittest.TestCase):
    """A test to verify that when a parent is an old butler repo that it still gets loaded correctly,
    including mapperArgs."""

    def setUp(self):
        """Create temp test dir and create an OldButler repo at 'repoA'"""
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestOldButlerParent-")
        self.repoADir = os.path.join(self.testDir, 'repoA')
        os.makedirs(self.repoADir)
        with open(os.path.join(self.repoADir, '_mapper'), 'w') as mapperFile:
            mapperFile.write('lsst.daf.persistence.test.EmptyTestMapper')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir, True)

    def testOldButlerCfgRecordedInOutputs(self):
        """Verify that when an Old Butler is used as an input with parameters such as mapperArgs, that those
        parameters get recalled correctly (in the RepositoryCfg nested in the parents list of the output repo)
        """
        repoBDir = os.path.join(self.testDir, 'repoB')
        # create a 'repoB' with the Old Butler 'repoA' as a parent, specify mapperArgs for 'repoA', and
        # verify that the mapperArgs got passed to the repoA mapper.
        butler = dp.Butler(inputs={'root': self.repoADir,
                                   'mapperArgs': {'foo': 1}},
                           outputs={'root': repoBDir,
                                    'mapperArgs': {'foo': 1},
                                    'mode': 'rw'})
        self.assertEqual(butler._repos.inputs()[0].repo._mapper.kwargs, {'foo': 1})
        # init a Butler again, with exactly the same configuration as before.
        butler = dp.Butler(inputs={'root': self.repoADir,
                                   'mapperArgs': {'foo': 1}},
                           outputs={'root': repoBDir,
                                    'mapperArgs': {'foo': 1},
                                    'mode': 'rw'})
        self.assertEqual(butler._repos.inputs()[0].repo._mapper.kwargs, {'foo': 1})
        # try initializing the butler again with the 'repoB' output, but do not specify the mapperArgs in the
        # repoA args. this should raise a runtime error because the input speficied this way does not match
        # the cfg recorded in 'repoB'.
        with self.assertRaises(RuntimeError):
            butler = dp.Butler(inputs=self.repoADir,
                               outputs=repoBDir)
        # initializing the butler, but only specifiying the root of 'repoB' (the rest of the cfg will be
        # loaded from its `repositoryCfg` file), but fully specify the input as required.
        butler = dp.Butler(inputs={'root': self.repoADir,
                                   'mapperArgs': {'foo': 1}},
                           outputs=repoBDir)
        self.assertEqual(butler._repos.inputs()[0].repo._mapper.kwargs, {'foo': 1})
        # use 'repoB' as an input, and verify that 'repoA' is loaded, including the mapperArgs.
        butler = dp.Butler(inputs=repoBDir)
        self.assertEqual(butler._repos.inputs()[1].repo._mapper.kwargs, {'foo': 1})


class TestOldButlerParentTagging(unittest.TestCase):
    """A test to verify that when a parent is an old butler repo that its tagging gets loaded correctly"""

    def setUp(self):
        """Remove testDir from any previous runs and create an OldButler repo at repoA and an Old Butler repoB
        with a _parent symlink to repoA.
        """
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestOldButlerParentTagging-")
        self.repoADir = os.path.join(self.testDir, 'repoA')
        os.makedirs(self.repoADir)
        with open(os.path.join(self.repoADir, '_mapper'), 'w') as mapperFile:
            mapperFile.write('lsst.daf.persistence.test.MapperForTestWriting')
        self.repoBDir = os.path.join(self.testDir, 'repoB')
        os.makedirs(self.repoBDir)
        os.symlink(self.repoADir, os.path.join(self.repoBDir, '_parent'))

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir, True)

    def test(self):
        """Verify that the tags on a repository with an Old Butler repository parent are applied to that
        parent
        """
        # put objA in repoA:
        objA = tstObj('a')
        butler = dp.Butler(outputs=self.repoADir)
        butler.put(objA, 'foo', {'bar': 1})
        del butler

        # create repoB and put objB in it:
        objB = tstObj('b')
        butler = dp.Butler(inputs=self.repoADir, outputs=self.repoBDir)
        butler.put(objB, 'foo', {'bar': 2})
        del butler

        # verify that repoB can be used as an input, and objA and objB can be gotten from it:
        butler = dp.Butler(inputs=self.repoBDir)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1})), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 2})), objB)
        del butler

        # apply a tag and verify that repoB can still be used as an input, and both objA (in repoA) and objB
        # can be gotten from it:
        butler = dp.Butler(inputs={'root': self.repoBDir, 'tags': 'baz'})
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='baz')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 2}, tag='baz')), objB)
        del butler

        # create a New Butler repoC and put objC in it:
        objC = tstObj('c')
        repoCDir = os.path.join(self.testDir, 'repoC')
        butler = dp.Butler(inputs=self.repoBDir, outputs=repoCDir)
        butler.put(objC, 'foo', {'bar': 3})
        del butler

        # verify that repoC can be used as an input, and objA, objB, and objC can be gotten from it:
        butler = dp.Butler(inputs=repoCDir)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1})), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 2})), objB)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 3})), objC)
        del butler

        # apply a tag and verify that repoC can be used as an input, and objA, objB, and objC can be gotten
        # from it:
        butler = dp.Butler(inputs={'root': repoCDir, 'tags': 'baz'})
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 1}, tag='baz')), objA)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 2}, tag='baz')), objB)
        self.assertEqual(butler.get('foo', dp.DataId({'bar': 3}, tag='baz')), objC)
        del butler


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
