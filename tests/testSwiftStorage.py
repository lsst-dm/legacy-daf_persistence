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
import gc
import lsst.daf.persistence as dp
import lsst.daf.persistence.test as dpTest
from lsst.utils import getPackageDir
import lsst.utils.tests
import pickle
import shutil
import sqlite3
import sys
import uuid

packageDir = os.path.join(getPackageDir('daf_persistence'))


class TestMapper:
    pass


def setup_module(module):
    lsst.utils.tests.init()


class TestSwiftStorage(unittest.TestCase):
    """A test case for getting the relative path to parent from a symlink in PosixStorage."""

    testDir = os.path.join(packageDir, 'tests', 'TestSwiftStorage')

    def setUp(self):
        # swiftStorage requires these keys to be set up to connect to swift:
        user = os.getenv('SWIFT_USERNAME')
        if user is None:
            raise unittest.SkipTest(
                'Can not run test: SWIFT_USERNAME not in environment')
        key = os.getenv('SWIFT_PASSWORD')
        if key is None:
            raise unittest.SkipTest(
                'Can not run test: SWIFT_PASSWORD not in environment')

        serverURI = "swift://nebula.ncsa.illinois.edu:5000/v2.0/LSST"
        self.container1Name = "testContainer_{}".format(uuid.uuid4())
        self.container2Name = "testContainer_{}".format(uuid.uuid4())
        self.uri = \
            "{}/{}".format(serverURI, self.container1Name)
        self.uri2 = \
            "{}/{}".format(serverURI, self.container2Name)
        self.tearDown()
        os.makedirs(TestSwiftStorage.testDir)

    def tearDown(self):
        for uri in (self.uri, self.uri2):
            storage = dp.SwiftStorage(uri=uri)
            if storage and storage.containerExists():
                storage.deleteContainer()
            if os.path.exists(TestSwiftStorage.testDir):
                shutil.rmtree(TestSwiftStorage.testDir)

    def testParseURI(self):
        """Test that SwiftStorage._parseURI returns the proper values and
        raises the proper exceptions."""
        uri = "swift://nebula.ncsa.illinois.edu:5000/v2.0/LSST/testContainer"
        res = dp.SwiftStorage._parseURI(uri)
        self.assertEqual(res, ('swift', 'nebula.ncsa.illinois.edu:5000',
                               'v2.0', 'LSST', 'testContainer'))
        with self.assertRaises(RuntimeError):
            dp.SwiftStorage._parseURI("file://foo/bar")
        with self.assertRaises(RuntimeError):
            dp.SwiftStorage._parseURI("foo/bar")
        # URI without version identifier:
        uri = "swift://nebula.ncsa.illinois.edu:5000/LSST/testContainer01"
        with self.assertRaises(ValueError):
            dp.SwiftStorage._parseURI(uri)

    def testGetSqliteRegistry(self):
        """Verify that an sqlite3 registry in the object store is downloaded
        to a temp file that can be used to instantiate an SqliteRegistry."""
        storage = dp.SwiftStorage(uri=self.uri)
        # create a local registry sqlite3 database.
        registryFilePath = os.path.join(TestSwiftStorage.testDir,
                                        'registry.sqlite3')
        conn = sqlite3.connect(registryFilePath)
        c = conn.cursor()
        c.execute('''CREATE TABLE foo (val text)''')
        c.execute("INSERT INTO foo VALUES ('myValue')")
        conn.commit()
        conn.close()
        # put the database into the storage as 'registry.sqlite3'
        storage._putFile('registry.sqlite3', registryFilePath)
        # delete the local copy of the database, just to be sure.
        os.remove(registryFilePath)
        # download a local copy of the registry database from the storage
        f = storage.getLocalFile('registry.sqlite3')
        # and instantiate an sqlite3 registry with it.
        registry = dp.Registry.create(f.name)
        # Release the temporary file handle. The registry should have cached a
        # copy of the handle to preserve it.
        fname = f.name
        del f
        self.assertIsInstance(registry, dp.SqliteRegistry)
        # check the value we insterted into the registry as a sanity check that
        # the object was written and read successfully
        vals = registry.executeQuery(returnFields=['val'],
                                     joinClause=['foo'],
                                     whereFields=None,
                                     range=None,
                                     values=[])
        self.assertEqual(vals, [('myValue',)])
        # check that the local file still exists
        self.assertTrue(os.path.exists(fname))
        # delete the registry, this releases its handle to the sqlite3 file
        del registry
        # also delete the swiftStorage because it keeps a handle to any
        # downloaded files.
        del storage
        # run the garbage collector to be sure the temp file handle gets closed
        gc.collect()
        self.assertFalse(os.path.exists(fname))

    def testSwiftStorage(self):
        """Verify that SwiftStorage implements all the StorageInterface
        functions."""
        storage = dp.SwiftStorage(uri=self.uri)
        self.assertEqual(storage._containerName, self.container1Name)
        self.assertTrue(storage.containerExists())
        # Test containerExists by changing the container name so that it will
        # return false, and then put the name back.
        containerName = storage._containerName
        storage._containerName = "foo"
        self.assertFalse(storage.containerExists())
        storage._containerName = containerName

        testObject = dpTest.TestObject("abc")
        butlerLocation = dp.ButlerLocation(
            pythonType='lsst.daf.persistence.test.TestObject', cppType=None,
            storageName='PickleStorage', locationList='firstTestObject',
            dataId={}, mapper=None, storage=storage)

        # Test writing an object to storage
        storage.write(butlerLocation, testObject)
        # Test getting a local copy of the file in storage.
        localFile = storage.getLocalFile('firstTestObject')
        # Test reading the file in a new object using the localFile's name, as
        # well as using the localFile handle directly.
        for f in (open(localFile.name, 'r'), localFile):
            if sys.version_info.major >= 3:
                obj = pickle.load(f, encoding="latin1")
            else:
                obj = pickle.load(f)
            self.assertEqual(testObject, obj)
        # Test reading the butlerLocation, should return the object instance.
        reloadedObject = storage.read(butlerLocation)
        self.assertEqual(testObject, reloadedObject[0])
        # Test the 'exists' function with a string
        self.assertTrue(storage.exists('firstTestObject'))
        self.assertFalse(storage.exists('secondTestObject'))
        # Test the 'exists' function with a ButlerLocation. (note that most of
        # the butler location fields are unused in exists and so are set to
        # None here.)
        location = dp.ButlerLocation(
            pythonType=None, cppType=None, storageName=None,
            locationList=['firstTestObject'], dataId={}, mapper=None,
            storage=None)
        self.assertTrue(storage.exists(location))
        location = dp.ButlerLocation(
            pythonType=None, cppType=None, storageName=None,
            locationList=['secondTestObject'], dataId={}, mapper=None,
            storage=None)
        self.assertFalse(storage.exists(location))
        # Test the 'instanceSearch' function, with and without the fits header
        # extension
        self.assertEqual(storage.instanceSearch('firstTestObject'),
                         ['firstTestObject'])
        self.assertEqual(storage.instanceSearch('firstTestObject[1]'),
                         ['firstTestObject[1]'])
        self.assertEqual(storage.instanceSearch('first*Object'),
                         ['firstTestObject'])
        self.assertEqual(storage.instanceSearch('*TestObject[1]'),
                         ['firstTestObject[1]'])
        self.assertIsNone(storage.instanceSearch('secondTestObject'))
        self.assertIsNone(storage.instanceSearch('secondTestObject[1]'))
        # Test the 'search' function
        self.assertEqual(storage.search(self.uri, 'firstTestObject'),
                         ['firstTestObject'])
        # Test the copy function
        storage.copyFile('firstTestObject', 'secondTestObject')
        with self.assertRaises(RuntimeError):
            storage.copyFile('thirdTestObject', 'fourthTestObject')
        # Test locationWithRoot
        self.assertEqual(storage.locationWithRoot('firstTestObject'),
                         self.uri + '/' + 'firstTestObject')
        # Test getRepositoryCfg and putRepositoryCfg
        repositoryCfg = dp.RepositoryCfg.makeFromArgs(dp.RepositoryArgs(
            root=self.uri, mapper=TestMapper),
            parents=None)
        storage.putRepositoryCfg(repositoryCfg)
        reloadedRepoCfg = storage.getRepositoryCfg(self.uri)
        self.assertEqual(repositoryCfg, reloadedRepoCfg)
        # Test getting a non-existant RepositoryCfg
        self.assertIsNone(storage.getRepositoryCfg(self.uri2))
        # Test getting the mapper class from the repoCfg in the repo.
        mapper = dp.SwiftStorage.getMapperClass(self.uri)
        self.assertEqual(mapper, TestMapper)
        # Test for a repoCfg that resides outside its repository; it has a
        # root that is not the same as its location.
        repositoryCfg = dp.RepositoryCfg.makeFromArgs(dp.RepositoryArgs(
            root='foo/bar/baz', mapper='lsst.obs.base.CameraMapper'),
            parents=None)
        storage.putRepositoryCfg(repositoryCfg, loc=self.uri)
        reloadedRepoCfg = storage.getRepositoryCfg(self.uri)
        self.assertEqual(repositoryCfg, reloadedRepoCfg)

        storage.deleteContainer()
        self.assertFalse(storage.containerExists())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
