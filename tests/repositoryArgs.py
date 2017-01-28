#!/usr/bin/env python -*- coding: UTF-8 -*-

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

from future import standard_library
standard_library.install_aliases()
import lsst.daf.persistence as dp
import lsst.utils.tests
import os
import shutil
import unittest
import urllib.parse


def setup_module(module):
    lsst.utils.tests.init()


class MapperTest(dp.Mapper):
    pass

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))

class DefaultMapper(unittest.TestCase):
    """Tests for finding the default mapper for a repository given different inputs.

    Butler should allow class objects, class instances , and importable strings to be passed in, and treat
    them as equivalent.

    Butler will find a default mapper only if all the inputs to the butler use the same mapper. If there are
    inputs with different mappers then the butler will not assume a default mapper and _getDefaultMapper
    will return None."""

    def setUp(self):
        self.testDir = os.path.join(ROOT, 'DefaultMapper')
        self.tearDown()
        os.makedirs(self.testDir)

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def _setup(self, mapper1, mapper2):
        repo1Root = os.path.join(self.testDir, 'repo1')
        repo2Root = os.path.join(self.testDir, 'repo2')
        butler = dp.Butler(outputs=(dp.RepositoryArgs(root=repo1Root, mapper=mapper1)))
        del butler
        butler = dp.Butler(outputs=(dp.RepositoryArgs(root=repo2Root, mapper=mapper2)))
        del butler
        butler = dp.Butler(inputs=(repo1Root, repo2Root))
        return butler

    def testClassObjAndMatchingString(self):
        """Pass a class object and a string that evaluates to the same object, and verify a default mapper
        can be found."""
        butler = self._setup(dp.Mapper, 'lsst.daf.persistence.Mapper')
        self.assertEqual(butler._getDefaultMapper(), lsst.daf.persistence.Mapper)

    def testClassObjAndNotMatchingString(self):
        """Pass a class object and a non-matching string, and verify a default mapper can not be found."""
        butler = self._setup(MapperTest, 'lsst.daf.persistence.Mapper')
        self.assertIsNone(butler._getDefaultMapper())

    def testInstanceAndMatchingString(self):
        """Pass a class instance and a string that evaluates to the same object, and verify a default mapper
        can be found."""
        butler = self._setup(dp.Mapper(), 'lsst.daf.persistence.Mapper')
        self.assertEqual(butler._getDefaultMapper(), lsst.daf.persistence.Mapper)

    def testInstanceAndNotMatchingString(self):
        """Pass a class instance and a non-matching string, and verify a default mapper can not be found."""
        butler = self._setup(MapperTest(), 'lsst.daf.persistence.Mapper')
        self.assertIsNone(butler._getDefaultMapper())

    def testClassObjAndMatchingInstance(self):
        """Pass a class object and a class instance of the same type, and verify a default mapper can be
        found."""
        butler = self._setup(dp.Mapper, dp.Mapper())
        self.assertEqual(butler._getDefaultMapper(), lsst.daf.persistence.Mapper)


    def testClassObjAndNotMatchingInstance(self):
        """Pass a class object and a class instance of a different type, and verify a default mapper can not
        be found."""
        butler = self._setup(MapperTest, dp.Mapper())
        self.assertIsNone(butler._getDefaultMapper())


class ParseRootURI(unittest.TestCase):
    """Verify that supported root URI schemas are identified and build the correct Storage.
    """
    def setUp(self):
        self.testDir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ParseRootURI')
        self.tearDown()

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testAbsoluteFilePathWithSchema(self):
        """Test writing & reading an absolute path that begins with 'file://"""
        uri = urllib.parse.urljoin('file://', self.testDir)
        args = dp.RepositoryArgs(root=uri, mapper='lsst.daf.persistence.Mapper')
        butler = dp.Butler(outputs=args)
        self.assertTrue(os.path.exists(os.path.join(self.testDir, 'repositoryCfg.yaml')))

        butler2 = dp.Butler(inputs=uri)

        try:
            butler2 = dp.Butler(inputs=uri)
        except RuntimeError:
            self.fail("Butler init raised a runtime error loading input %s" % uri)

    def testAbsoluteFilePathWithoutSchema(self):
        """Test writing and reading an absolute path that begins with '/' (not 'file://')"""
        uri = self.testDir
        args = dp.RepositoryArgs(root=uri, mapper='lsst.daf.persistence.Mapper')
        butler = dp.Butler(outputs=args)
        self.assertTrue(os.path.exists(os.path.join(uri, 'repositoryCfg.yaml')))

        try:
            butler2 = dp.Butler(inputs=uri)
        except RuntimeError:
            self.fail("Butler init raised a runtime error loading input %s" % uri)

    def testRelativeFilePath(self):
        """Test writing & reading a relative filepath.

        Relative filepaths can not start with 'file://' so there will be no relative file path test starting
        with the 'file' schema."""
        uri = os.path.relpath(self.testDir)
        args = dp.RepositoryArgs(root=uri, mapper='lsst.daf.persistence.Mapper')
        butler = dp.Butler(outputs=args)
        self.assertTrue(self.testDir, 'repositoryCfg.yaml')

        try:
            butler2 = dp.Butler(inputs=uri)
        except RuntimeError:
            self.fail("Butler init raised a runtime error loading input %s" % uri)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
