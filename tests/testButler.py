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

import unittest
import lsst.daf.persistence as dp
import lsst.daf.persistence.test as dpTest
import lsst.utils.tests
import os
import shutil

ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class ButlerTest(unittest.TestCase):
    """Test case for basic Butler operations."""

    testDir = os.path.join(ROOT, 'ButlerTest')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testV1withV2InitApiRaises(self):
        """Test that a RuntimeError is raised when Butler v1 init api (root,
        mapper, mapperArgs**) is used with Butler v2 init api
        (inputs, outputs)."""
        with self.assertRaises(RuntimeError):
            dp.Butler(root='foo/bar', inputs='foo/bar')
        with self.assertRaises(RuntimeError):
            dp.Butler(mapper='lsst.obs.base.CameraMapper', inputs='foo/bar')
        with self.assertRaises(RuntimeError):
            dp.Butler(inputs='foo/bar', calibRoot='foo/baz')
        with self.assertRaises(RuntimeError):
            dp.Butler(root='foo/bar', outputs='foo/bar')
        with self.assertRaises(RuntimeError):
            dp.Butler(mapper='lsst.obs.base.CameraMapper', outputs='foo/bar')
        with self.assertRaises(RuntimeError):
            dp.Butler(inputs='foo/bar', outputs='foo/baz')

    def testV1RepoWithRootOnly(self):
        repoDir = os.path.join(self.testDir, 'repo')
        os.makedirs(repoDir)
        with open(os.path.join(repoDir, '_mapper'), 'w') as f:
            f.write('lsst.daf.persistence.test.EmptyTestMapper')
        butler = dp.Butler(repoDir)

    def testMapperCanBeString(self):
        # should not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': 'lsst.daf.persistence.test.EmptyTestMapper'})

    def testMapperCanBeStringV1Api(self):
        # should not raise
        butler = dp.Butler(root=self.testDir, mapper='lsst.daf.persistence.test.EmptyTestMapper')

    def testMapperCanBeClassObject(self):
        # should not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': dpTest.EmptyTestMapper})

    def testMapperCanBeClassObjectV1Api(self):
        # should not raise
        butler = dp.Butler(root=self.testDir, mapper=dpTest.EmptyTestMapper)

    def testMapperCanBeClassInstance(self):
        # should warn but not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': dpTest.EmptyTestMapper()})

    def testMapperCanBeClassInstanceV1Api(self):
        # should warn but not raise
        butler = dp.Butler(root=self.testDir, mapper=dpTest.EmptyTestMapper())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
