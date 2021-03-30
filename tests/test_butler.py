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
import tempfile

ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class ButlerTest(unittest.TestCase):
    """Test case for basic Butler operations."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='ButlerTest-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testV1withV2InitApiRaises(self):
        """Test that a RuntimeError is raised when Butler v1 init api (root,
        mapper, mapperArgs**) is used with Butler v2 init api
        (inputs, outputs)."""
        foobar = os.path.join(self.testDir, "bar")
        foobaz = os.path.join(self.testDir, "baz")
        with self.assertRaises(RuntimeError):
            dp.Butler(root=foobar, inputs=foobar)
        with self.assertRaises(RuntimeError):
            dp.Butler(mapper='lsst.obs.base.CameraMapper', inputs=foobar)
        with self.assertRaises(RuntimeError):
            dp.Butler(inputs=foobar, calibRoot=foobaz)
        with self.assertRaises(RuntimeError):
            dp.Butler(root=foobar, outputs=foobar)
        with self.assertRaises(RuntimeError):
            dp.Butler(mapper='lsst.obs.base.CameraMapper', outputs=foobar)
        with self.assertRaises(RuntimeError):
            dp.Butler(inputs=foobar, outputs=foobaz)

    def testV1RepoWithRootOnly(self):
        repoDir = os.path.join(self.testDir, 'repo')
        os.makedirs(repoDir)
        with open(os.path.join(repoDir, '_mapper'), 'w') as f:
            f.write('lsst.daf.persistence.test.EmptyTestMapper')
        butler = dp.Butler(repoDir)
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeString(self):
        # should not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': 'lsst.daf.persistence.test.EmptyTestMapper'})
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeStringV1Api(self):
        # should not raise
        butler = dp.Butler(root=self.testDir, mapper='lsst.daf.persistence.test.EmptyTestMapper')
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeClassObject(self):
        # should not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': dpTest.EmptyTestMapper})
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeClassObjectV1Api(self):
        # should not raise
        butler = dp.Butler(root=self.testDir, mapper=dpTest.EmptyTestMapper)
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeClassInstance(self):
        # should warn but not raise
        butler = dp.Butler(outputs={'root': self.testDir,
                                    'mapper': dpTest.EmptyTestMapper()})
        self.assertIsInstance(butler, dp.Butler)

    def testMapperCanBeClassInstanceV1Api(self):
        # should warn but not raise
        butler = dp.Butler(root=self.testDir, mapper=dpTest.EmptyTestMapper())
        self.assertIsInstance(butler, dp.Butler)

    def testWarning(self):
        with self.assertWarns(FutureWarning):
            current = lsst.daf.persistence.deprecation.always_warn
            lsst.daf.persistence.deprecation.always_warn = True
            dp.Butler(outputs={'root': self.testDir,
                               'mapper': 'lsst.daf.persistence.test.EmptyTestMapper'})
            lsst.daf.persistence.deprecation.always_warn = current


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
