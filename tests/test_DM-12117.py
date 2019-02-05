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
import lsst.utils.tests
import os
import shutil
import tempfile

ROOT = os.path.abspath(os.path.dirname(__file__))


def setup_module(module):
    lsst.utils.tests.init()


class MyTestMapper(dp.Mapper):

    def __init__(self, root, *args, **kwargs):
        self.root = root
        self.args = args
        self.kwargs = kwargs


class TestDM12117(unittest.TestCase):
    """Test case for basic Butler operations."""

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix='test_DM-12117-')

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    @staticmethod
    def repoBYaml(mapperArgs):
        return """!RepositoryCfg_v1
_mapper: 'lsst.daf.persistence.test.EmptyTestMapper'
_mapperArgs: {}
_parents: ['../repoA']
_policy: null
_root: null
dirty: true
""".format(mapperArgs)

    def _verifyOldButlerParentWithArgs(self, mapperArgs):
        """Test that an Old Butler parent repo that is can be loaded by a New
        Butler output repo and that the output repo's mapper args are used by
        the OldButler repo.

        1. create an Old Butler repo
        2. create a New Butler repo with passed-in mapper args (which may be
           an empty dict)
        3. reload that New Butler repo without naming its parent as an input
        4. verify that the parent is loaded as an input
        5. verify that that the passed-in mapper args are passed to the parent
           as well as the root repo.

        Parameters
        ----------
        mapperArgs : dict or None
            Arguments to be passed to
        """
        repoAPath = os.path.join(self.testDir, 'repoA')
        repoBPath = os.path.join(self.testDir, 'repoB')
        os.makedirs(repoAPath)
        with open(os.path.join(repoAPath, '_mapper'), 'w') as f:
            f.write('lsst.daf.persistence.test.EmptyTestMapper')
        os.makedirs(repoBPath)
        with open(os.path.join(repoBPath, 'repositoryCfg.yaml'), 'w') as f:
            f.write(self.repoBYaml(mapperArgs))
        butler = dp.Butler(repoBPath)
        self.assertEqual(butler._repos.inputs()[0].repo._mapper.root, repoBPath)
        self.assertEqual(butler._repos.inputs()[1].repo._mapper.root, repoAPath)
        self.assertEqual(butler._repos.outputs()[0].repo._mapper.root, repoBPath)
        self.assertEqual(butler._repos.inputs()[0].repo._mapper.kwargs, mapperArgs)
        self.assertEqual(butler._repos.inputs()[1].repo._mapper.kwargs, mapperArgs)

    def testOldButlerParentWithoutMapperArgs(self):
        """Test that an Old Butler parent repo that is can be loaded by a New
        Butler output repo and that the output repo's mapper args are used by
        the OldButler repo.
        """
        self._verifyOldButlerParentWithArgs({})

    def testOldButlerParentWithMapperArgs(self):
        """Test that an Old Butler parent repo that is can be loaded by a New
        Butler output repo and that the output repo's mapper args are used by
        the OldButler repo.
        """
        self._verifyOldButlerParentWithArgs({'calib': 'foo'})


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
