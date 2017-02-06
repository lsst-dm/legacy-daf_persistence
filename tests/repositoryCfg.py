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
from builtins import object

import os
import shutil
import unittest
import yaml

import lsst.daf.persistence as dp
import lsst.utils.tests

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


class NullMapper(object):

    def __init__(self, **kwargs):
        pass


class TestCfgRelationship(unittest.TestCase):

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(os.path.join(ROOT, 'repositoryCfg')):
            shutil.rmtree(os.path.join(ROOT, 'repositoryCfg'))

    def testRWModes(self):
        args = dp.RepositoryArgs(mode='w', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        butler = dp.Butler(outputs=args)
        # inputs must be read-only or read-write and not write-only
        args = dp.RepositoryArgs(mode='r', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        butler = dp.Butler(inputs=args)
        args = dp.RepositoryArgs(mode='rw', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        butler = dp.Butler(inputs=args)
        args = dp.RepositoryArgs(mode='w', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        self.assertRaises(RuntimeError, dp.Butler, inputs=args)

        # outputs must be write-only or read-write and not read-only
        args = dp.RepositoryArgs(mode='w', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        butler = dp.Butler(outputs=args)
        args = dp.RepositoryArgs(mode='rw', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        butler = dp.Butler(outputs=args)
        args = dp.RepositoryArgs(mode='r', mapper=NullMapper, root=os.path.join(ROOT, 'repositoryCfg'))
        self.assertRaises(RuntimeError, dp.Butler, outputs=args)

    def testExistingParents(self):
        # parents of inputs should be added to the inputs list
        butler = dp.Butler(outputs=dp.RepositoryArgs(mode='w',
                                                     mapper=NullMapper(),
                                                     root=os.path.join(ROOT, 'repositoryCfg/a')))
        del butler
        butler = dp.Butler(inputs=os.path.join(ROOT, 'repositoryCfg/a'),
                           outputs=os.path.join(ROOT, 'repositoryCfg/b'))
        del butler
        butler = dp.Butler(inputs=os.path.join(ROOT, 'repositoryCfg/b'))
        self.assertEqual(len(butler._repos.inputs()), 2)
        # verify serach order:
        self.assertEqual(butler._repos.inputs()[0].cfg.root, os.path.join(ROOT, 'repositoryCfg/b'))
        self.assertEqual(butler._repos.inputs()[1].cfg.root, os.path.join(ROOT, 'repositoryCfg/a'))
        self.assertEqual(len(butler._repos.outputs()), 0)

        # parents of readable outputs should be added to the inputs list
        butler = dp.Butler(outputs=dp.RepositoryArgs(cfgRoot=os.path.join(ROOT, 'repositoryCfg/b'),
                                                     mode='rw'))
        self.assertEqual(len(butler._repos.inputs()), 2)
        # verify serach order:
        self.assertEqual(butler._repos.inputs()[0].cfg.root, os.path.join(ROOT, 'repositoryCfg/b'))
        self.assertEqual(butler._repos.inputs()[1].cfg.root, os.path.join(ROOT, 'repositoryCfg/a'))
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.outputs()[0].cfg.root, os.path.join(ROOT, 'repositoryCfg/b'))

        # if an output repository is write-only its parents should not be added to the inputs.
        butler = dp.Butler(outputs=os.path.join(ROOT, 'repositoryCfg/b'))
        self.assertEqual(len(butler._repos.inputs()), 0)
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.outputs()[0].cfg.root, os.path.join(ROOT, 'repositoryCfg/b'))


# "fake" repository version 0
class RepositoryCfg(yaml.YAMLObject):
    yaml_tag = u"!RepositoryCfg_v0"

    def __init__(self, mapper, mapperArgs):
        self._mapper = mapper
        self._mapperArgs = mapperArgs

    @staticmethod
    def v0Constructor(loader, node):
        d = loader.construct_mapping(node)
        return dp.RepositoryCfg(root=d['_root'], mapper=None, mapperArgs=None, parents=None, policy=None)

yaml.add_constructor(u"!RepositoryCfg_v0", RepositoryCfg.v0Constructor)


class TestCfgFileVersion(unittest.TestCase):
    """Proof-of-concept test case for a fictitious version 0 of a persisted repository cfg.
    """

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists(os.path.join(ROOT, 'repositoryCfg')):
            shutil.rmtree(os.path.join(ROOT, 'repositoryCfg'))

    def test(self):
        os.makedirs(os.path.join(ROOT, 'repositoryCfg'))
        f = open(os.path.join(ROOT, 'repositoryCfg/repositoryCfg.yaml'), 'w')
        f.write("""!RepositoryCfg_v0
                   _root: 'foo/bar'""")
        f.close()
        cfg = dp.PosixStorage.getRepositoryCfg(os.path.join(ROOT, 'repositoryCfg'))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
