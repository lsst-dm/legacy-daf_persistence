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

import os
import shutil
import unittest

import lsst.daf.persistence as dp
import lsst.utils.tests

class NullMapper:
    def __init__(self):
        pass

class TestCfgRelationship(unittest.TestCase):

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        if os.path.exists('tests/repository'):
            shutil.rmtree('tests/repository')

    def testRWModes(self):
        # inputs must be read-only or read-write and not write-only
        args = dp.RepositoryArgs(mode='r', mapper=NullMapper, root='tests/repository')
        butler = dp.Butler(inputs=args)
        args = dp.RepositoryArgs(mode='rw', mapper=NullMapper, root='tests/repository')
        butler = dp.Butler(inputs=args)
        args = dp.RepositoryArgs(mode='w', mapper=NullMapper, root='tests/repository')
        self.assertRaises(RuntimeError, dp.Butler, inputs=args)

        # outputs must be write-only or read-write and not read-only
        args = dp.RepositoryArgs(mode='w', mapper=NullMapper, root='tests/repository')
        butler = dp.Butler(outputs=args)
        args = dp.RepositoryArgs(mode='rw', mapper=NullMapper, root='tests/repository')
        butler = dp.Butler(outputs=args)
        args = dp.RepositoryArgs(mode='r', mapper=NullMapper, root='tests/repository')
        self.assertRaises(RuntimeError, dp.Butler, outputs=args)


    def testExistingParents(self):
        # parents of inputs should be added to the inputs list
        butler = dp.Butler(outputs=dp.RepositoryArgs(mode='w', 
                                                     mapper=NullMapper(), 
                                                     root='tests/repository/a'))
        del butler
        butler = dp.Butler(inputs='tests/repository/a', outputs='tests/repository/b')
        del butler
        butler = dp.Butler(inputs='tests/repository/b')
        self.assertEqual(len(butler._repos.inputs()), 2)
        # verify serach order:
        self.assertEqual(butler._repos.inputs()[0].cfg.root, 'tests/repository/b')
        self.assertEqual(butler._repos.inputs()[1].cfg.root, 'tests/repository/a')
        self.assertEqual(len(butler._repos.outputs()), 0)

        # parents of readable outputs should be added to the inputs list
        butler = dp.Butler(outputs=dp.RepositoryArgs(cfgRoot='tests/repository/b', mode='rw'))
        self.assertEqual(len(butler._repos.inputs()), 2)
        # verify serach order:
        self.assertEqual(butler._repos.inputs()[0].cfg.root, 'tests/repository/b')
        self.assertEqual(butler._repos.inputs()[1].cfg.root, 'tests/repository/a')
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.outputs()[0].cfg.root, 'tests/repository/b')

        # if an output repository is write-only its parents should not be added to the inputs.
        butler = dp.Butler(outputs='tests/repository/b')
        self.assertEqual(len(butler._repos.inputs()), 0)
        self.assertEqual(len(butler._repos.outputs()), 1)
        self.assertEqual(butler._repos.outputs()[0].cfg.root, 'tests/repository/b')


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass

if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
