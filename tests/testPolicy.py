#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2015 LSST Corporation.
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

from lsst.daf.persistence import Policy
import lsst.utils.tests as utilsTests


class PolicyTestCase(unittest.TestCase):
    """A test case for the butler policy"""

    def setUp(self):
        self.policy = Policy(
            data={'body': {'job': {'position': 'Developer', 'company': 'Microsoft'}, 'name': 'John'},
                           'error': False})

    def testBasic(self):
        p = Policy()
        p['a'] = {1:2}
        self.assertEqual(p, {'a':{1:2}})
        p.update({'a':{3:4}})
        self.assertEqual(p, {'a':{1:2, 3:4}})

    def testUpdateWithDict(self):
        self.policy.update({'body': {'job': {'position': 'Manager'}}})
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Manager', 'company': 'Microsoft'}, 'name': 'John'})
        self.policy.update({'body': {'name': {'first':'John', 'last':'Smith'}}})
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Manager', 'company': 'Microsoft'},
                          'name': {'first':'John', 'last':'Smith'}})

    def testUpdateWithPolicy(self):
        p1 = Policy(data={'body': {'job': {'position': 'Manager'}}})
        self.policy.update(p1)
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Manager', 'company': 'Microsoft'}, 'name': 'John'})

    def testGet(self):
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Developer', 'company': 'Microsoft'}, 'name': 'John'})
        self.assertEqual(self.policy['body.job'], {'position': 'Developer', 'company': 'Microsoft'})
        self.assertEqual(self.policy['body.job.position'], 'Developer')

    def testSet(self):
        # change an item
        self.policy['body.job.company'] = 'SLAC'
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Developer', 'company': 'SLAC'}, 'name': 'John'})
        # add an item
        self.policy['body.job.salary'] = 100000
        self.assertEqual(self.policy['body'],
                         {'job': {'position': 'Developer', 'company': 'SLAC', 'salary': 100000},
                          'name': 'John'})

    def testNames(self):
        names = self.policy.names()
        names.sort()
        expectedNames = ['body', 'body.job', 'body.job.position', 'body.job.company', 'body.name', 'error']
        expectedNames.sort()
        self.assertEqual(names, expectedNames)

    def testCopyPolicy(self):
        pol = Policy(policy=self.policy)
        self.assertEqual(pol, self.policy)

    def testGetPolicy(self):
        policy = self.policy['body']
        self.assertEqual(policy, {'job': {'position': 'Developer', 'company': 'Microsoft'}, 'name': 'John'})
        self.assertEqual(policy['job.position'], 'Developer') # note: verifies dot naming
        self.assertTrue(isinstance(policy, Policy))

    def testDotsInBraces(self):
        self.assertEqual(self.policy['body.job.company'], 'Microsoft')

    def testMerge(self):
        a = Policy()
        b = Policy()
        a['a.b.c'] = 1
        b['a.b.c'] = 2
        b['a.b.d'] = 3
        a.merge(b)
        self.assertEqual(a['a.b.c'], 1)
        self.assertEqual(a['a.b.d'], 3)

        # b should remain unchanged:
        self.assertEqual(b['a.b.c'], 2)
        self.assertEqual(b['a.b.d'], 3)

    def testOpenDefaultPolicy(self):
        policy = Policy(defaultInitData=('daf_persistence', 'testPolicy.yaml', 'tests'))
        self.assertEqual(policy['exposures.raw.template'], 'foo/bar/baz.fits.gz')

    def testDumpLoad(self):
        self.policy.dumpToFile('testDumpFile.yaml')
        loadedPolicy = Policy(filePath='testDumpFile.yaml')
        self.assertEqual(self.policy, loadedPolicy)
        os.remove('testDumpFile.yaml')

    def testNonExistantPolicyAtPath(self):
        self.assertRaises(IOError, Policy, filePath="c:/policy.yaml")
        self.assertRaises(IOError, Policy, filePath="c:/policy.paf")
        self.assertRaises(IOError, Policy, filePath="c:/policy")

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(PolicyTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
