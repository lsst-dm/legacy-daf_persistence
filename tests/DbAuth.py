#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

import lsst.utils.tests as tests
from lsst.daf.base import Citizen
from lsst.daf.persistence import DbAuth
from lsst.pex.policy import Policy

class DbAuthTestCase(unittest.TestCase):
    """A test case for DbAuth."""

    def testSetPolicy(self):
        pol = Policy("tests/testDbAuth.paf")
        DbAuth.setPolicy(pol)
        self.assert_(DbAuth.available("lsst10.ncsa.uiuc.edu", "3306"))
        self.assertEqual(DbAuth.authString("lsst10.ncsa.uiuc.edu", "3306"),
                "test:globular.test")
        self.assertEqual(DbAuth.username("lsst10.ncsa.uiuc.edu", "3306"),
                "test")
        self.assertEqual(DbAuth.password("lsst10.ncsa.uiuc.edu", "3306"),
                "globular.test")
        self.assert_(DbAuth.available("lsst10.ncsa.uiuc.edu", "3307"))
        self.assertEqual(DbAuth.authString("lsst10.ncsa.uiuc.edu", "3307"),
                "boris:natasha")
        self.assertEqual(DbAuth.username("lsst10.ncsa.uiuc.edu", "3307"),
                "boris")
        self.assertEqual(DbAuth.password("lsst10.ncsa.uiuc.edu", "3307"),
                "natasha")
        self.assert_(DbAuth.available("lsst9.ncsa.uiuc.edu", "3306"))
        self.assertEqual(DbAuth.authString("lsst9.ncsa.uiuc.edu", "3306"),
                "rocky:squirrel")
        self.assertEqual(DbAuth.username("lsst9.ncsa.uiuc.edu", "3306"),
                "rocky")
        self.assertEqual(DbAuth.password("lsst9.ncsa.uiuc.edu", "3306"),
                "squirrel")

class MemoryTestCase(unittest.TestCase):
    def setUp(self):
        pass
    def testLeaks(self):
        memId0 = 0
        nleakPrintMax = 20

        nleak = Citizen.census(0, memId0)
        if nleak != 0:
            print "\n%d Objects leaked:" % Citizen.census(0, memId0)

            if nleak <= nleakPrintMax:
                print Citizen.census(dafBase.cout, memId0)
            else:
                census = Citizen.census()
                print "..."
                for i in range(nleakPrintMax - 1, -1, -1):
                    print census[i].repr()

            self.fail("Leaked %d blocks" % Citizen.census(0, memId0))

def run():
    tests.init()
    suites = []
    suites.append(unittest.makeSuite(DbAuthTestCase))
    suites.append(unittest.makeSuite(tests.MemoryTestCase))
    tests.run(unittest.TestSuite(suites))

if __name__ == '__main__':
    run()
