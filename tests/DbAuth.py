#!/usr/bin/env python

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

if __name__ == '__main__':
    tests.init()
    suites = []
    suites.append(unittest.makeSuite(DbAuthTestCase))
    suites.append(unittest.makeSuite(tests.MemoryTestCase))
    tests.run(unittest.TestSuite(suites))

