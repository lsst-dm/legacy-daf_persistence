#!/usr/bin/env python

import unittest

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

if __name__ == '__main__':
    unittest.main()
