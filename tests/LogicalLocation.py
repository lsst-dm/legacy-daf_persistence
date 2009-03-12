#!/usr/bin/env python

import unittest

from lsst.daf.persistence import LogicalLocation
from lsst.daf.base import PropertySet

import lsst.pex.logging
lsst.pex.logging.Trace_setVerbosity("daf.persistence.LogicalLocation", 10)

class LogicalLocationTestCase(unittest.TestCase):
    """A test case for LogicalLocation."""

    def testSubst(self):
        ad = PropertySet()
        ad.set("foo", "bar")
        ad.setInt("x", 3)
        LogicalLocation.setLocationMap(ad)
        l = LogicalLocation("%(foo)xx")
        self.assertEqual(l.locString(), "barxx")
        l = LogicalLocation("%(x)foo")
        self.assertEqual(l.locString(), "3foo")
        l = LogicalLocation("yy%04d(x)yy")
        self.assertEqual(l.locString(), "yy0003yy")

        ad2 = PropertySet()
        ad2.set("foo", "baz")
        ad2.setInt("y", 2009)
        l = LogicalLocation("%(foo)%(x)%(y)", ad2)
        self.assertEqual(l.locString(), "bar32009")
        LogicalLocation.setLocationMap(PropertySet())
        l = LogicalLocation("%(foo)%3d(y)", ad2)
        self.assertEqual(l.locString(), "baz2009")

if __name__ == '__main__':
    unittest.main()
