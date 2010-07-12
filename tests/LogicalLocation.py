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
