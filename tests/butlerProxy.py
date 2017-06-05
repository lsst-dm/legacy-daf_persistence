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

from __future__ import absolute_import

import os
import shutil
import unittest
import lsst.utils.tests

import lsst.daf.persistence as dafPersist
import lsst.daf.base as dafBase

from testLib import isValidDateTime, TypeWithProxy, TypeWithoutProxy

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


class ButlerProxyTestCase(unittest.TestCase):
    """A test case for the data butler finding a Mapper in a root"""

    inputDir = os.path.join(ROOT, 'root')
    outputDir = os.path.join(ROOT, 'ButlerProxyTestCase')

    def removeTestDir(self):
        if os.path.exists(self.outputDir):
            shutil.rmtree(self.outputDir)

    def setUp(self):
        self.removeTestDir()
        self.butler = dafPersist.Butler(self.inputDir,
                                        outPath=os.path.join(self.outputDir, "proxyOut"))

    def tearDown(self):
        del self.butler
        self.removeTestDir()

    def testCheckProxy(self):
        """Attempt to cycle a DateTime object through the butler
        """
        dt = dafBase.DateTime.now()
        self.butler.put(dt, "dt", ccd=1)

        # The next two types should not be castable to a DateTime object
        # when the proxy is passed to a function
        p1 = TypeWithoutProxy()
        self.butler.put(p1, "p1", ccd=1)

        p2 = TypeWithProxy()
        self.butler.put(p2, "p2", ccd=1)

        # First try with immediate read, this should obviously work
        dt = self.butler.get("dt", ccd=1, immediate=True)
        self.assertIsInstance(dt, dafBase.DateTime)
        self.assertTrue(isValidDateTime(dt))

        # Now try again with lazy read
        dt = self.butler.get("dt", ccd=1, immediate=False)
        self.assertIsInstance(dt, dafPersist.readProxy.ReadProxy)
        self.assertTrue(isValidDateTime(dt))

        # Now try with a type for which a proxy is not registered
        p1 = self.butler.get("p1", ccd=1, immediate=True)
        self.assertIsInstance(p1, TypeWithoutProxy)
        with self.assertRaises(TypeError):
            isValidDateTime(p1)

        # Now try with a type for which a proxy was registered but
        # that cannot convert to a DateTime
        p2 = self.butler.get("p2", ccd=1, immediate=True)
        self.assertIsInstance(p2, TypeWithProxy)
        with self.assertRaises(TypeError):
            isValidDateTime(p2)

        # Finally try with an invalid DateTime
        dt = dafBase.DateTime()
        self.butler.put(dt, "dt", ccd=1)

        dt = self.butler.get("dt", ccd=1, immediate=False)
        self.assertIsInstance(dt, dafPersist.readProxy.ReadProxy)
        self.assertFalse(isValidDateTime(dt))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
