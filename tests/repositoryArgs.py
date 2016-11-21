#!/usr/bin/env python -*- coding: UTF-8 -*-

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

import lsst.daf.persistence as dp
import lsst.utils.tests
import unittest


def setup_module(module):
    lsst.utils.tests.init()


class MapperTest(dp.Mapper):
    pass


class DefaultMapper(unittest.TestCase):
    """Tests for finding the default mapper for a repository given different inputs.

    Butler should allow class objects, class instances , and importable strings to be passed in, and treat
    them as equivalent.

    Butler will find a default mapper only if all the inputs to the butler use the same mapper. If there are
    inputs with different mappers then the butler will not assume a default mapper and _getDefaultMapper
    will return None."""

    def testClassObjAndMatchingString(self):
        """Pass a class object and a string that evaluates to the same object, and verify a default mapper
        can be found."""
        args1 = dp.RepositoryArgs(mapper=dp.Mapper)
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

    def testClassObjAndNotMatchingString(self):
        """Pass a class object and a non-matching string, and verify a default mapper can not be found."""
        args1 = dp.RepositoryArgs(mapper=MapperTest)
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)

    def testInstanceAndMatchingString(self):
        """Pass a class instance and a string that evaluates to the same object, and verify a default mapper
        can be found."""
        args1 = dp.RepositoryArgs(mapper=dp.Mapper())
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

    def testInstanceAndNotMatchingString(self):
        """Pass a class instance and a non-matching string, and verify a default mapper can not be found."""
        args1 = dp.RepositoryArgs(mapper=MapperTest())
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)

    def testClassObjAndMatchingInstance(self):
        """Pass a class object and a class instance of the same type, and verify a default mapper can be
        found."""
        args1 = dp.RepositoryArgs(mapper=dp.Mapper)
        args2 = dp.RepositoryArgs(mapper=dp.Mapper())
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

    def testClassObjAndNotMatchingInstance(self):
        """Pass a class object and a class instance of a different type, and verify a default mapper can not
        be found."""
        args1 = dp.RepositoryArgs(mapper=MapperTest)
        args2 = dp.RepositoryArgs(mapper=dp.Mapper())
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
