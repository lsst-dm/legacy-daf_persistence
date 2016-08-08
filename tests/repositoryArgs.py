# -*- coding: UTF-8 -*-
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

import lsst.daf.persistence as dp
import lsst.utils.tests
import unittest

def setup_module(module):
    lsst.utils.tests.init()

class MapperTest(dp.Mapper):
    pass


class DefaultMapper(unittest.TestCase):

    def testClassObjAndString(self):
        args1 = dp.RepositoryArgs(mapper=dp.Mapper)
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

        args1 = dp.RepositoryArgs(mapper=MapperTest)
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)

    def testInstanceAndString(self):
        args1 = dp.RepositoryArgs(mapper=dp.Mapper())
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

        args1 = dp.RepositoryArgs(mapper=MapperTest())
        args2 = dp.RepositoryArgs(mapper='lsst.daf.persistence.Mapper')
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)

    def testClassObjAndInstance(self):
        args1 = dp.RepositoryArgs(mapper=dp.Mapper)
        args2 = dp.RepositoryArgs(mapper=dp.Mapper())
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertEqual(mapper, lsst.daf.persistence.Mapper)

        args1 = dp.RepositoryArgs(mapper=MapperTest)
        args2 = dp.RepositoryArgs(mapper=dp.Mapper())
        mapper = dp.Butler._getDefaultMapper(inputs=(args1, args2))
        self.assertIsNone(mapper)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
