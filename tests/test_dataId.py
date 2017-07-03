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


import unittest

import lsst.utils.tests
from lsst.daf.persistence import DataId


class TestDataId(unittest.TestCase):
    """A test case for the DataId class.
    """

    def test(self):
        dataId = DataId()
        self.assertEqual(dataId.tag, set())
        self.assertEqual(dataId, {})

    def testInputDictNoTags(self):
        dataId = DataId({'a': 1, 'b': 2})
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set())

    def testInputDictWithTag(self):
        # single tag
        dataId = DataId({'a': 1, 'b': 2}, 'foo')
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo']))

        # tag list
        dataId = DataId({'a': 1, 'b': 2}, ['foo', 'bar'])
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo', 'bar']))

        # tag tuple
        dataId = DataId({'a': 1, 'b': 2}, ('foo', 'bar'))
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo', 'bar']))

        # tag set
        dataId = DataId({'a': 1, 'b': 2}, set(['foo', 'bar']))
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo', 'bar']))

    def testInputDataId(self):
        initDataId = DataId({'a': 1, 'b': 2}, ['foo', 'bar'])
        dataId = DataId(initDataId)
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo', 'bar']))

        dataId = DataId(initDataId, 'baz')
        self.assertEqual(dataId, {'a': 1, 'b': 2})
        self.assertEqual(dataId.tag, set(['foo', 'bar', 'baz']))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
