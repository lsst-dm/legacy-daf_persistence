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

import lsst.daf.persistence as dp
import lsst.utils.tests


def setup_module(module):
    lsst.utils.tests.init()


class TestUtils(unittest.TestCase):
    """Test case for functions in the utils file."""

    # Many of the functions in lsst.daf.persistence.utils are not represented here.
    # https://jira.lsstcorp.org/browse/DM-8236 was created to finish the tests in this file.

    def testSequencify(self):
        self.assertEqual((1, ), dp.sequencify(1))
        self.assertEqual(('abc', ), dp.sequencify('abc'))
        self.assertEqual((1, ), dp.sequencify((1,)))
        self.assertEqual([1], dp.sequencify([1]))
        self.assertEqual(set([1]), dp.sequencify(set([1])))
        self.assertEqual(('a', 'b', 'c'), dp.sequencify({'a': 1, 'b': 2, 'c': 3}))
        self.assertNotEqual(('b', 'c', 'a'), dp.sequencify({'a': 1, 'b': 2, 'c': 3}))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
