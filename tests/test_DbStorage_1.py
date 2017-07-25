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
from __future__ import print_function
from past.builtins import long
import unittest
import time

import lsst.utils.tests
import lsst.daf.persistence as dafPers
import lsst.pex.policy

HOST = "lsst-db.ncsa.illinois.edu"
PORT = "3306"


class DbStorage1TestCase(unittest.TestCase):

    def setUp(self):
        if not dafPers.DbAuth.available(HOST, PORT):
            raise unittest.SkipTest("Database authenticator unavailable. Skipping test.")
        self.db = dafPers.DbStorage()
        policy = lsst.pex.policy.Policy()
        self.db.setPolicy(policy)
        self.testId = long(time.time() * 1000000000)
        print(self.testId)

    def tearDown(self):
        del self.db

    def testWriteRead(self):
        loc = dafPers.LogicalLocation("mysql://{}:{}/test".format(HOST, PORT))
        db = self.db
        db.setPersistLocation(loc)

        # Write a row
        db.startTransaction()

        db.setTableForInsert("DbStorage_Test_1")
        db.setColumnInt64("id", self.testId)
        db.setColumnDouble("ra", 9.87654)
        db.setColumnDouble("decl", 1.23456)
        db.setColumnToNull("something")
        db.setColumnString("final", "foo")
        db.insertRow()

        db.endTransaction()

        # Get it back

        db.setRetrieveLocation(loc)

        db.startTransaction()

        db.setTableForQuery("DbStorage_Test_1")

        db.outColumn("decl")
        db.outColumn("id")
        db.outColumn("something")
        db.outColumn("final")
        db.outColumn("ra")

        db.condParamInt64("id", self.testId)
        db.setQueryWhere("id = :id")

        db.query()

        self.assertTrue(next(db))

        self.assertFalse(db.columnIsNull(0))
        self.assertFalse(db.columnIsNull(1))
        self.assertTrue(db.columnIsNull(2))
        self.assertFalse(db.columnIsNull(3))
        self.assertFalse(db.columnIsNull(4))
        self.assertEqual(db.getColumnByPosInt64(1), self.testId)
        self.assertEqual(db.getColumnByPosDouble(0), 1.23456)
        self.assertEqual(db.getColumnByPosString(3), "foo")
        self.assertEqual(db.getColumnByPosDouble(4), 9.87654)

        self.assertFalse(next(db))

        db.finishQuery()

        db.endTransaction()


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
