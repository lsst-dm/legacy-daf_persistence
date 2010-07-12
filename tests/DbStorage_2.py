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


import sys
import time

import lsst.daf.persistence as dafPers
import lsst.pex.policy

if not dafPers.DbAuth.available("lsst10.ncsa.uiuc.edu", "3306"):
    print "*** WARNING*** Database authenticator unavailable.  Skipping test."
    sys.exit()

testId = long(time.time() * 1000000000L);
print testId

db = dafPers.DbStorage()
policy = lsst.pex.policy.Policy()

db.setPolicy(policy)

# Write a row

loc = dafPers.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")
db.setPersistLocation(loc)

db.startTransaction()

db.setTableForInsert("DbStorage_Test_1")
db.setColumnInt64("id", testId)
db.setColumnDouble("ra", 12345.0)
db.setColumnDouble("decl", 9876.0)
db.setColumnToNull("something")
db.setColumnString("final", "bar")
db.insertRow()

db.executeSql("""
UPDATE DbStorage_Test_1 SET ra = 9876.0, decl = 12345.0, final = 'foo'
WHERE id = %ld
""" % (testId))

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

db.condParamInt64("id", testId)
db.setQueryWhere("id = :id")

db.query()

assert db.next()

assert db.columnIsNull(0) == False
assert db.columnIsNull(1) == False
assert db.columnIsNull(2) == True
assert db.columnIsNull(3) == False
assert db.columnIsNull(4) == False
assert db.getColumnByPosInt64(1) == testId
assert db.getColumnByPosDouble(0) == 12345.0
assert db.getColumnByPosString(3) == "foo"
assert db.getColumnByPosDouble(4) == 9876.0

assert not db.next()

db.finishQuery()

db.endTransaction()
