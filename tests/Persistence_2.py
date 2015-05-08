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
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy as pexPolicy

if not dafPersist.DbAuth.available("lsst10.ncsa.uiuc.edu", "3306"):
    print "*** WARNING*** Database authenticator unavailable.  Skipping test."
    sys.exit()

def test1():
    """
    Test PropertySet persistence to database without policy.
    """

    dp = dafBase.PropertySet()
    dp.addInt("intField", 1)
    dp.addDouble("doubleField", 1.2)
    dp.addString("varcharField", "Testing")
    dp.addBool("boolField", True)
    dp.addLongLong("int64Field", 9876543210L)
    dp.addFloat("floatField", 3.14)

    pol = pexPolicy.Policy()

    additionalData = dafBase.PropertySet()
    additionalData.add("itemName", "Persistence_Test_2")

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp, storageList, additionalData)

def test2():
    """
    Test PropertySet persistence to database with policy mapping itemName to
    database table name.
    """

    dp = dafBase.PropertySet()
    dp.addInt("intField", 2)
    dp.addDouble("doubleField", 2.3)
    dp.addString("varcharField", "gnitseT")
    dp.addBool("boolField", False)
    dp.addLongLong("int64Field", 9988776655L)
    dp.addFloat("floatField", 2.718)

    pol = pexPolicy.Policy()
    itemPol = pexPolicy.Policy()
    itemPol.set("TableName", "Persistence_Test_2")
    pol.set("Formatter.PropertySet.testItem", itemPol)

    additionalData = dafBase.PropertySet()
    additionalData.add("itemName", "testItem")

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp, storageList, additionalData)

def test3():
    """
    Test PropertySet persistence to database with policy mapping itemName to
    database table name and mapping property keys to table columns.
    """

    dp = dafBase.PropertySet()
    dp.addInt("i", 3)
    dp.addDouble("d", 3.4)
    dp.addString("v", "LastOne")
    dp.addBool("b", True)
    dp.addLongLong("I", 9998887776L)
    dp.addFloat("f", 1.414)

    pol = pexPolicy.Policy()
    itemPol = pexPolicy.Policy()
    itemPol.set("TableName", "Persistence_Test_2")
    itemPol.add("KeyList", "floatField=f")
    itemPol.add("KeyList", "int64Field=I")
    itemPol.add("KeyList", "boolField=b")
    itemPol.add("KeyList", "varcharField=v")
    itemPol.add("KeyList", "doubleField=d")
    itemPol.add("KeyList", "intField=i")
    pol.set("Formatter.PropertySet.testItem", itemPol)

    additionalData = dafBase.PropertySet()
    additionalData.add("itemName", "testItem")

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp, storageList, additionalData)

test1()
test2()
test3()
