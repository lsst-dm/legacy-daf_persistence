import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy as pexPolicy

def test1():
    """
    Test DataProperty persistence to database without policy.
    """

    dp = dafBase.DataProperty.createPropertyNode("foo")
    dp.addProperty(dafBase.DataProperty("intField", 1))
    dp.addProperty(dafBase.DataProperty("doubleField", 1.2))
    dp.addProperty(dafBase.DataProperty("varcharField", "Testing"))
    dp.addProperty(dafBase.DataProperty.createBoolDataProperty("boolField", True))
    dp.addProperty(dafBase.DataProperty("int64Field", 9876543210L))
    dp.addProperty(dafBase.DataProperty.createFloatDataProperty("floatField", \
        3.14))

    pol = pexPolicy.PolicyPtr()

    additionalData = dafBase.DataProperty.createPropertyNode("additionalData")
    additionalData.addProperty(dafBase.DataProperty( \
            "itemName", "Persistence_Test_2"))

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

def test2():
    """
    Test DataProperty persistence to database with policy mapping itemName to
    database table name.
    """

    dp = dafBase.DataProperty.createPropertyNode("foo2")
    dp.addProperty(dafBase.DataProperty("intField", 2))
    dp.addProperty(dafBase.DataProperty("doubleField", 2.3))
    dp.addProperty(dafBase.DataProperty("varcharField", "gnitseT"))
    dp.addProperty(dafBase.DataProperty.createBoolDataProperty("boolField", \
            False))
    dp.addProperty(dafBase.DataProperty("int64Field", \
            9988776655L))
    dp.addProperty(dafBase.DataProperty.createFloatDataProperty("floatField", \
            2.718))

    pol = pexPolicy.PolicyPtr()
    itemPol = pexPolicy.PolicyPtr()
    itemPol.set("TableName", "Persistence_Test_2")
    pol.set("Formatter.DataProperty.testItem", itemPol)

    additionalData = dafBase.DataProperty.createPropertyNode("additionalData")
    additionalData.addProperty(dafBase.DataProperty("itemName", "testItem"))

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

def test3():
    """
    Test DataProperty persistence to database with policy mapping itemName to
    database table name and mapping property keys to table columns.
    """

    dp = dafBase.DataProperty.createPropertyNode("foo3")
    dp.addProperty(dafBase.DataProperty("i", 3))
    dp.addProperty(dafBase.DataProperty("d", 3.4))
    dp.addProperty(dafBase.DataProperty("v", "LastOne"))
    dp.addProperty(dafBase.DataProperty.createBoolDataProperty("b", True))
    dp.addProperty(dafBase.DataProperty("I", 9998887776L))
    dp.addProperty(dafBase.DataProperty.createFloatDataProperty("f", 1.414))

    pol = pexPolicy.PolicyPtr()
    itemPol = pexPolicy.PolicyPtr()
    itemPol.set("TableName", "Persistence_Test_2")
    itemPol.add("KeyList", "floatField=f")
    itemPol.add("KeyList", "int64Field=I")
    itemPol.add("KeyList", "boolField=b")
    itemPol.add("KeyList", "varcharField=v")
    itemPol.add("KeyList", "doubleField=d")
    itemPol.add("KeyList", "intField=i")
    pol.set("Formatter.DataProperty.testItem", itemPol)

    additionalData = dafBase.DataProperty.createPropertyNode("additionalData")
    additionalData.addProperty(dafBase.DataProperty("itemName", "testItem"))

    loc = dafPersist.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = dafPersist.Persistence.getPersistence(pol)

    storageList = dafPersist.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

test1()
test2()
test3()
