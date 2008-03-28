import lsst.mwi.data as DATA
import lsst.mwi.persistence as PER
import lsst.mwi.policy as POL

def test1():
    """
    Test DataProperty persistence to database without policy.
    """

    dp = DATA.SupportFactory.createPropertyNode("foo")
    dp.addProperty(DATA.SupportFactory.createLeafProperty("intField", 1))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("doubleField", 1.2))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("varcharField", \
        "Testing"))
    dp.addProperty(DATA.DataProperty.createBoolDataProperty("boolField", True))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("int64Field", \
        9876543210L))
    dp.addProperty(DATA.DataProperty.createFloatDataProperty("floatField", \
        3.14))

    pol = POL.PolicyPtr()

    additionalData = DATA.SupportFactory.createPropertyNode("additionalData")
    additionalData.addProperty(DATA.SupportFactory.createLeafProperty( \
            "itemName", "Persistence_Test_2"))

    loc = PER.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = PER.Persistence.getPersistence(pol)

    storageList = PER.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

def test2():
    """
    Test DataProperty persistence to database with policy mapping itemName to
    database table name.
    """

    dp = DATA.SupportFactory.createPropertyNode("foo2")
    dp.addProperty(DATA.SupportFactory.createLeafProperty("intField", 2))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("doubleField", 2.3))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("varcharField", \
            "gnitseT"))
    dp.addProperty(DATA.DataProperty.createBoolDataProperty("boolField", \
            False))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("int64Field", \
            9988776655L))
    dp.addProperty(DATA.DataProperty.createFloatDataProperty("floatField", \
            2.718))

    pol = POL.PolicyPtr()
    itemPol = POL.PolicyPtr()
    itemPol.set("TableName", "Persistence_Test_2")
    pol.set("Formatter.DataProperty.testItem", itemPol)

    additionalData = DATA.SupportFactory.createPropertyNode("additionalData")
    additionalData.addProperty(DATA.SupportFactory.createLeafProperty( \
            "itemName", "testItem"))

    loc = PER.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = PER.Persistence.getPersistence(pol)

    storageList = PER.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

def test3():
    """
    Test DataProperty persistence to database with policy mapping itemName to
    database table name and mapping property keys to table columns.
    """

    dp = DATA.SupportFactory.createPropertyNode("foo3")
    dp.addProperty(DATA.SupportFactory.createLeafProperty("i", 3))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("d", 3.4))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("v", "LastOne"))
    dp.addProperty(DATA.DataProperty.createBoolDataProperty("b", True))
    dp.addProperty(DATA.SupportFactory.createLeafProperty("I", 9998887776L))
    dp.addProperty(DATA.DataProperty.createFloatDataProperty("f", 1.414))

    pol = POL.PolicyPtr()
    itemPol = POL.PolicyPtr()
    itemPol.set("TableName", "Persistence_Test_2")
    itemPol.add("KeyList", "floatField=f")
    itemPol.add("KeyList", "int64Field=I")
    itemPol.add("KeyList", "boolField=b")
    itemPol.add("KeyList", "varcharField=v")
    itemPol.add("KeyList", "doubleField=d")
    itemPol.add("KeyList", "intField=i")
    pol.set("Formatter.DataProperty.testItem", itemPol)

    additionalData = DATA.SupportFactory.createPropertyNode("additionalData")
    additionalData.addProperty(DATA.SupportFactory.createLeafProperty( \
            "itemName", "testItem"))

    loc = PER.LogicalLocation("mysql://lsst10.ncsa.uiuc.edu:3306/test")

    persistence = PER.Persistence.getPersistence(pol)

    storageList = PER.StorageList()
    storage = persistence.getPersistStorage("DbStorage", loc)
    storageList.append(storage)
    persistence.persist(dp.get(), storageList, additionalData)

test1()
test2()
test3()
