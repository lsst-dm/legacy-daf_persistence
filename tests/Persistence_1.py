import lsst.mwi.data as DATA
import lsst.mwi.persistence as PER
import lsst.mwi.policy as POL

dp = DATA.SupportFactory.createLeafProperty("foo", 3)

pol = POL.PolicyPtr()

additionalData = DATA.SupportFactory.createPropertyNode("additionalData")
additionalData.addProperty(DATA.SupportFactory.createLeafProperty( \
        "sliceId", 5))

loc = PER.LogicalLocation("test.boost")

persistence = PER.Persistence.getPersistence(pol)

storageList = PER.StorageList()
storage = persistence.getPersistStorage("BoostStorage", loc)
storageList.append(storage)
persistence.persist(dp.get(), storageList, additionalData)

storageList = PER.StorageList()
storage = persistence.getRetrieveStorage("BoostStorage", loc)
storageList.append(storage)

rdp = DATA.DataProperty.swigConvert( \
        persistence.unsafeRetrieve("DataProperty", storageList, \
            additionalData))

print rdp.getName(), rdp.getValueInt()
