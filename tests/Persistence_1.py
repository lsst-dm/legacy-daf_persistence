import lsst.daf.base as dafBase
import lsst.daf.data as dafData
import lsst.daf.persistence as dafPersist
import lsst.pex.policy

dp = dafData.SupportFactory.createLeafProperty("foo", 3)

pol = lsst.pex.policy.PolicyPtr()

additionalData = dafData.SupportFactory.createPropertyNode("additionalData")
additionalData.addProperty(dafData.SupportFactory.createLeafProperty( \
        "sliceId", 5))

loc = dafPersist.LogicalLocation("test.boost")

persistence = dafPersist.Persistence.getPersistence(pol)

storageList = dafPersist.StorageList()
storage = persistence.getPersistStorage("BoostStorage", loc)
storageList.append(storage)
persistence.persist(dp.get(), storageList, additionalData)

storageList = dafPersist.StorageList()
storage = persistence.getRetrieveStorage("BoostStorage", loc)
storageList.append(storage)

rdp = dafBase.DataProperty.swigConvert( \
        persistence.unsafeRetrieve("DataProperty", storageList, \
            additionalData))

print rdp.getName(), rdp.getValueInt()
