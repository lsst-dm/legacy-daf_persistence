import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy

dp = dafBase.DataProperty("foo", 3)

pol = lsst.pex.policy.PolicyPtr()

additionalData = dafBase.DataProperty.createPropertyNode("additionalData")
additionalData.addProperty(dafBase.DataProperty("sliceId", 5))

loc = dafPersist.LogicalLocation("test.boost")

persistence = dafPersist.Persistence.getPersistence(pol)

storageList = dafPersist.StorageList()
storage = persistence.getPersistStorage("BoostStorage", loc)
storageList.append(storage)
persistence.persist(dp, storageList, additionalData)

storageList = dafPersist.StorageList()
storage = persistence.getRetrieveStorage("BoostStorage", loc)
storageList.append(storage)

rdp = dafBase.DataProperty.swigConvert( \
        persistence.unsafeRetrieve("DataProperty", storageList, \
            additionalData))

print rdp.getName(), rdp.getValueInt()
