import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy

dp = dafBase.PropertySet()
dp.addInt("foo", 3)

pol = lsst.pex.policy.Policy()

additionalData = dafBase.PropertySet()
additionalData.addInt("sliceId", 5)

loc = dafPersist.LogicalLocation("test.boost")

persistence = dafPersist.Persistence.getPersistence(pol)

storageList = dafPersist.StorageList()
storage = persistence.getPersistStorage("BoostStorage", loc)
storageList.append(storage)
persistence.persist(dp, storageList, additionalData)

storageList = dafPersist.StorageList()
storage = persistence.getRetrieveStorage("BoostStorage", loc)
storageList.append(storage)

rdp = dafBase.PropertySet.swigConvert( \
        persistence.unsafeRetrieve("PropertySet", storageList, \
            additionalData))

assert(rdp.nameCount() == 1)
assert(rdp.exists("foo"))
assert(rdp.getInt("foo") == 3)
