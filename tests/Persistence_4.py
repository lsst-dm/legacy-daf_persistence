import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy

dp = dafBase.PropertySet()
dp.addInt("foo", 3)

pol = lsst.pex.policy.Policy()

additionalData = dafBase.PropertySet()
additionalData.addInt("sliceId", 5)

loc = dafPersist.LogicalLocation("tests/data/test4.boost")

persistence = dafPersist.Persistence.getPersistence(pol)

storageList = dafPersist.StorageList()
storage = persistence.getPersistStorage("BoostStorage", loc)
storageList.append(storage)
persistence.persist(dp, storageList, additionalData, 0, 3)
dp.setInt("foo", 4)
persistence.persist(dp, storageList, additionalData, 1, 3)
dp.setInt("foo", 5)
persistence.persist(dp, storageList, additionalData, 2, 3)

storageList = dafPersist.StorageList()
storage = persistence.getRetrieveStorage("BoostStorage", loc)
storageList.append(storage)

v = persistence.retrieveVector("PropertySet", storageList, additionalData)
assert(len(v) == 3)
result = []
for i in v:
    result.append(dafBase.PropertySet.swigConvert(i))

for i in xrange(len(result)):
    assert(result[i].nameCount() == 1)
    assert(result[i].exists("foo"))
    assert(result[i].getInt("foo") == 3 + i)
