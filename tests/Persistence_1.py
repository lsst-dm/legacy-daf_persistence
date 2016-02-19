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

import unittest
import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.pex.policy

class DbPersistence1TestCase(unittest.TestCase):

    def testPersistence1(self):
        dp = dafBase.PropertySet()
        dp.addInt("foo", 3)

        pol = lsst.pex.policy.Policy()

        additionalData = dafBase.PropertySet()
        additionalData.addInt("sliceId", 5)

        loc = dafPersist.LogicalLocation("tests/data/test.boost")

        persistence = dafPersist.Persistence.getPersistence(pol)

        storageList = dafPersist.StorageList()
        storage = persistence.getPersistStorage("BoostStorage", loc)
        storageList.append(storage)
        persistence.persist(dp, storageList, additionalData)

        storageList = dafPersist.StorageList()
        storage = persistence.getRetrieveStorage("BoostStorage", loc)
        storageList.append(storage)

        rdp = dafBase.PropertySet.swigConvert(
                persistence.unsafeRetrieve("PropertySet", storageList,
                    additionalData))

        self.assertEqual(rdp.nameCount(),1)
        self.assertTrue(rdp.exists("foo"))
        self.assertEqual(rdp.getInt("foo"), 3)

if __name__ == '__main__':
    unittest.main()
