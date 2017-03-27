/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

/**
 * \file FitsStorage_1.cc
 *
 * This test tests the FitsStorage class.
 */
#include <sstream>
#include <string>
#include <sys/time.h>
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/FitsStorage.h"
#include "lsst/pex/exceptions.h"

#define BOOST_TEST_MODULE FitsStorage_1
#define BOOST_TEST_DYN_LINK
#include "boost/test/unit_test.hpp"

namespace test = boost::test_tools;
namespace dafPersist = lsst::daf::persistence;

BOOST_AUTO_TEST_SUITE(FitsStorageSuite)

BOOST_AUTO_TEST_CASE(FitsStorageRetrieveTest) {
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    for (int i = 0; i <= 4; ++i) {
        std::string loc = (boost::format("tests/data/mef.fits[%d]") % i).str();
        dafPersist::LogicalLocation pathLoc(loc);
        dafPersist::Persistence::Ptr persist =
            dafPersist::Persistence::getPersistence(policy);
        BOOST_CHECK_NE(persist, dafPersist::Persistence::Ptr());
        PTR(dafPersist::FormatterStorage) storage =
            persist->getRetrieveStorage("FitsStorage", pathLoc);
        BOOST_CHECK_NE(storage, PTR(dafPersist::FormatterStorage)());
        dafPersist::FitsStorage* fits =
            dynamic_cast<dafPersist::FitsStorage*>(storage.get());
        dafPersist::FitsStorage* null = 0;
        BOOST_CHECK_NE(fits, null);
        BOOST_CHECK_EQUAL(fits->getPath(), loc);
        BOOST_CHECK_EQUAL(fits->getHdu(), i);
    }
}

BOOST_AUTO_TEST_CASE(FitsStoragePersistTest) {
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    dafPersist::LogicalLocation pathLoc("tests/data/mef.fits[2]");
    dafPersist::Persistence::Ptr persist =
        dafPersist::Persistence::getPersistence(policy);
    BOOST_CHECK_NE(persist, dafPersist::Persistence::Ptr());
    PTR(dafPersist::FormatterStorage) storage =
        persist->getPersistStorage("FitsStorage", pathLoc);
    BOOST_CHECK_NE(storage, PTR(dafPersist::FormatterStorage)());
    dafPersist::FitsStorage* fits =
        dynamic_cast<dafPersist::FitsStorage*>(storage.get());
    dafPersist::FitsStorage* null = 0;
    BOOST_CHECK_NE(fits, null);
    BOOST_CHECK_EQUAL(fits->getPath(), "tests/data/mef.fits[2]");
    // Persistence ignores HDU
    BOOST_CHECK_EQUAL(fits->getHdu(), INT_MIN);
}

BOOST_AUTO_TEST_SUITE_END()
