// -*- lsst-c++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#include <lsst/daf/base/PropertySet.h>
#include <lsst/daf/base/Citizen.h>
#include <lsst/pex/logging/Trace.h>
#include <lsst/utils/Utils.h>

#include "lsst/pex/policy/Policy.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/LogicalLocation.h"

#define BOOST_TEST_MODULE dafBase::PropertySetPersist
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;

BOOST_AUTO_TEST_SUITE(PropertySetPersistSuite)

BOOST_AUTO_TEST_CASE(PersistToBoostAndXML) {
    dafBase::PropertySet::Ptr additionalData(new dafBase::PropertySet); // empty for testing

    dafBase::PropertySet::Ptr root(new dafBase::PropertySet);
    root->add("name1", "value1");
    root->add("name2", 2);
    root->add("name2", 4);

    lsst::pex::policy::Policy::Ptr policyPtr(new lsst::pex::policy::Policy);
    dafPersist::Persistence::Ptr persist =
        dafPersist::Persistence::getPersistence(policyPtr);
    dafPersist::Storage::List storageList;

    dafPersist::LogicalLocation loc("tests/data/root.boost");
    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*root, storageList, additionalData);

    dafPersist::LogicalLocation loc2("tests/data/root.xml");
    storageList[0] = persist->getPersistStorage("XmlStorage", loc2);
    persist->persist(*root, storageList, additionalData);
}     

BOOST_AUTO_TEST_CASE(PersistDifferentTypes) {
    dafBase::PropertySet::Ptr additionalData(new dafBase::PropertySet); // empty for testing

    dafBase::PropertySet::Ptr fooProp(new dafBase::PropertySet);
    fooProp->set("foo", -1234);
    dafBase::PropertySet::Ptr fooProp2(new dafBase::PropertySet);
    fooProp2->set("foo2", 1.234e-1);
    dafBase::PropertySet::Ptr fooProp3(new dafBase::PropertySet);
    fooProp3->set("foo3", "This is a Fits string");

    lsst::pex::policy::Policy::Ptr policyPtr(new lsst::pex::policy::Policy);
    dafPersist::Persistence::Ptr persist =
        dafPersist::Persistence::getPersistence(policyPtr);
    dafPersist::Storage::List storageList;

    dafPersist::LogicalLocation loc("tests/data/foo.boost");
    dafPersist::LogicalLocation loc2("tests/data/foo2.boost");
    dafPersist::LogicalLocation loc3("tests/data/foo3.boost");

    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*fooProp, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc2);
    persist->persist(*fooProp2, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc3);
    persist->persist(*fooProp3, storageList, additionalData);
}

BOOST_AUTO_TEST_SUITE_END()
