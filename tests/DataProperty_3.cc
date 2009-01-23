// -*- lsst-c++ -*-
#include <lsst/daf/base/dafBase::PropertySet.h>
#include <lsst/daf/base/Citizen.h>
#include <lsst/pex/logging/Trace.h>
#include <lsst/utils/Utils.h>

#include "lsst/pex/policy/Policy.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/LogicalLocation.h"

#define BOOST_TEST_MODULE dafBase::PropertySetPersist
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test::tools;
namespace dafBase = lsst::daf::base;
namespace dafPers = lsst::daf::persistence;

BOOST_AUTO_TEST_SUITE(PropertySetPersistSuite)

BOOST_AUTO_TEST_CASE(PersistToBoostAndXML) {
    dafBase::PropertySet::Ptr additionalData(new dafBase::PropertySet); // empty for testing

    dafBase::PropertySet::Ptr root(new dafBase::PropertySet);
    root->add("name1", "value1");
    root->add("name2", 2);
    root->add("name2", 4);

    lsst::pex::policy::Policy::Ptr policyPtr(new lsst::pex::policy::Policy);
    dafPers::Persistence::Ptr persist =
        dafPers::Persistence::getPersistence(policyPtr);
    dafPers::Storage::List storageList;

    dafPers::LogicalLocation loc("tests/data/root.boost");
    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*root, storageList, additionalData);

    dafPers::LogicalLocation loc2("tests/data/root.xml");
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
    dafPers::Persistence::Ptr persist =
        dafPers::Persistence::getPersistence(policyPtr);
    dafPers::Storage::List storageList;

    dafPers::LogicalLocation loc("tests/data/foo.boost");
    dafPers::LogicalLocation loc2("tests/data/foo2.boost");
    dafPers::LogicalLocation loc3("tests/data/foo3.boost");

    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*fooProp, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc2);
    persist->persist(*fooProp2, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc3);
    persist->persist(*fooProp3, storageList, additionalData);
}

BOOST_AUTO_TEST_SUITE_END()
