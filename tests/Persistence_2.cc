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
 
/**
 * \file Persistence_2.cc
 *
 * This test checks that objects can be persisted and retrieved from multiple
 * Storages.
 */

#include <sstream>
#include <sys/time.h>
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"

#define BOOST_TEST_MODULE Persistence_2
#define BOOST_TEST_DYN_LINK
#include "boost/test/unit_test.hpp"

namespace test = boost::test_tools;
namespace dafBase = lsst::daf::base;
namespace dafPersist = lsst::daf::persistence;

// A small Persistable.

// Forward declaration may be needed with gcc 4+.
class MyFormatter;

class MyPersistable : public dafBase::Persistable {
public:
    typedef boost::shared_ptr<MyPersistable> Ptr;
    MyPersistable(double ra = 0.0, double decl = 0.0) : _ra(ra), _decl(decl) { };
    double getRa(void) const { return _ra; };
    double getDecl(void) const { return _decl; };
private:
    LSST_PERSIST_FORMATTER(MyFormatter);
    double _ra;
    double _decl;
};

// A small Formatter.
class MyFormatter : public dafPersist::Formatter {
public:
    MyFormatter(void) : dafPersist::Formatter(typeid(*this)) { };
    virtual void write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData);
    virtual dafBase::Persistable* read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData);
    virtual void update(dafBase::Persistable* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData);
    template <class Archive> static void delegateSerialize(Archive& ar, unsigned int const version, dafBase::Persistable* persistable);
private:
    static dafPersist::Formatter::Ptr createInstance(lsst::pex::policy::Policy::Ptr policy);
    static dafPersist::FormatterRegistration registration;
};

// Include this file when implementing a Formatter.
#include "lsst/daf/persistence/FormatterImpl.h"

// Register the formatter factory function.
dafPersist::FormatterRegistration MyFormatter::registration("MyPersistable", typeid(MyPersistable), createInstance);

// The definition of the factory function.
dafPersist::Formatter::Ptr MyFormatter::createInstance(lsst::pex::policy::Policy::Ptr policy) {
    return dafPersist::Formatter::Ptr(new MyFormatter);
}

// Persistence for MyPersistables.
// Supports BoostStorage and DbStorage.
void MyFormatter::write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    BOOST_CHECK_MESSAGE(persistable != 0, "Persisting null");
    BOOST_CHECK_MESSAGE(storage, "No Storage provided");
    long long testId = additionalData->get<long long>("visitId");
    MyPersistable const* mp = dynamic_cast<MyPersistable const*>(persistable);
    BOOST_CHECK_MESSAGE(mp != 0, "Persisting non-MyPersistable");
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        dafPersist::BoostStorage* boost = dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(boost != 0, "Didn't get BoostStorage");
        boost->getOArchive() & mp->_ra;
        return;
    }
    else if (typeid(*storage) == typeid(dafPersist::DbStorage)) {
        dafPersist::DbStorage* db = dynamic_cast<dafPersist::DbStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(db != 0, "Didn't get DbStorage");
        db->setTableForInsert("DbStorage_Test_2");
        db->setColumn<long long>("id", testId);
        db->setColumn<double>("decl", mp->_decl);
        db->insertRow();
        return;
    }
    BOOST_FAIL("Didn't recognize Storage type");

}

// Retrieval for MyPersistables.
// Supports BoostStorage, DbStorage, and DbTsvStorage.
dafBase::Persistable* MyFormatter::read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    MyPersistable* mp = new MyPersistable;

    long long testId = additionalData->get<long long>("visitId");
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        dafPersist::BoostStorage* boost = dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(boost != 0, "Didn't get BoostStorage");
        boost->getIArchive() & mp->_ra;
        return mp;
    }
    else if (typeid(*storage) == typeid(dafPersist::DbStorage)) {
        dafPersist::DbStorage* db = dynamic_cast<dafPersist::DbStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(db != 0, "Didn't get DbStorage");
        db->setTableForQuery("DbStorage_Test_2");
        db->condParam<long long>("id", testId);
        db->setQueryWhere("id = :id");
        db->outParam("decl", &(mp->_decl));

        db->query();

        BOOST_CHECK_MESSAGE(db->next() == true, "Failed to get row");
        BOOST_CHECK_MESSAGE(db->columnIsNull(0) == false, "Null column 0");
        BOOST_CHECK_MESSAGE(db->next() == false, "Got more than one row");

        db->finishQuery();
        return mp;
    }
    BOOST_FAIL("Didn't recognize Storage type");
    return mp;
}

void MyFormatter::update(dafBase::Persistable* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    long long testId = additionalData->get<long long>("visitId");
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        // BoostStorage replaces ra only
        dafPersist::BoostStorage* boost = dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(boost != 0, "Didn't get BoostStorage");
        boost->getIArchive() & mp->_ra;
        return;
    }
    else if (typeid(*storage) == typeid(dafPersist::DbStorage)) {
        // DbStorage replaces decl only
        dafPersist::DbStorage* db = dynamic_cast<dafPersist::DbStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(db != 0, "Didn't get DbStorage");
        db->setTableForQuery("DbStorage_Test_2");
        db->condParam<long long>("id", testId);
        db->setQueryWhere("id = :id");
        db->outParam("decl", &(mp->_decl));

        db->query();

        BOOST_CHECK_MESSAGE(db->next() == true, "Failed to get row");
        BOOST_CHECK_MESSAGE(db->columnIsNull(0) == false, "Null column 0");
        BOOST_CHECK_MESSAGE(db->next() == false, "Got more than one row");

        db->finishQuery();
        return;
    }
    BOOST_FAIL("Didn't recognize Storage type");
}

// Actually serialize the MyPersistable.
// Send/get the RA and declination to/from the archive.
template <class Archive> void MyFormatter::delegateSerialize(Archive& ar, unsigned int const version, dafBase::Persistable* persistable) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    ar & boost::serialization::base_object<dafBase::Persistable>(*mp);
    ar & mp->_ra;
    ar & mp->_decl;
};

///////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(Persistence2Suite)

BOOST_AUTO_TEST_CASE(Persistence2Test) {
    // Define a blank Policy.
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    // Get a unique id for this test.
    struct timeval tv;
    gettimeofday(&tv, 0);      
    long long testId = tv.tv_sec * 1000000LL + tv.tv_usec;

    std::ostringstream os;
    os << testId;
    std::string testIdString = os.str();

    dafBase::PropertySet::Ptr additionalData(new dafBase::PropertySet);
    additionalData->add("visitId", testId);
    additionalData->add("sliceId", 0);


    MyPersistable mp(1.73205, 1.61803);

    dafPersist::LogicalLocation pathLoc("tests/data/MyPersistable.boost." + testIdString);
    dafPersist::LogicalLocation dbLoc("mysql://lsst10.ncsa.uiuc.edu:3306/test");

    {
        dafPersist::Persistence::Ptr persist = dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getPersistStorage("BoostStorage", pathLoc));
        storageList.push_back(persist->getPersistStorage("DbStorage", dbLoc));
        persist->persist(mp, storageList, additionalData);
    }

    {
        dafPersist::Persistence::Ptr persist = dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getRetrieveStorage("BoostStorage", pathLoc));
        storageList.push_back(persist->getRetrieveStorage("DbStorage", dbLoc));
        dafBase::Persistable::Ptr pp = persist->retrieve("MyPersistable", storageList, additionalData);
        BOOST_CHECK_MESSAGE(pp != 0, "Didn't get a Persistable");
        BOOST_CHECK_MESSAGE(typeid(*pp) == typeid(MyPersistable), "Didn't get MyPersistable");
        MyPersistable::Ptr mp1 = boost::dynamic_pointer_cast<MyPersistable, dafBase::Persistable>(pp);
        BOOST_CHECK_MESSAGE(mp1, "Couldn't cast to MyPersistable");
        BOOST_CHECK_MESSAGE(mp1.get() != &mp, "Got same MyPersistable");
        BOOST_CHECK_MESSAGE(mp1->getRa() == 1.73205, "RA is incorrect");
        BOOST_CHECK_MESSAGE(mp1->getDecl() == 1.61803, "Decl is incorrect");
    }
}

BOOST_AUTO_TEST_SUITE_END()
