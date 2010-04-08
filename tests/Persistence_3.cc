/**
 * \file Persistence_3.cc
 *
 * This test checks that Persistable objects can be persisted and retrieved
 * as components of PropertySet objects to and from BoostStorage.
 */

#include <sstream>
#include <sys/time.h>
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/pex/exceptions.h"

#include <boost/serialization/export.hpp>

#define BOOST_TEST_MODULE Persistence_3
#include "boost/test/included/unit_test.hpp"

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

BOOST_CLASS_EXPORT(MyPersistable);

// A small Formatter.
class MyFormatter : public dafPersist::Formatter {
public:
    MyFormatter(void) : dafPersist::Formatter(typeid(*this)) { };
    virtual void write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, int iter, int len);
    virtual dafBase::Persistable* read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, bool* done);
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
// Supports BoostStorage only.
void MyFormatter::write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, int iter, int len) {
    BOOST_FAIL("write() called unexpectedly");
}

// Retrieval for MyPersistables.
// Supports BoostStorage only.
dafBase::Persistable* MyFormatter::read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData, bool* done) {
    BOOST_FAIL("read() called unexpectedly");
    return 0;
}

void MyFormatter::update(dafBase::Persistable* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr additionalData) {
    BOOST_FAIL("update() called unexpectedly");
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

BOOST_AUTO_TEST_SUITE(Persistence3Suite)

BOOST_AUTO_TEST_CASE(Persistence3Test) {
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
    additionalData->add("info.visitId", testId);
    additionalData->add("info.sliceId", 0);


    dafBase::Persistable::Ptr ppOrig(new MyPersistable(1.73205, 1.61803));
    dafBase::PropertySet::Ptr theProperty(new dafBase::PropertySet);
    theProperty->add("prop", ppOrig);

    dafPersist::LogicalLocation pathLoc("tests/data/MyPersistable.boost." + testIdString);

    {
        dafPersist::Persistence::Ptr persist = dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getPersistStorage("BoostStorage", pathLoc));
        persist->persist(*theProperty, storageList, additionalData);
    }

    {
        dafPersist::Persistence::Ptr persist = dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getRetrieveStorage("BoostStorage", pathLoc));
        dafBase::Persistable::Ptr pp = persist->retrieve("PropertySet", storageList, additionalData);
        BOOST_CHECK_MESSAGE(pp != 0, "Didn't get a Persistable");
        BOOST_CHECK_MESSAGE(typeid(*pp) == typeid(dafBase::PropertySet), "Didn't get PropertySet");
        dafBase::PropertySet::Ptr dp = boost::dynamic_pointer_cast<dafBase::PropertySet, dafBase::Persistable>(pp);
        BOOST_CHECK_MESSAGE(dp, "Couldn't cast to PropertySet");
        BOOST_CHECK_MESSAGE(dp != theProperty, "Got same PropertySet");
        dafBase::Persistable::Ptr pp1 = dp->getAsPersistablePtr("prop");
        BOOST_CHECK_MESSAGE(pp1, "Couldn't retrieve Persistable");
        BOOST_CHECK_MESSAGE(typeid(*pp1) == typeid(MyPersistable), "Not a MyPersistable");
        MyPersistable::Ptr mp = boost::dynamic_pointer_cast<MyPersistable, dafBase::Persistable>(pp1);
        BOOST_CHECK_MESSAGE(mp, "Couldn't retrieve MyPersistable");
        BOOST_CHECK_MESSAGE(mp->getRa() == 1.73205, "RA is incorrect");
        BOOST_CHECK_MESSAGE(mp->getDecl() == 1.61803, "Decl is incorrect");
    }
}

BOOST_AUTO_TEST_SUITE_END()
