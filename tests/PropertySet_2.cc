/**
 * This test tests PropertySet persistence more extensively.
 */
extern "C" {
#  include <sys/time.h>
}
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"

#include "boost/serialization/export.hpp"

#define BOOST_TEST_MODULE Persistence_1
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
    LSST_PERSIST_FORMATTER(MyFormatter)
    double _ra;
    double _decl;
};

// A small Formatter.
class MyFormatter : public dafPersist::Formatter {
public:
    MyFormatter(void) : dafPersist::Formatter(typeid(this)) { };
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
dafPersist::Formatter::Ptr MyFormatter::createInstance(lsst::pex::policy::Policy::Ptr ) {
    return dafPersist::Formatter::Ptr(new MyFormatter);
}

// Persistence for MyPersistables.
// Supports BoostStorage only.
void MyFormatter::write(dafBase::Persistable const* persistable, dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr) {
    BOOST_CHECK_MESSAGE(persistable != 0, "Persisting null");
    BOOST_CHECK_MESSAGE(storage, "No Storage provided");
    MyPersistable const* mp = dynamic_cast<MyPersistable const*>(persistable);
    BOOST_CHECK_MESSAGE(mp != 0, "Persisting non-MyPersistable");
    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(boost != 0, "Didn't get BoostStorage");
        boost->getOArchive() & *mp;
        return;
    }
    BOOST_FAIL("Didn't recognize Storage type");

}

// Retrieval for MyPersistables.
// Supports BoostStorage only.
dafBase::Persistable* MyFormatter::read(dafPersist::Storage::Ptr storage, dafBase::PropertySet::Ptr) {
    MyPersistable* mp = new MyPersistable;

    if (typeid(*storage) == typeid(dafPersist::BoostStorage)) {
        dafPersist::BoostStorage* boost =
            dynamic_cast<dafPersist::BoostStorage*>(storage.get());
        BOOST_CHECK_MESSAGE(boost != 0, "Didn't get BoostStorage");
        boost->getIArchive() & *mp;
        return mp;
    }
    BOOST_FAIL("Didn't recognize Storage type");
    return mp;
}

void MyFormatter::update(dafBase::Persistable* , dafPersist::Storage::Ptr, dafBase::PropertySet::Ptr) {
    BOOST_FAIL("Shouldn't be updating");
}

// Actually serialize the MyPersistable.
// Send/get the RA and declination to/from the archive.
template <class Archive> void MyFormatter::delegateSerialize(Archive& ar, unsigned int const, dafBase::Persistable* persistable) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    ar & boost::serialization::base_object<dafBase::Persistable>(*mp);
    ar & mp->_ra;
    ar & mp->_decl;
}

BOOST_CLASS_EXPORT(MyPersistable)

///////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE(PropertySet2Suite)

BOOST_AUTO_TEST_CASE(PropertySet2Test) {
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

    boost::shared_ptr<MyPersistable> mp(new MyPersistable(1.73205, 1.61803));
    dafBase::PropertySet ps;
    ps.add("mp", boost::static_pointer_cast<dafBase::Persistable, MyPersistable>(mp));
    ps.add("first.second", 8118);

    dafPersist::LogicalLocation pathLoc("tests/data/PropSet.boost." + testIdString);
    {

        dafPersist::Persistence::Ptr persist =
            dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getPersistStorage("BoostStorage", pathLoc));
        persist->persist(ps, storageList, additionalData);
    }

    {
        dafPersist::Persistence::Ptr persist =
            dafPersist::Persistence::getPersistence(policy);
        dafPersist::Storage::List storageList;
        storageList.push_back(persist->getRetrieveStorage("BoostStorage", pathLoc));
        dafBase::Persistable::Ptr pp = persist->retrieve("PropertySet", storageList, additionalData);
        BOOST_CHECK(pp != 0);
        BOOST_CHECK(typeid(*pp) == typeid(dafBase::PropertySet));
        dafBase::PropertySet::Ptr ps1 = boost::dynamic_pointer_cast<dafBase::PropertySet, dafBase::Persistable>(pp);
        BOOST_CHECK(ps1);
        BOOST_CHECK(ps1.get() != &ps);
        BOOST_CHECK_EQUAL(ps1->get<int>("first.second"), 8118);
        boost::shared_ptr<MyPersistable> mp1 = boost::dynamic_pointer_cast<MyPersistable, dafBase::Persistable>(ps1->getAsPersistablePtr("mp"));
        BOOST_CHECK(mp1);
        BOOST_CHECK_EQUAL(mp1->getRa(), 1.73205);
        BOOST_CHECK_EQUAL(mp1->getDecl(), 1.61803);
    }
}

BOOST_AUTO_TEST_SUITE_END()
