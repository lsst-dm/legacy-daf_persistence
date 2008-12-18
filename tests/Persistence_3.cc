/**
 * \file Persistence_3.cc
 *
 * This test checks that Persistable objects can be persisted and retrieved
 * as components of PropertySet objects to and from BoostStorage.
 */
#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>
#include <sys/time.h>
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/pex/exceptions.h"

#include <boost/serialization/export.hpp>

using namespace lsst::daf::persistence;

#define Assert(b, m) tattle(b, m, __LINE__)

static void tattle(bool mustBeTrue, std::string const& failureMsg, int line) {
    if (! mustBeTrue) {
        std::ostringstream msg;
        msg << __FILE__ << ':' << line << ":\n" << failureMsg << std::ends;
        throw std::runtime_error(msg.str());
    }
}

// A small Persistable.

// Forward declaration may be needed with gcc 4+.
class MyFormatter;

class MyPersistable : public lsst::daf::base::Persistable {
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
class MyFormatter : public Formatter {
public:
    MyFormatter(void) : Formatter(typeid(*this)) { };
    virtual void write(lsst::daf::base::Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData);
    virtual lsst::daf::base::Persistable* read(Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData);
    virtual void update(lsst::daf::base::Persistable* persistable, Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData);
    template <class Archive> static void delegateSerialize(Archive& ar, unsigned int const version, lsst::daf::base::Persistable* persistable);
private:
    static Formatter::Ptr createInstance(lsst::pex::policy::Policy::Ptr policy);
    static FormatterRegistration registration;
};

// Include this file when implementing a Formatter.
#include "lsst/daf/persistence/FormatterImpl.h"

// Register the formatter factory function.
FormatterRegistration MyFormatter::registration("MyPersistable", typeid(MyPersistable), createInstance);

// The definition of the factory function.
Formatter::Ptr MyFormatter::createInstance(lsst::pex::policy::Policy::Ptr policy) {
    return Formatter::Ptr(new MyFormatter);
}

// Persistence for MyPersistables.
// Supports BoostStorage only.
void MyFormatter::write(lsst::daf::base::Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData) {
    Assert(false, "write() called unexpectedly");

}

// Retrieval for MyPersistables.
// Supports BoostStorage only.
lsst::daf::base::Persistable* MyFormatter::read(Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData) {
    Assert(false, "read() called unexpectedly");
    return 0;
}

void MyFormatter::update(lsst::daf::base::Persistable* persistable, Storage::Ptr storage, lsst::daf::base::PropertySet::Ptr additionalData) {
    Assert(false, "update() called unexpectedly");
}

// Actually serialize the MyPersistable.
// Send/get the RA and declination to/from the archive.
template <class Archive> void MyFormatter::delegateSerialize(Archive& ar, unsigned int const version, lsst::daf::base::Persistable* persistable) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    ar & boost::serialization::base_object<lsst::daf::base::Persistable>(*mp);
    ar & mp->_ra;
    ar & mp->_decl;
};

///////////////////////////////////////////////////////////////////////////////

void test(void) {
    std::cout << "Initial setup" << std::endl;

    // Define a blank Policy.
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    // Get a unique id for this test.
    struct timeval tv;
    gettimeofday(&tv, 0);      
    long long testId = tv.tv_sec * 1000000LL + tv.tv_usec;

    std::ostringstream os;
    os << testId;
    std::string testIdString = os.str();

    lsst::daf::base::PropertySet::Ptr additionalData(new lsst::daf::base::PropertySet);
    additionalData->add("info.visitId", testId);
    additionalData->add("info.sliceId", 0);


    lsst::daf::base::Persistable::Ptr ppOrig(new MyPersistable(1.73205, 1.61803));
    lsst::daf::base::PropertySet::Ptr theProperty(new lsst::daf::base::PropertySet);
    theProperty->add("prop", ppOrig);

    LogicalLocation pathLoc("MyPersistable.boost." + testIdString);

    {
        Persistence::Ptr persist = Persistence::getPersistence(policy);
        Storage::List storageList;
        storageList.push_back(persist->getPersistStorage("BoostStorage", pathLoc));
        persist->persist(*theProperty, storageList, additionalData);
    }

    {
        Persistence::Ptr persist = Persistence::getPersistence(policy);
        Storage::List storageList;
        storageList.push_back(persist->getRetrieveStorage("BoostStorage", pathLoc));
        lsst::daf::base::Persistable::Ptr pp = persist->retrieve("PropertySet", storageList, additionalData);
        Assert(pp != 0, "Didn't get a Persistable");
        Assert(typeid(*pp) == typeid(lsst::daf::base::PropertySet), "Didn't get PropertySet");
        lsst::daf::base::PropertySet::Ptr dp = boost::dynamic_pointer_cast<lsst::daf::base::PropertySet, lsst::daf::base::Persistable>(pp);
        Assert(dp, "Couldn't cast to PropertySet");
        Assert(dp != theProperty, "Got same PropertySet");
        lsst::daf::base::Persistable::Ptr pp1 = dp->getAsPersistablePtr("prop");
        Assert(pp1, "Couldn't retrieve Persistable");
        Assert(typeid(*pp1) == typeid(MyPersistable), "Not a MyPersistable");
        MyPersistable::Ptr mp = boost::dynamic_pointer_cast<MyPersistable, lsst::daf::base::Persistable>(pp1);
        Assert(mp, "Couldn't retrieve MyPersistable");
        Assert(mp->getRa() == 1.73205, "RA is incorrect");
        Assert(mp->getDecl() == 1.61803, "Decl is incorrect");
    }
}



int main(void) {
    // Run the tests.
    test();

    // Check for memory leaks.
    if (lsst::daf::base::Citizen::census(0) == 0) {
        std::cerr << "No leaks detected" << std::endl;
    }
    else {
        std::cerr << "Leaked memory blocks:" << std::endl;
        lsst::daf::base::Citizen::census(std::cerr);
        Assert(false, "Had memory leaks");
    }

    return 0;
}
