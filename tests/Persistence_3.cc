/**
 * \file Persistence_3.cc
 *
 * This test checks that Persistable objects can be persisted and retrieved
 * as components of DataProperty objects to and from BoostStorage.
 */
#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>
#include <sys/time.h>
#include "lsst/daf/base/DataProperty.h"
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

class MyPersistable : public Persistable {
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
    virtual void write(Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData);
    virtual Persistable* read(Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData);
    virtual void update(Persistable* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData);
    template <class Archive> static void delegateSerialize(Archive& ar, unsigned int const version, Persistable* persistable);
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
void MyFormatter::write(Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    Assert(false, "write() called unexpectedly");

}

// Retrieval for MyPersistables.
// Supports BoostStorage only.
Persistable* MyFormatter::read(Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    Assert(false, "read() called unexpectedly");
    return 0;
}

void MyFormatter::update(Persistable* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    Assert(false, "update() called unexpectedly");
}

// Actually serialize the MyPersistable.
// Send/get the RA and declination to/from the archive.
template <class Archive> void MyFormatter::delegateSerialize(Archive& ar, unsigned int const version, Persistable* persistable) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    ar & boost::serialization::base_object<Persistable>(*mp);
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

    lsst::daf::base::DataProperty::PtrType additionalData = lsst::daf::data::SupportFactory::createPropertyNode("info");
    lsst::daf::base::DataProperty::PtrType child1(new lsst::daf::base::DataProperty("visitId", testId));
    lsst::daf::base::DataProperty::PtrType child2(new lsst::daf::base::DataProperty("sliceId", 0));
    additionalData->addProperty(child1);
    additionalData->addProperty(child2);


    Persistable::Ptr ppOrig(new MyPersistable(1.73205, 1.61803));
    lsst::daf::base::DataProperty::PtrType theProperty = lsst::daf::data::SupportFactory::createLeafProperty("prop", ppOrig);

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
        Persistable::Ptr pp = persist->retrieve("DataProperty", storageList, additionalData);
        Assert(pp != 0, "Didn't get a Persistable");
        Assert(typeid(*pp) == typeid(lsst::daf::base::DataProperty), "Didn't get DataProperty");
        lsst::daf::base::DataProperty::PtrType dp = boost::dynamic_pointer_cast<lsst::daf::base::DataProperty, Persistable>(pp);
        Assert(dp, "Couldn't cast to DataProperty");
        Assert(dp != theProperty, "Got same DataProperty");
        Persistable::Ptr pp1 = boost::any_cast<Persistable::Ptr>(dp->getValue());
        Assert(pp1, "Couldn't retrieve Persistable");
        Assert(typeid(*pp1) == typeid(MyPersistable), "Not a MyPersistable");
        MyPersistable::Ptr mp = boost::dynamic_pointer_cast<MyPersistable, Persistable>(pp1);
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
