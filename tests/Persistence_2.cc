/**
 * \file Persistence_2.cc
 *
 * This test checks that objects can be persisted and retrieved from multiple
 * Storages.
 */
#include <iostream>
#include <sstream>
#include <string>
#include <stdexcept>
#include <sys/time.h>
#include "lsst/daf/persistence/BoostStorage.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/Formatter.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/pex/exceptions.h"

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
// Supports BoostStorage and DbStorage.
void MyFormatter::write(Persistable const* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    Assert(persistable != 0, "Persisting null");
    Assert(storage, "No Storage provided");
    long long testId = boost::any_cast<long long>(additionalData->findUnique("visitId")->getValue());
    MyPersistable const* mp = dynamic_cast<MyPersistable const*>(persistable);
    Assert(mp != 0, "Persisting non-MyPersistable");
    if (typeid(*storage) == typeid(BoostStorage)) {
        BoostStorage* boost = dynamic_cast<BoostStorage*>(storage.get());
        Assert(boost != 0, "Didn't get BoostStorage");
        boost->getOArchive() & mp->_ra;
        return;
    }
    else if (typeid(*storage) == typeid(DbStorage)) {
        DbStorage* db = dynamic_cast<DbStorage*>(storage.get());
        Assert(db != 0, "Didn't get DbStorage");
        db->setTableForInsert("DbStorage_Test_2");
        db->setColumn<long long>("id", testId);
        db->setColumn<double>("decl", mp->_decl);
        db->insertRow();
        return;
    }
    Assert(false, "Didn't recognize Storage type");

}

// Retrieval for MyPersistables.
// Supports BoostStorage, DbStorage, and DbTsvStorage.
Persistable* MyFormatter::read(Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    MyPersistable* mp = new MyPersistable;

    long long testId = boost::any_cast<long long>(additionalData->findUnique("visitId")->getValue());
    if (typeid(*storage) == typeid(BoostStorage)) {
        BoostStorage* boost = dynamic_cast<BoostStorage*>(storage.get());
        Assert(boost != 0, "Didn't get BoostStorage");
        boost->getIArchive() & mp->_ra;
        return mp;
    }
    else if (typeid(*storage) == typeid(DbStorage)) {
        DbStorage* db = dynamic_cast<DbStorage*>(storage.get());
        Assert(db != 0, "Didn't get DbStorage");
        db->setTableForQuery("DbStorage_Test_2");
        db->condParam<long long>("id", testId);
        db->setQueryWhere("id = :id");
        db->outParam("decl", &(mp->_decl));

        db->query();

        Assert(db->next() == true, "Failed to get row");
        Assert(db->columnIsNull(0) == false, "Null column 0");
        Assert(db->next() == false, "Got more than one row");

        db->finishQuery();
        return mp;
    }
    Assert(false, "Didn't recognize Storage type");
    return mp;
}

void MyFormatter::update(Persistable* persistable, Storage::Ptr storage, lsst::daf::base::DataProperty::PtrType additionalData) {
    MyPersistable* mp = dynamic_cast<MyPersistable*>(persistable);
    long long testId = boost::any_cast<long long>(additionalData->findUnique("visitId")->getValue());
    if (typeid(*storage) == typeid(BoostStorage)) {
        // BoostStorage replaces ra only
        BoostStorage* boost = dynamic_cast<BoostStorage*>(storage.get());
        Assert(boost != 0, "Didn't get BoostStorage");
        boost->getIArchive() & mp->_ra;
        return;
    }
    else if (typeid(*storage) == typeid(DbStorage)) {
        // DbStorage replaces decl only
        DbStorage* db = dynamic_cast<DbStorage*>(storage.get());
        Assert(db != 0, "Didn't get DbStorage");
        db->setTableForQuery("DbStorage_Test_2");
        db->condParam<long long>("id", testId);
        db->setQueryWhere("id = :id");
        db->outParam("decl", &(mp->_decl));

        db->query();

        Assert(db->next() == true, "Failed to get row");
        Assert(db->columnIsNull(0) == false, "Null column 0");
        Assert(db->next() == false, "Got more than one row");

        db->finishQuery();
        return;
    }
    Assert(false, "Didn't recognize Storage type");
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


    MyPersistable mp(1.73205, 1.61803);

    LogicalLocation pathLoc("MyPersistable.boost." + testIdString);
    LogicalLocation dbLoc("mysql://lsst10.ncsa.uiuc.edu:3306/test");

    {
        Persistence::Ptr persist = Persistence::getPersistence(policy);
        Storage::List storageList;
        storageList.push_back(persist->getPersistStorage("BoostStorage", pathLoc));
        storageList.push_back(persist->getPersistStorage("DbStorage", dbLoc));
        persist->persist(mp, storageList, additionalData);
    }

    {
        Persistence::Ptr persist = Persistence::getPersistence(policy);
        Storage::List storageList;
        storageList.push_back(persist->getRetrieveStorage("BoostStorage", pathLoc));
        storageList.push_back(persist->getRetrieveStorage("DbStorage", dbLoc));
        Persistable::Ptr pp = persist->retrieve("MyPersistable", storageList, additionalData);
        Assert(pp != 0, "Didn't get a Persistable");
        Assert(typeid(*pp) == typeid(MyPersistable), "Didn't get MyPersistable");
        MyPersistable::Ptr mp1 = boost::dynamic_pointer_cast<MyPersistable, Persistable>(pp);
        Assert(mp1, "Couldn't cast to MyPersistable");
        Assert(mp1.get() != &mp, "Got same MyPersistable");
        Assert(mp1->getRa() == 1.73205, "RA is incorrect");
        Assert(mp1->getDecl() == 1.61803, "Decl is incorrect");
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
