// -*- lsst-c++ -*-
#include <lsst/daf/base/PropertySet.h>
#include <lsst/daf/base/Citizen.h>
#include <lsst/pex/logging/Trace.h>
#include <lsst/utils/Utils.h>

#include "lsst/pex/policy/Policy.h"
#include "lsst/daf/persistence/Persistence.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using namespace std;
using namespace lsst::daf::base;
using namespace lsst::pex::logging;

void test() {
    PropertySet::Ptr additionalData(new PropertySet); // empty for testing

    PropertySet::Ptr root(new PropertySet);
    root->add("name1", "value1");
    root->add("name2", 2);
    root->add("name2", 4);

    lsst::pex::policy::Policy::Ptr policyPtr(new lsst::pex::policy::Policy);
    lsst::daf::persistence::Persistence::Ptr persist =
        lsst::daf::persistence::Persistence::getPersistence(policyPtr);
    lsst::daf::persistence::Storage::List storageList;

    lsst::daf::persistence::LogicalLocation loc("fov391/root.boost");
    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*root, storageList, additionalData);

    lsst::daf::persistence::LogicalLocation loc2("fov391/root.xml");
    storageList[0] = persist->getPersistStorage("XmlStorage", loc2);
    persist->persist(*root, storageList, additionalData);
}     

void test2()
{
    PropertySet::Ptr additionalData(new PropertySet); // empty for testing

    PropertySet::Ptr fooProp(new PropertySet);
    fooProp->set("foo", -1234);
    PropertySet::Ptr fooProp2(new PropertySet);
    fooProp2->set("foo2", 1.234e-1);
    PropertySet::Ptr fooProp3(new PropertySet);
    fooProp3->set("foo3", "This is a Fits string");

    lsst::pex::policy::Policy::Ptr policyPtr(new lsst::pex::policy::Policy);
    lsst::daf::persistence::Persistence::Ptr persist =
        lsst::daf::persistence::Persistence::getPersistence(policyPtr);
    lsst::daf::persistence::Storage::List storageList;

    lsst::daf::persistence::LogicalLocation loc("fov391/foo.boost");
    lsst::daf::persistence::LogicalLocation loc2("fov391/foo2.boost");
    lsst::daf::persistence::LogicalLocation loc3("fov391/foo3.boost");

    storageList.push_back(persist->getPersistStorage("BoostStorage", loc));
    persist->persist(*fooProp, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc2);
    persist->persist(*fooProp2, storageList, additionalData);

    storageList[0] = persist->getPersistStorage("BoostStorage", loc3);
    persist->persist(*fooProp3, storageList, additionalData);
}

int main(int argc, char** argv) {
    int verbosity = 100;
    int exitVal = 0;
    
    if( argc > 1 )
    {
       try
       {
           int x = atoi(argv[1]);
           verbosity = x;
       }    
       catch(...)
       {
           verbosity = 0;
       }
    }

    Trace::setVerbosity("", verbosity);

    test();

    test2();

     //
     // Check for memory leaks
     //
     if (Citizen::census(0) == 0) {
         cerr << "No leaks detected" << endl;
         exitVal = EXIT_SUCCESS;
     } else {
         cerr << "Leaked memory blocks:" << endl;
         Citizen::census(cerr);
         exitVal = ~EXIT_SUCCESS;
     }
     
     return exitVal;
}
