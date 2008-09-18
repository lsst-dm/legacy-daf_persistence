// -*- lsst-c++ -*-
#include <lsst/daf/base/DataProperty.h>
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
     DataProperty::PtrType additionalData = DataProperty::createPropertyNode("additionalData"); // empty for testing

     DataProperty::PtrType root = DataProperty::createPropertyNode("root");

     DataProperty::PtrType prop1(new DataProperty("name1", std::string("value1")));
     DataProperty::PtrType prop2(new DataProperty("name2", 2));
     DataProperty::PtrType prop2a(new DataProperty("name2", 4));
     
     root->addProperty(prop1);
     root->addProperty(prop2);
     root->addProperty(prop2a);

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
    DataProperty::PtrType additionalData = DataProperty::createPropertyNode("additionalData"); // empty for testing

    boost::any foo = lsst::utils::stringToAny("-1234");
    boost::any foo2 = lsst::utils::stringToAny("1.234e-1");
    boost::any foo3 = lsst::utils::stringToAny("'This is a Fits string'");
    
    DataProperty::PtrType fooProp(new DataProperty("foo", foo));
    DataProperty::PtrType fooProp2(new DataProperty("foo2", foo2));
    DataProperty::PtrType fooProp3(new DataProperty("foo3", foo3));

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
