/**
 * \file DbStorage_1.cc
 *
 * This test tests the DbStorage class.
 */
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/time.h>
#include "lsst/mwi/persistence/DbStorage.h"
#include "lsst/mwi/persistence/LogicalLocation.h"
#include "lsst/mwi/exceptions.h"

using namespace lsst::mwi::persistence;

#define Assert(b, m) tattle(b, m, __LINE__)

static void tattle(bool mustBeTrue, std::string const& failureMsg, int line) {
    if (! mustBeTrue) {
        std::ostringstream msg;
        msg << __FILE__ << ':' << line << ":\n" << failureMsg << std::ends;
        throw std::runtime_error(msg.str());
    }
}


void test(void) {
    std::cout << "Initial setup" << std::endl;
    lsst::mwi::policy::Policy::Ptr policy(new lsst::mwi::policy::Policy);

    struct timeval tv;
    gettimeofday(&tv, 0); 
    long long testId = tv.tv_sec * 1000000LL + tv.tv_usec;
    std::ostringstream os;
    os << "DbStorage_Test_N_" << testId;
    std::string tempTableName = os.str();

    // Normally, we would create a DbStorage via
    // Persistence::getPersistStorage().  For testing purposes, we create one
    // ourselves.
    DbStorage dbs;

    dbs.setPolicy(policy);
    LogicalLocation loc("mysql://lsst10.ncsa.uiuc.edu:3306/test");
    dbs.setPersistLocation(loc);

    dbs.startTransaction();
    dbs.createTableFromTemplate(tempTableName, "DbStorage_Test_1");
    dbs.endTransaction();

    dbs.startTransaction();
    dbs.truncateTable(tempTableName);
    dbs.endTransaction();

    dbs.startTransaction();
    dbs.dropTable(tempTableName);
    dbs.endTransaction();

    dbs.startTransaction();
    dbs.setTableForInsert("DbStorage_Test_1");
    dbs.setColumn<long long>("id", testId);
    dbs.setColumn<double>("ra", 3.14159);
    dbs.setColumn<double>("decl", 2.71828);
    dbs.setColumnToNull("something");
    dbs.insertRow();
    dbs.endTransaction();
    // Everything is OK as long as we didn't throw an exception above.

    // Normally, DbStorages are not reused.  There is no reason they cannot
    // be, however.
    dbs.setRetrieveLocation(loc);
    dbs.startTransaction();
    dbs.setTableForQuery("DbStorage_Test_1");
    dbs.condParam<long long>("id", testId);
    dbs.setQueryWhere("id = :id");
    dbs.outColumn("decl");
    dbs.outColumn("something");
    dbs.outColumn("ra");

    dbs.query();

    Assert(dbs.next() == true, "Failed to get row");
    Assert(dbs.columnIsNull(0) == false, "Null decl column");
    Assert(dbs.columnIsNull(1) == true, "Non-null something column");
    Assert(dbs.columnIsNull(2) == false, "Null ra column");
    double ra = dbs.getColumnByPos<double>(2);
    Assert(ra == 3.14159, "RA is incorrect");
    double decl = dbs.getColumnByPos<double>(0);
    Assert(decl == 2.71828, "Decl is incorrect");
    std::cout << "Row: " << ra << ", " << decl << std::endl;

    Assert(dbs.next() == false, "Got more than one row");

    dbs.finishQuery();
    dbs.endTransaction();

    // Let's do that query again, this time using bound variables.
    dbs.setRetrieveLocation(loc);
    dbs.startTransaction();
    dbs.setTableForQuery("DbStorage_Test_1");
    dbs.condParam<long long>("id", testId);
    dbs.setQueryWhere("id = :id");
    dbs.outParam("decl", &decl);
    int junk;
    dbs.outParam("something", &junk);
    dbs.outParam("ra", &ra);

    dbs.query();

    Assert(dbs.next() == true, "Failed to get row");
    Assert(dbs.columnIsNull(0) == false, "Null decl column");
    Assert(dbs.columnIsNull(1) == true, "Non-null something column");
    Assert(dbs.columnIsNull(2) == false, "Null ra column");
    Assert(ra == 3.14159, "RA is incorrect");
    Assert(decl == 2.71828, "Decl is incorrect");
    std::cout << "Row: " << ra << ", " << decl << std::endl;

    Assert(dbs.next() == false, "Got more than one row");

    dbs.finishQuery();
    dbs.endTransaction();
}

int main(void) {

    test();

    if (lsst::mwi::data::Citizen::census(0) == 0) {
        std::cerr << "No leaks detected" << std::endl;
    }
    else {
        std::cerr << "Leaked memory blocks:" << std::endl;
        lsst::mwi::data::Citizen::census(std::cerr);
        Assert(false, "Had memory leaks");
    }

    return 0;
}
