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
 * \file DbStorage_1.cc
 *
 * This test tests the DbStorage class.
 */
#include <sstream>
#include <string>
#include <sys/time.h>
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/pex/exceptions.h"

#define BOOST_TEST_MODULE DbStorage_1
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
namespace dafPersist = lsst::daf::persistence;

BOOST_AUTO_TEST_SUITE(DbStorageSuite)

BOOST_AUTO_TEST_CASE(DbStorage) {
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    struct timeval tv;
    gettimeofday(&tv, 0); 
    long long testId = tv.tv_sec * 1000000LL + tv.tv_usec;
    std::ostringstream os;
    os << "DbStorage_Test_N_" << testId;
    std::string tempTableName = os.str();

    // Normally, we would create a DbStorage via
    // Persistence::getPersistStorage().  For testing purposes, we create one
    // ourselves.
    dafPersist::DbStorage dbs;

    dbs.setPolicy(policy);
    dafPersist::LogicalLocation loc("mysql://lsst10.ncsa.uiuc.edu:3306/test");
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
    dbs.outColumn("DbStorage_Test_1.something");
    dbs.outColumn("ra");

    dbs.query();

    BOOST_CHECK_MESSAGE(dbs.next() == true, "Failed to get row");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(0) == false, "Null decl column");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(1) == true, "Non-null something column");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(2) == false, "Null ra column");
    double ra = dbs.getColumnByPos<double>(2);
    BOOST_CHECK_MESSAGE(ra == 3.14159, "RA is incorrect");
    double decl = dbs.getColumnByPos<double>(0);
    BOOST_CHECK_MESSAGE(decl == 2.71828, "Decl is incorrect");
    BOOST_CHECK_MESSAGE(dbs.next() == false, "Got more than one row");

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

    BOOST_CHECK_MESSAGE(dbs.next() == true, "Failed to get row");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(0) == false, "Null decl column");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(1) == true, "Non-null something column");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(2) == false, "Null ra column");
    BOOST_CHECK_MESSAGE(ra == 3.14159, "RA is incorrect");
    BOOST_CHECK_MESSAGE(decl == 2.71828, "Decl is incorrect");
    BOOST_CHECK_MESSAGE(dbs.next() == false, "Got more than one row");

    dbs.finishQuery();
    dbs.endTransaction();
}

BOOST_AUTO_TEST_CASE(DbStorageExprs) {
    lsst::pex::policy::Policy::Ptr policy(new lsst::pex::policy::Policy);

    // Normally, we would create a DbStorage via
    // Persistence::getRetrieveStorage().  For testing purposes, we create one
    // ourselves.
    dafPersist::DbStorage dbs;

    dbs.setPolicy(policy);
    dafPersist::LogicalLocation loc("mysql://lsst10.ncsa.uiuc.edu:3306/test");
    dbs.setRetrieveLocation(loc);

    dbs.startTransaction();
    dbs.setTableForQuery("DUAL", true);
    dbs.outColumn("1 + 1", true);
    dbs.query();

    BOOST_CHECK_MESSAGE(dbs.next() == true, "Failed to get row");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(0) == false, "Null output column");
    int result = dbs.getColumnByPos<int>(0);
    BOOST_CHECK_MESSAGE(result == 2, "Result is incorrect");
    BOOST_CHECK_MESSAGE(dbs.next() == false, "Got more than one row");

    dbs.finishQuery();
    dbs.endTransaction();

    dbs.startTransaction();
    dbs.setTableForQuery("DUAL", true);
    result = 0;
    dbs.outParam("2 + 2", &result, true);
    dbs.query();

    BOOST_CHECK_MESSAGE(dbs.next() == true, "Failed to get row");
    BOOST_CHECK_MESSAGE(dbs.columnIsNull(0) == false, "Null output column");
    BOOST_CHECK_MESSAGE(result == 4, "Result is incorrect");
    BOOST_CHECK_MESSAGE(dbs.next() == false, "Got more than one row");

    dbs.finishQuery();
    dbs.endTransaction();
}

BOOST_AUTO_TEST_SUITE_END()
