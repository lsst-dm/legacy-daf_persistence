#include <iomanip>
#include <iostream>
#include <sys/time.h>

#include "lsst/mwi/persistence/DateTime.h"
#include "lsst/mwi/persistence/DbStorage.h"
#include "lsst/mwi/persistence/DbTsvStorage.h"
#include "lsst/mwi/persistence/LogicalLocation.h"

using namespace lsst::mwi::persistence;


// Test writing DateTime objects to the database.
void test(DbStorage* dbs, long long now) {

    // Create a couple of DateTime objects and use the class' methods.
    DateTime utc(now);
    DateTime tai(utc.utc2tai());
    double mjd = utc.utc2mjd();

    std::cerr << "Input UTC: " << utc.nsecs() << std::endl;
    std::cerr << "Input MJD: " << std::setprecision(17) << mjd << std::endl;

    // Insert a row into the database.
    LogicalLocation loc("mysql://lsst10.ncsa.uiuc.edu:3306/test");
    dbs->setPersistLocation(loc);
    dbs->startTransaction();
    dbs->setTableForInsert("DateTimeTest");
    if (typeid(*dbs) == typeid(DbTsvStorage)) {
        // Cast to get templates right.
        DbTsvStorage* dbts = dynamic_cast<DbTsvStorage*>(dbs);
        dbts->setColumn<DateTime>("utc", utc);
        dbts->setColumn<DateTime>("tai", tai);
        dbts->setColumn<double>("mjd", mjd);
    }
    else {
        dbs->setColumn<DateTime>("utc", utc);
        dbs->setColumn<DateTime>("tai", tai);
        dbs->setColumn<double>("mjd", mjd);
    }
    dbs->insertRow();
    dbs->endTransaction();

    // Retrieve that row from the database, using a DateTime in the WHERE
    // clause and using bound output variables.
    dbs->setRetrieveLocation(loc);
    dbs->startTransaction();
    dbs->setTableForQuery("DateTimeTest");
    dbs->condParam<DateTime>("nowTAI", tai);
    dbs->setQueryWhere("tai = :nowTAI");
    double outMJD;
    dbs->outParam<double>("mjd", &outMJD);
    DateTime outUTC;
    dbs->outParam<DateTime>("utc", &outUTC);
    dbs->query();

    // We should have at least one row.
    assert(dbs->next());

    // Neither result column should be null.
    assert(!dbs->columnIsNull(0));
    assert(!dbs->columnIsNull(1));

    std::cerr << "Output UTC: " << outUTC.nsecs() << std::endl;
    std::cerr << "Output MJD: " << outMJD << std::endl;

    // The output UTC should match the input value to within one second.
    long long diffUTC = outUTC.nsecs() - utc.nsecs();
    if (diffUTC < 0) assert(diffUTC > -1000000000LL);
    else assert(diffUTC < 1000000000LL);

    // The output MJD should match the input value to within 1e-14.
    double diffMJD = outMJD - mjd;
    if (diffMJD < 0) assert(diffMJD / mjd > -1.0e-14);
    else assert(diffMJD / mjd < 1.0e-14);

    // We should only have one row.
    assert(!dbs->next());

    // Clean up.
    dbs->finishQuery();
    dbs->endTransaction();
}


int main(void) {
    // Get the current time.
    struct timeval tv;
    gettimeofday(&tv, 0);      
    long long now = tv.tv_sec * 1000000000LL + tv.tv_usec * 1000LL;

    DbStorage dbs;
    test(&dbs, now);

    now -= 1000000000LL;

    DbTsvStorage dbts;
    lsst::mwi::policy::Policy::Ptr pol(new lsst::mwi::policy::Policy);
    pol->set("SaveTemp", true);
    dbts.setPolicy(pol);
    test(&dbts, now);

    return 0;
}
