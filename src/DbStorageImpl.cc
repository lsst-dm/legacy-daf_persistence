// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of DbStorageImpl class
 *
 * Uses Coral library for DBMS-independence.
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * \ingroup daf_persistence
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/daf/persistence/DbStorageImpl.h"
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

#include <mysql/mysql.h>

#include "CoralBase/Attribute.h"
#include "CoralBase/TimeStamp.h"
#include "PluginManager/PluginManager.h"
#include "RelationalAccess/AccessMode.h"
#include "RelationalAccess/IConnection.h"
#include "RelationalAccess/ICursor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ISession.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/ITransaction.h"
#include "RelationalAccess/RelationalServiceException.h"
#include "SealKernel/ComponentLoader.h"

#include "lsst/pex/exceptions.h"
#include "lsst/daf/persistence/DbStorageLocation.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/base/DateTime.h"

using lsst::daf::base::DateTime;

namespace lsst {
namespace daf {
namespace persistence {

// Static member variables
DbStorageImpl::State DbStorageImpl::initialized = DbStorageImpl::UNINITIALIZED;
seal::Handle<seal::Context> DbStorageImpl::context(0);
seal::Handle<seal::ComponentLoader> DbStorageImpl::loader(0);


// Helper conversion functions

/** Convert an lsst::daf::base::DateTime to a coral::TimeStamp.
 * Since the representation is identical, use a reinterpret_cast.
 * \param value Const reference to a DateTime
 * \return Const reference to a TimeStamp
 */
static coral::TimeStamp const& dt2ts(DateTime const& value) {
    return *(reinterpret_cast<coral::TimeStamp const*>(&value));
}

/** Convert a coral::TimeStamp to an lsst::daf::base::DateTime.
 * Since the representation is identical, use a reinterpret_cast.
 * \param value Const reference to a TimeStamp
 * \return Const reference to a DateTime
 */
static DateTime const& ts2dt(coral::TimeStamp const& value) {
    return *(reinterpret_cast<DateTime const*>(&value));
}


/** Constructor.
 * Initialize SEAL plugin manager and load CORAL relational service if not
 * already setup.
 */
DbStorageImpl::DbStorageImpl(void) : lsst::daf::base::Citizen(typeid(*this)) {
    if (initialized == UNINITIALIZED) {
        initialized = PENDING;
        context = new seal::Context;

        seal::PluginManager* pm = seal::PluginManager::get();
        pm->initialise(); // note British/Canadian spelling
        loader = new seal::ComponentLoader(context.get());
        loader->load("CORAL/Services/RelationalService");
        initialized = INITIALIZED;
    } else {
        while (initialized != INITIALIZED) {
            sleep(1);
        }
    }
}


/** Destructor.
 * End session if present.
 */
DbStorageImpl::~DbStorageImpl(void) {
    if (_session) {
       // if a transaction is active, preemptively roll it back to avoid deadlock in CORAL
       if (_session->transaction().isActive()) {
           _session->transaction().rollback();
       }
       _session.reset(0);
    }
    // Note the CORAL session is destroyed before the connection that created it
    _connection.reset(0);
}

/** Allow a Policy to be used to configure the DbStorage.
 * \param[in] policy
 */
void DbStorageImpl::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
}

/** Start a database session.
 * \param[in] location Physical database location
 * \param[in] am Access mode for the database (ReadOnly or Update)
 */
void DbStorageImpl::startSession(std::string const& location,
                                 coral::AccessMode am) {
    // Save the storage location for later use (by MySQL for now).
    _location = location;

    // Set the timezone for any DATE/TIME/TIMESTAMP fields.
    setenv("TZ", "UTC", 1);

    DbStorageLocation dbloc(location);

    // Query for the relational service.
    std::vector< seal::IHandle<coral::IRelationalService> > svcList;
    context->query(svcList);
    if (svcList.empty()) {
        throw lsst::pex::exceptions::Runtime("Unable to locate CORAL RelationalService");
    }

    // Use the connection string to get the relational domain.
    std::string connString(dbloc.getConnString());
    coral::IRelationalDomain& domain =
        svcList.front()->domainForConnection(connString);

    // first destroy old session, then old connection (at least in MySQLAccess/, the session
    // object has references to state that is internal to connection)
    _session.reset(0);
    
    // Use the domain to decode the connection string and create a connection.
    std::pair<std::string, std::string> databaseAndSchema =
        domain.decodeUserConnectionString(connString);
    _connection.reset(domain.newConnection(databaseAndSchema.first));
    if (_connection == 0) {
        throw lsst::pex::exceptions::Runtime(
            "Unable to connect to database with string: " + connString);
    }

    // Create a session with the appropriate access mode and login.
    _session.reset(_connection->newSession(databaseAndSchema.second, am));
    if (_session == 0) {
        throw lsst::pex::exceptions::Runtime("Unable to start database session");
    }
    _session->startUserSession(dbloc.getUsername(),
                               dbloc.getPassword());
    if (!_connection->isConnected()) {
        throw lsst::pex::exceptions::Runtime(
            "Unable to login to database with username: " +
            dbloc.getUsername());
    }
}

/** Set the database location to persist to.
 * \param[in] location Database connection string to insert into
 */
void DbStorageImpl::setPersistLocation(LogicalLocation const& location) {
    startSession(location.locString(), coral::Update);
}

/** Set the database location to retrieve from.
 * \param[in] location Database connection string to query
 */
void DbStorageImpl::setRetrieveLocation(LogicalLocation const& location) {
    startSession(location.locString(), coral::ReadOnly);
}

/** Start a transaction.
 */
void DbStorageImpl::startTransaction(void) {
    if (_session == 0) throw lsst::pex::exceptions::Runtime("Database session not initialized in DbStorage::startTransaction()");
    _session->transaction().start();
}

/** Start a transaction.
 */
void DbStorageImpl::endTransaction(void) {
    if (_session == 0) throw lsst::pex::exceptions::Runtime("Database session not initialized in DbStorage::endTransaction()");
    _session->transaction().commit();
}

/** Create a new table from an existing template table.
 * \param[in] tableName Name of the new table
 * \param[in] templateName Name of the existing template table
 * \param[in] mayAlreadyExist False (default) if the table must not be present
 *
 * Note: currently works with MySQL only.
 */
void DbStorageImpl::createTableFromTemplate(std::string const& tableName,
                                        std::string const& templateName,
                                        bool mayAlreadyExist) {
    DbStorageLocation dbloc(_location);
    MYSQL* db = mysql_init(0);
    if (db == 0) {
        throw lsst::pex::exceptions::Runtime(
            "Unable to allocate MySQL connection: " + _location);
    }
    unsigned int port = strtoul(dbloc.getPort().c_str(), 0, 10);
    if (mysql_real_connect(db,
                           dbloc.getHostname().c_str(),
                           dbloc.getUsername().c_str(),
                           dbloc.getPassword().c_str(),
                           dbloc.getDbName().c_str(),
                           port, 0, 0) == 0) {
        throw lsst::pex::exceptions::Runtime(
            "Unable to connect to MySQL database: " + _location);
    }

    std::string query = "CREATE TABLE ";
    if (mayAlreadyExist) query += "IF NOT EXISTS ";
    query += "`";
    query += tableName;
    query += "` LIKE `";
    query += templateName;
    query += "`";

    if (mysql_query(db, query.c_str()) != 0) {
        mysql_close(db);
        throw lsst::pex::exceptions::Runtime("Unable to create new table: " +
                                             tableName + " LIKE " +
                                             templateName);
    }
    mysql_close(db);
}

/** Drop a table.
 * \param[in] tableName Name of the table to drop
 */
void DbStorageImpl::dropTable(std::string const& tableName) {
    _session->nominalSchema().dropTable(tableName);
}

/** Truncate a table.
 * \param[in] tableName Name of the table to truncate
 */
void DbStorageImpl::truncateTable(std::string const& tableName) {
    _session->nominalSchema().truncateTable(tableName);
}

/** Set the table to insert rows into.
 * \param[in] tableName Name of the table
 */
void DbStorageImpl::setTableForInsert(std::string const& tableName) {
    if (_session == 0) throw lsst::pex::exceptions::Runtime("Database session not initialized in DbStorage::setTableForInsert()");
    _table = &(_session->nominalSchema().tableHandle(tableName));
    _table->dataEditor().rowBuffer(_rowBuffer);
}

/** Set the value to insert in a given column.
 * \param[in] columnName Name of the column
 * \param[in] value Value to set in the column
 */
template <typename T>
void DbStorageImpl::setColumn(std::string const& columnName, T const& value) {
    _rowBuffer[columnName].template data<T>() = value;
}

template<>
void DbStorageImpl::setColumn(std::string const& columnName, DateTime const& value) {
    _rowBuffer[columnName].data<coral::TimeStamp>() = dt2ts(value);
}

template<>
void DbStorageImpl::setColumn<long>(std::string const& columnName, long const& value) {
    if (sizeof(long) == sizeof(long long)) {
        _rowBuffer[columnName].data<long long>() = static_cast<long>(value);
    }
    else if (sizeof(long) == sizeof(int)) {
        _rowBuffer[columnName].data<int>() = static_cast<int>(value); 
    }
    else {
        _rowBuffer[columnName].data<long>() = value;
    }
}

/** Set a given column to NULL.
 * \param[in] columnName Name of the column
 */
void DbStorageImpl::setColumnToNull(std::string const& columnName) {
    _rowBuffer[columnName].setNull();
}

/** Insert the row.
 * Row values must have been set with setColumn() calls.
 */
void DbStorageImpl::insertRow(void) {
    if (_table == 0) throw lsst::pex::exceptions::Runtime("Insert table not initialized in DbStorage::insertRow()");
    _table->dataEditor().insertRow(_rowBuffer);
    for(std::size_t i = 0; i < _rowBuffer.size(); ++i) {
        _rowBuffer[i].setNull(false);
    }
}


/** Set the table to query (single-table queries only).
 * \param[in] tableName Name of the table
 */
void DbStorageImpl::setTableForQuery(std::string const& tableName) {
    if (_session == 0) throw lsst::pex::exceptions::Runtime("Database session not initialized in DbStorage::setTableForQuery()");
    _query.reset(_session->nominalSchema().newQuery());
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Unable to create new query in DbStorage::setTableForQuery()");
    _query->addToTableList(tableName);
    _condAttributeList.reset(new coral::AttributeList);
    _outAttributeList.reset(new coral::AttributeList);
}

/** Set a list of tables to query (multiple-table queries).
 * \param[in] tableNameList Vector of names of tables
 */
void DbStorageImpl::setTableListForQuery(
    std::vector<std::string> const& tableNameList) {
    if (_session == 0) throw lsst::pex::exceptions::Runtime("Database session not initialized in DbStorage::setTableListForQuery()");
    _query.reset(_session->nominalSchema().newQuery());
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Unable to create new query in DbStorage::setTableListForQuery()");
    for (std::vector<std::string>::const_iterator i = tableNameList.begin();
         i != tableNameList.end(); ++i) {
        _query->addToTableList(*i);
    }
    _condAttributeList.reset(new coral::AttributeList);
    _outAttributeList.reset(new coral::AttributeList);
}

/** Request a column in the query output.
 * \param[in] columnName Name of the column
 *
 * The order of outColumn() calls is the order of appearance in the output
 * row.  Use either outColumn() or outParam() but not both.
 */
void DbStorageImpl::outColumn(std::string const& columnName) {
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::outColumn()");
    _query->addToOutputList(columnName);
}

/** Request a column in the query output and bind a destination location.
 * \param[in] columnName Name of the column
 * \param[in] location Pointer to the destination
 *
 * The order of outParam() calls is the order of appearance in the output row.
 * Use either outColumn() or outParam() but not both.
 */
template <typename T>
void DbStorageImpl::outParam(std::string const& columnName, T* location) {
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::outParam()");
    _query->addToOutputList(columnName);
    if (_outAttributeList == 0) throw lsst::pex::exceptions::Runtime("Output attribute list not initialized in DbStorage::outParam()");
    _outAttributeList->extend<T>(columnName);
    (*_outAttributeList)[_outAttributeList->size() - 1].template bind<T>(*location);
}

template<>
void DbStorageImpl::outParam<DateTime>(std::string const& columnName,
                                        DateTime* location) {
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::outParam()");
    _query->addToOutputList(columnName);
    if (_outAttributeList == 0) throw lsst::pex::exceptions::Runtime("Output attribute list not initialized in DbStorage::outParam()");
    _outAttributeList->extend<coral::TimeStamp>(columnName);
    (*_outAttributeList)[_outAttributeList->size() - 1].bind<coral::TimeStamp>(
        *(reinterpret_cast<coral::TimeStamp*>(location)));
}

template<>
void DbStorageImpl::outParam<long>(std::string const& columnName,
                                   long* location) {
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::outParam()");
    _query->addToOutputList(columnName);
    if (_outAttributeList == 0) throw lsst::pex::exceptions::Runtime("Output attribute list not initialized in DbStorage::outParam()");
    if (sizeof(long) == sizeof(long long)) {
        _outAttributeList->extend<long long>(columnName);
        (*_outAttributeList)[_outAttributeList->size() - 1].bind<long long>(
            *(reinterpret_cast<long long*>(location)));
    }
    else if (sizeof(long) == sizeof(int)) {
        _outAttributeList->extend<int>(columnName);
        (*_outAttributeList)[_outAttributeList->size() - 1].bind<int>(
            *(reinterpret_cast<int*>(location)));
    }
    else {
        _outAttributeList->extend<long>(columnName);
        (*_outAttributeList)[_outAttributeList->size() - 1].bind<long>(
            *(reinterpret_cast<long*>(location)));
    }
}

/** Bind a value to a WHERE condition parameter.
 * \param[in] paramName Name of the parameter (prefixed by ":" in the WHERE
 * clause)
 * \param[in] value Value to be bound to the parameter.
 */
template <typename T>
void DbStorageImpl::condParam(std::string const& paramName, T const& value) {
    if (_condAttributeList == 0) throw lsst::pex::exceptions::Runtime("Condition attribute list not initialized in DbStorage::condParam()");
    _condAttributeList->extend<T>(paramName);
    (*_condAttributeList)[_condAttributeList->size() - 1].template data<T>() =
        value;
}

template<>
void DbStorageImpl::condParam<DateTime>(std::string const& paramName, DateTime const& value) {
    if (_condAttributeList == 0) throw lsst::pex::exceptions::Runtime("Condition attribute list not initialized in DbStorage::condParam()");
    _condAttributeList->extend<coral::TimeStamp>(paramName);
    (*_condAttributeList)[_condAttributeList->size() - 1].data<coral::TimeStamp>() = dt2ts(value);
}

template<>
void DbStorageImpl::condParam<long>(std::string const& paramName, long const &value) {
    if (_condAttributeList == 0) throw lsst::pex::exceptions::Runtime("Condition attribute list not initialized in DbStorage::condParam()");
    if (sizeof(long) == sizeof(long long)) {
        _condAttributeList->extend<long long>(paramName);
        (*_condAttributeList)[_condAttributeList->size() - 1].data<long long>() =
            static_cast<long long>(value);
    }
    else if (sizeof(long) == sizeof(int)) {
        _condAttributeList->extend<int>(paramName);
        (*_condAttributeList)[_condAttributeList->size() - 1].data<int>() =
            static_cast<int>(value);
    }
    else {
        _condAttributeList->extend<long>(paramName);
        (*_condAttributeList)[_condAttributeList->size() - 1].data<long>() = value;
    }
}

/** Request that the query output be sorted by an expression.  Multiple
 * expressions may be specified, in order.
 * \param[in] expression Text of the SQL expression
 */
void DbStorageImpl::orderBy(std::string const& expression) {
    _query->addToOrderList(expression);
}

/** Request that the query output be grouped by an expression.
 * \param[in] expression Text of the SQL expression
 */
void DbStorageImpl::groupBy(std::string const& expression) {
    _query->groupBy(expression);
}

/** Set the condition for the WHERE clause of the query.
 * \param[in] whereClause SQL text of the WHERE clause
 *
 * May include join conditions.
 */
void DbStorageImpl::setQueryWhere(std::string const& whereClause) {
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::setQueryWhere()");
    _query->setCondition(whereClause, *_condAttributeList);
}

/** Execute the query.
 */
void DbStorageImpl::query(void) {
    if (_outAttributeList == 0) throw lsst::pex::exceptions::Runtime("Output attribute list not initialized in DbStorage::query()");
    if (_query == 0) throw lsst::pex::exceptions::Runtime("Query not initialized in DbStorage::query()");
    if (_outAttributeList->size() > 0) {
        _query->defineOutput(*_outAttributeList);
    }
    _cursor = &(_query->execute());
}

/** Move to the next (first) row of the query result.
 * \return false if no more rows
 */
bool DbStorageImpl::next(void) {
    if (_cursor == 0) throw lsst::pex::exceptions::Runtime("Cursor not initialized in DbStorage::next()");
    return _cursor->next();
}

/** Get the value of a column of the query result row by position.
 * \param[in] pos Position of the column (starts at 0)
 * \return Reference to the value of the column
 */
template <typename T>
T const& DbStorageImpl::getColumnByPos(int pos) {
    if (_cursor == 0) throw lsst::pex::exceptions::Runtime("Cursor not initialized in DbStorage::getColumnByPos()");
    return _cursor->currentRow()[pos].template data<T>();
}

template<>
DateTime const& DbStorageImpl::getColumnByPos(int pos) {
    if (_cursor == 0) throw lsst::pex::exceptions::Runtime("Cursor not initialized in DbStorage::getColumnByPos()");
    return ts2dt(_cursor->currentRow()[pos].data<coral::TimeStamp>());
}

template<>
long const& DbStorageImpl::getColumnByPos(int pos) {
    if (_cursor == 0) throw lsst::pex::exceptions::Runtime("Cursor not initialized in DbStorage::getColumnByPos()");
    if (sizeof(long) == sizeof(long long)) {
        return *reinterpret_cast<long const*>(&_cursor->currentRow()[pos].data<long long>());
    }
    else if (sizeof(long) == sizeof(int)) {
        return *reinterpret_cast<long const *>(&_cursor->currentRow()[pos].data<int>());
    }
    else {
        return _cursor->currentRow()[pos].data<long>();
    }
}

/** Determine if the value of a column is NULL.
 * \param[in] pos Position of the column (starts at 0)
 * \return true if value is NULL
 */
bool DbStorageImpl::columnIsNull(int pos) {
    if (_cursor == 0) throw lsst::pex::exceptions::Runtime("Cursor not initialized in DbStorage::columnIsNull()");
    return _cursor->currentRow()[pos].isNull();
}

/** Indicate that query processing is finished.
 */
void DbStorageImpl::finishQuery(void) {
    _query.reset();
}


// Explicit template member function instantiations
// Ignore for doxygen processing.
//! \cond
template void DbStorageImpl::setColumn<>(std::string const& columnName, char const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, short const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, int const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, long const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, long long const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, float const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, double const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, std::string const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, bool const& value);
template void DbStorageImpl::setColumn<>(std::string const& columnName, DateTime const& value);

template void DbStorageImpl::outParam<>(std::string const& columnName, char* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, short* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, int* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, long* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, long long* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, float* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, double* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, std::string* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, bool* location);
template void DbStorageImpl::outParam<>(std::string const& columnName, DateTime* location);

template void DbStorageImpl::condParam<>(std::string const& paramName, char const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, short const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, int const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, long const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, long long const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, float const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, double const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, std::string const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, bool const& value);
template void DbStorageImpl::condParam<>(std::string const& paramName, DateTime const& value);

template char const& DbStorageImpl::getColumnByPos<>(int pos);
template short const& DbStorageImpl::getColumnByPos<>(int pos);
template int const& DbStorageImpl::getColumnByPos<>(int pos);
template long const& DbStorageImpl::getColumnByPos<>(int pos);
template long long const& DbStorageImpl::getColumnByPos<>(int pos);
template float const& DbStorageImpl::getColumnByPos<>(int pos);
template double const& DbStorageImpl::getColumnByPos<>(int pos);
template std::string const& DbStorageImpl::getColumnByPos<>(int pos);
template bool const& DbStorageImpl::getColumnByPos<>(int pos);
template DateTime const& DbStorageImpl::getColumnByPos<>(int pos);
//! \endcond

}}} // namespace lsst::daf::persistence
