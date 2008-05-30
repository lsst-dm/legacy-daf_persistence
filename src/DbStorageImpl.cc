// -*- lsst-c++ -*-

/** @file
 * @brief Implementation of DbStorageImpl class
 *
 * Uses MySQL C API directly.
 *
 * @author $Author$
 * @version $Revision$
 * @date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 *
 * @ingroup daf_persistence
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

#include "lsst/pex/exceptions.h"
#include "lsst/daf/persistence/DbStorageLocation.h"
#include "lsst/daf/persistence/LogicalLocation.h"
#include "lsst/daf/base/DateTime.h"

namespace dafPer = lsst::daf::persistence;
namespace dafBase = lsst::daf::base;
namespace pexExcept = lsst::pex::exceptions;
namespace pexPolicy = lsst::pex::policy;

namespace lsst {
namespace daf {
namespace persistence {

template <typename T>
struct BoundVarTraits {
public:
    static enum_field_types mysqlType;
    static bool isUnsigned;
};

}}} // namespace lsst::daf::persistence

template<> enum_field_types dafPer::BoundVarTraits<char>::mysqlType = MYSQL_TYPE_TINY;
template<> bool dafPer::BoundVarTraits<char>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<signed char>::mysqlType = MYSQL_TYPE_TINY;
template<> bool dafPer::BoundVarTraits<signed char>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<unsigned char>::mysqlType = MYSQL_TYPE_TINY;
template<> bool dafPer::BoundVarTraits<unsigned char>::isUnsigned = true;

template<> enum_field_types dafPer::BoundVarTraits<short>::mysqlType = MYSQL_TYPE_SHORT;
template<> bool dafPer::BoundVarTraits<short>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<unsigned short>::mysqlType = MYSQL_TYPE_SHORT;
template<> bool dafPer::BoundVarTraits<unsigned short>::isUnsigned = true;

template<> enum_field_types dafPer::BoundVarTraits<int>::mysqlType = MYSQL_TYPE_LONG;
template<> bool dafPer::BoundVarTraits<int>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<unsigned int>::mysqlType = MYSQL_TYPE_LONG;
template<> bool dafPer::BoundVarTraits<unsigned int>::isUnsigned = true;

template<> enum_field_types dafPer::BoundVarTraits<long long>::mysqlType = MYSQL_TYPE_LONGLONG;
template<> bool dafPer::BoundVarTraits<long long>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<unsigned long long>::mysqlType = MYSQL_TYPE_LONGLONG;
template<> bool dafPer::BoundVarTraits<unsigned long long>::isUnsigned = true;

template<> enum_field_types dafPer::BoundVarTraits<float>::mysqlType = MYSQL_TYPE_FLOAT;
template<> bool dafPer::BoundVarTraits<float>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<double>::mysqlType = MYSQL_TYPE_DOUBLE;
template<> bool dafPer::BoundVarTraits<double>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<dafBase::DateTime>::mysqlType = MYSQL_TYPE_DATETIME;
template<> bool dafPer::BoundVarTraits<dafBase::DateTime>::isUnsigned = false;

template<> enum_field_types dafPer::BoundVarTraits<std::string>::mysqlType = MYSQL_TYPE_STRING;
template<> bool dafPer::BoundVarTraits<std::string>::isUnsigned = false;


/** Constructor.
 */
dafPer::BoundVar::BoundVar(size_t size) :
    lsst::daf::base::Citizen(typeid(*this)), _data(new char[size]) {
}


/** Constructor.
 */
dafPer::DbStorageImpl::DbStorageImpl(void) :
    lsst::daf::base::Citizen(typeid(*this)), _db(0) {
}

/** Destructor.
 * End session if present.
 */
dafPer::DbStorageImpl::~DbStorageImpl(void) {
    if (_db) {
        mysql_close(_db);
        _db = 0;
    }
}

/** Allow a Policy to be used to configure the DbStorage.
 * @param[in] policy
 */
void dafPer::DbStorageImpl::setPolicy(pexPolicy::Policy::Ptr policy) {
}

/** Start a database session.
 * @param[in] location Physical database location
 * @param[in] am Access mode for the database (ReadOnly or Update)
 */
void dafPer::DbStorageImpl::startSession(std::string const& location) {
    // Set the timezone for any DATE/TIME/TIMESTAMP fields.
    setenv("TZ", "UTC", 1);

    DbStorageLocation dbloc(location);

    if (_db) {
        mysql_close(_db);
    }
    _db = mysql_init(0);

    unsigned int port = strtoul(dbloc.getPort().c_str(), 0, 10);
    if (mysql_real_connect(_db,
                           dbloc.getHostname().c_str(),
                           dbloc.getUsername().c_str(),
                           dbloc.getPassword().c_str(),
                           dbloc.getDbName().c_str(),
                           port, 0, 0) == 0) {
        throw pexExcept::Runtime(
            "Unable to connect to MySQL database: " + _location);
    }
}

/** Set the database location to persist to.
 * @param[in] location Database connection string to insert into
 */
void dafPer::DbStorageImpl::setPersistLocation(LogicalLocation const& location) {
    startSession(location.locString());
    _readonly = false;
}

/** Set the database location to retrieve from.
 * @param[in] location Database connection string to query
 */
void dafPer::DbStorageImpl::setRetrieveLocation(LogicalLocation const& location) {
    startSession(location.locString());
    _readonly = true;
}

/** Start a transaction.
 */
void dafPer::DbStorageImpl::startTransaction(void) {
    // autocommit(off)
    if (_db == 0) throw pexExcept::Runtime("Database session not initialized in DbStorage::startTransaction()");
    if (mysql_autocommit(_db, false)) throw pexExcept::Runtime("Unable to turn off autocommit");
}

/** End a transaction.
 */
void dafPer::DbStorageImpl::endTransaction(void) {
    if (_db == 0) throw pexExcept::Runtime("Database session not initialized in DbStorage::endTransaction()");
    if (mysql_commit(_db)) throw pexExcept::Runtime("Unable to commit transaction");
    if (mysql_autocommit(_db, true)) throw pexExcept::Runtime("Unable to turn on autocommit");
}

///////////////////////////////////////////////////////////////////////////////
// UTILITIES
///////////////////////////////////////////////////////////////////////////////

/** Execute a query string.
  */
void dafPer::DbStorageImpl::executeQuery(std::string const& query) {
    if (_db == 0) {
        throw pexExcept::Runtime(
            "No DB connection for query: " + query);
    }
    if (mysql_query(_db, query.c_str()) != 0) {
        mysql_close(_db);
        _db = 0;
        throw pexExcept::Runtime("Unable to execute query: " +
                                             query);
    }
}

/** Quote a name in ANSI-standard fashion.
  */
std::string dafPer::DbStorageImpl::quote(std::string const& name) {
    return '`' + name + '`';
}

///////////////////////////////////////////////////////////////////////////////
// TABLE OPERATIONS
///////////////////////////////////////////////////////////////////////////////

/** Create a new table from an existing template table.
 * @param[in] tableName Name of the new table
 * @param[in] templateName Name of the existing template table
 * @param[in] mayAlreadyExist False (default) if the table must not be present
 */
void dafPer::DbStorageImpl::createTableFromTemplate(std::string const& tableName,
                                        std::string const& templateName,
                                        bool mayAlreadyExist) {
    std::string query = "CREATE TABLE ";
    if (mayAlreadyExist) query += "IF NOT EXISTS ";
    query += quote(tableName) + " LIKE " + quote(templateName);
    executeQuery(query);
}

/** Drop a table.
 * @param[in] tableName Name of the table to drop
 */
void dafPer::DbStorageImpl::dropTable(std::string const& tableName) {
    executeQuery("DROP TABLE " + quote(tableName));
}

/** Truncate a table.
 * @param[in] tableName Name of the table to truncate
 */
void dafPer::DbStorageImpl::truncateTable(std::string const& tableName) {
    executeQuery("TRUNCATE TABLE " + quote(tableName));
}

///////////////////////////////////////////////////////////////////////////////
// PERSISTENCE
///////////////////////////////////////////////////////////////////////////////

/** Set the table to insert rows into.
 * @param[in] tableName Name of the table
 */
void dafPer::DbStorageImpl::setTableForInsert(std::string const& tableName) {
    if (_readonly) {
        throw pexExcept::Runtime("Attempt to insert into read-only database");
    }
    _insertTable = tableName;
    _inputVars.clear();
}

/** Set the value to insert in a given column.
 * @param[in] columnName Name of the column
 * @param[in] value Value to set in the column
 */
template <typename T>
void dafPer::DbStorageImpl::setColumn(std::string const& columnName, T const& value) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    size_t size = sizeof(T);
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName, BoundVar(size))).first;
    }
    else if (bv->second._length != size) {
        bv->second._data.reset(new char[size]);
    }
    bv->second._type = BoundVarTraits<T>::mysqlType;
    bv->second._isNull = false;
    bv->second._isUnsigned = BoundVarTraits<T>::isUnsigned;
    bv->second._length = size;
    memcpy(bv->second._data.get(), &value, size);
}

namespace lsst {
namespace daf {
namespace persistence {

template <>
void DbStorageImpl::setColumn(std::string const& columnName, std::string const& value) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    size_t size = value.length();
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName, BoundVar(size))).first;
    }
    else if (bv->second._length != size) {
        bv->second._data.reset(new char[size]);
    }
    bv->second._type = BoundVarTraits<std::string>::mysqlType;
    bv->second._isNull = false;
    bv->second._isUnsigned = BoundVarTraits<std::string>::isUnsigned;
    bv->second._length = size;
    memcpy(bv->second._data.get(), value.data(), size);
}

}}} // namespace lsst::daf::persistence

/** Set a given column to NULL.
 * @param[in] columnName Name of the column
 */
void dafPer::DbStorageImpl::setColumnToNull(std::string const& columnName) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName, BoundVar(1))).first;
    }
    bv->second._isNull = true;
    bv->second._length = 1;
}

/** Insert the row.
 * Row values must have been set with setColumn() calls.
 */
void dafPer::DbStorageImpl::insertRow(void) {
    if (_readonly) {
        throw pexExcept::Runtime("Attempt to insert into read-only database");
    }
    if (_insertTable.empty()) throw pexExcept::Runtime("Insert table not initialized in DbStorage::insertRow()");
    if (_inputVars.empty()) throw pexExcept::Runtime("No values to insert");

    std::string query = "INSERT INTO " + quote(_insertTable) = " (";

    std::auto_ptr<MYSQL_BIND> binder(new MYSQL_BIND[_inputVars.size()]);

    int j = 0;
    for (BoundVarMap::const_iterator it = _inputVars.begin();
         it != _inputVars.end(); ++it) {
        if (it != _inputVars.begin()) {
            query += ", ";
        }
        query += quote(it->first);

        // Bind variables
        MYSQL_BIND* bind = binder.get() + j;
        bind->buffer_type = it->second._type;
        bind->buffer = it->second._data.get();
        bind->buffer_length = it->second._length;
        bind->length = const_cast<unsigned long*>(&(it->second._length));
        bind->is_null = const_cast<my_bool*>(
            reinterpret_cast<my_bool const*>(&(it->second._isNull)));
        bind->is_unsigned = it->second._isUnsigned;
        bind->error = 0;
        ++j;
    }
    query += " VALUES (";
    for (size_t i = 0; i < _inputVars.size(); ++i) {
        if (i != 0) {
            query += ", ";
        }
        query += "?";
    }

    // Execute statement
    MYSQL_STMT* statement = mysql_stmt_init(_db);
    mysql_stmt_prepare(statement, query.c_str(), query.length());
    mysql_stmt_bind_param(statement, binder.get());
    mysql_stmt_execute(statement);
    mysql_stmt_close(statement);
}

///////////////////////////////////////////////////////////////////////////////
// RETRIEVAL
///////////////////////////////////////////////////////////////////////////////

/** Set the table to query (single-table queries only).
 * @param[in] tableName Name of the table
 */
void dafPer::DbStorageImpl::setTableForQuery(std::string const& tableName) {
    if (_db == 0) throw pexExcept::Runtime("Database session not initialized in DbStorage::setTableForQuery()");
    _queryTables.clear();
    _queryTables.push_back(tableName);
    _inputVars.clear();
    _outputVars.clear();
    _groupBy.clear();
    _orderBy.clear();
}

/** Set a list of tables to query (multiple-table queries).
 * @param[in] tableNameList Vector of names of tables
 */
void dafPer::DbStorageImpl::setTableListForQuery(
    std::vector<std::string> const& tableNameList) {
    if (_db == 0) throw pexExcept::Runtime("Database session not initialized in DbStorage::setTableListForQuery()");
    _queryTables = tableNameList;
    _inputVars.clear();
    _outputVars.clear();
    _groupBy.clear();
    _orderBy.clear();
}

/** Request a column in the query output.
 * @param[in] columnName Name of the column
 *
 * The order of outColumn() calls is the order of appearance in the output
 * row.  Use either outColumn() or outParam() but not both.
 */
void dafPer::DbStorageImpl::outColumn(std::string const& columnName) {
    // Bind output variable (with what size?)
}

/** Request a column in the query output and bind a destination location.
 * @param[in] columnName Name of the column
 * @param[in] location Pointer to the destination
 *
 * The order of outParam() calls is the order of appearance in the output row.
 * Use either outColumn() or outParam() but not both.
 */
template <typename T>
void dafPer::DbStorageImpl::outParam(std::string const& columnName, T* location) {
    size_t size = sizeof(T);
    if (typeid(T) == typeid(std::string)) {
        size = 4096;
    }
    BoundVarMap::iterator bv = _inputVars.insert(
        BoundVarMap::value_type(columnName, BoundVar(size))).first;
    // ... more binding stuff
}

/** Bind a value to a WHERE condition parameter.
 * @param[in] paramName Name of the parameter (prefixed by ":" in the WHERE
 * clause)
 * @param[in] value Value to be bound to the parameter.
 */
template <typename T>
void dafPer::DbStorageImpl::condParam(std::string const& paramName, T const& value) {
    // add to _inputVars
}

/** Request that the query output be sorted by an expression.  Multiple
 * expressions may be specified, in order.
 * @param[in] expression Text of the SQL expression
 */
void dafPer::DbStorageImpl::orderBy(std::string const& expression) {
    if (!_orderBy.empty()) {
        _orderBy += ", ";
    }
    _orderBy += expression;
}

/** Request that the query output be grouped by an expression.
 * @param[in] expression Text of the SQL expression
 */
void dafPer::DbStorageImpl::groupBy(std::string const& expression) {
    if (!_groupBy.empty()) {
        _groupBy += ", ";
    }
    _groupBy += expression;
}

/** Set the condition for the WHERE clause of the query.
 * @param[in] whereClause SQL text of the WHERE clause
 *
 * May include join conditions.
 */
void dafPer::DbStorageImpl::setQueryWhere(std::string const& whereClause) {
    // Go through whereClause looking for bound variables
    // Set up MYSQL_BIND array using information from inputVars
}

/** Execute the query.
 */
void dafPer::DbStorageImpl::query(void) {
    if (_outputVars.empty()) throw pexExcept::Runtime("Output attribute list not initialized in DbStorage::query()");
    // SELECT outVars FROM queryTables WHERE whereClause GROUP BY groupBy
    // ORDER BY orderBy
    std::string query = "SELECT ";
    // add outVars (quoted)
    query += " FROM ";
    // add queryTables (quoted)
    query += " WHERE ";
    // add whereClause
    if (!_groupBy.empty()) query += " GROUP BY " + _groupBy;
    if (!_orderBy.empty()) query += " ORDER BY " + _orderBy;

    MYSQL_STMT* statement = mysql_stmt_init(_db);
    mysql_stmt_prepare(statement, query.c_str(), query.length());
    mysql_stmt_bind_param(statement, binder.get());
    mysql_stmt_bind_result(statement, outBinder.get());
    mysql_stmt_execute(statement);
    mysql_stmt_close(statement);
    // mysql_store_result etc.
}

/** Move to the next (first) row of the query result.
 * @return false if no more rows
 */
bool dafPer::DbStorageImpl::next(void) {
    if (_cursor == 0) throw pexExcept::Runtime("Cursor not initialized in DbStorage::next()");
    return _cursor->next();
}

/** Get the value of a column of the query result row by position.
 * @param[in] pos Position of the column (starts at 0)
 * @return Reference to the value of the column
 */
template <typename T>
T const& dafPer::DbStorageImpl::getColumnByPos(int pos) {
    if (_cursor == 0) throw pexExcept::Runtime("Cursor not initialized in DbStorage::getColumnByPos()");
    return _cursor->currentRow()[pos].template data<T>();
}

/** Determine if the value of a column is NULL.
 * @param[in] pos Position of the column (starts at 0)
 * @return true if value is NULL
 */
bool dafPer::DbStorageImpl::columnIsNull(int pos) {
    if (_cursor == 0) throw pexExcept::Runtime("Cursor not initialized in DbStorage::columnIsNull()");
    return _cursor->currentRow()[pos].isNull();
}

/** Indicate that query processing is finished.
 */
void dafPer::DbStorageImpl::finishQuery(void) {
    // Clean up if needed
}


// Explicit template member function instantiations
// Ignore for doxygen processing.
//! @cond
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, char const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, short const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, int const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, long const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, long long const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, float const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, double const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, std::string const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, bool const& value);
template void dafPer::DbStorageImpl::setColumn<>(std::string const& columnName, dafBase::DateTime const& value);

template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, char* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, short* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, int* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, long* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, long long* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, float* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, double* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, std::string* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, bool* location);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, dafBase::DateTime* location);

template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, char const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, short const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, int const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, long const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, long long const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, float const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, double const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, std::string const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, bool const& value);
template void dafPer::DbStorageImpl::condParam<>(std::string const& paramName, dafBase::DateTime const& value);

template char const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template short const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template int const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template long const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template long long const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template float const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template double const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template std::string const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template bool const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
template dafBase::DateTime const& dafPer::DbStorageImpl::getColumnByPos<>(int pos);
//! @endcond
