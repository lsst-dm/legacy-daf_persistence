// -*- lsst-c++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010, 2016 LSST Corporation.
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
#include "boost/regex.hpp"

#include <stdlib.h>
#include <unistd.h>

#include <ctime>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>

#include <mysql/mysql.h>

#include "lsst/pex/exceptions.h"
#include "lsst/pex/logging/Trace.h"
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

template <size_t N>
struct IntegerTypeTraits {
public:
    static enum_field_types mysqlType;
};

template <typename T>
struct BoundVarTraits {
public:
    static enum_field_types mysqlType;
    static bool isUnsigned;
};

template<> enum_field_types
    IntegerTypeTraits<1>::mysqlType = MYSQL_TYPE_TINY;
template<> enum_field_types
    IntegerTypeTraits<2>::mysqlType = MYSQL_TYPE_SHORT;
template<> enum_field_types
    IntegerTypeTraits<4>::mysqlType = MYSQL_TYPE_LONG;
template<> enum_field_types
    IntegerTypeTraits<8>::mysqlType = MYSQL_TYPE_LONGLONG;

template <typename N> enum_field_types
    BoundVarTraits<N>::mysqlType =
    IntegerTypeTraits<sizeof(N)>::mysqlType;

template<> enum_field_types
    BoundVarTraits<bool>::mysqlType = MYSQL_TYPE_LONG;
template<> bool BoundVarTraits<bool>::isUnsigned = true;

template<> bool BoundVarTraits<char>::isUnsigned = false;
template<> bool BoundVarTraits<signed char>::isUnsigned = false;
template<> bool BoundVarTraits<unsigned char>::isUnsigned = true;
template<> bool BoundVarTraits<short>::isUnsigned = false;
template<> bool BoundVarTraits<unsigned short>::isUnsigned = true;
template<> bool BoundVarTraits<int>::isUnsigned = false;
template<> bool BoundVarTraits<unsigned int>::isUnsigned = true;
template<> bool BoundVarTraits<long>::isUnsigned = false;
template<> bool BoundVarTraits<unsigned long>::isUnsigned = true;
template<> bool BoundVarTraits<long long>::isUnsigned = false;
template<> bool BoundVarTraits<unsigned long long>::isUnsigned = true;

template<> enum_field_types
    BoundVarTraits<float>::mysqlType = MYSQL_TYPE_FLOAT;
template<> bool BoundVarTraits<float>::isUnsigned = false;

template<> enum_field_types
    BoundVarTraits<double>::mysqlType = MYSQL_TYPE_DOUBLE;
template<> bool BoundVarTraits<double>::isUnsigned = false;

template<> enum_field_types
    BoundVarTraits<dafBase::DateTime>::mysqlType = MYSQL_TYPE_DATETIME;
template<> bool BoundVarTraits<dafBase::DateTime>::isUnsigned = false;

template<> enum_field_types
    BoundVarTraits<std::string>::mysqlType = MYSQL_TYPE_VAR_STRING;
template<> bool BoundVarTraits<std::string>::isUnsigned = false;

}}} // namespace lsst::daf::persistence

///////////////////////////////////////////////////////////////////////////////
// BoundVar
///////////////////////////////////////////////////////////////////////////////

/** Default constructor.
 */
dafPer::BoundVar::BoundVar(void) :
    lsst::daf::base::Citizen(typeid(*this)), _data(0) {
}

/** Constructor from pointer.
  */
dafPer::BoundVar::BoundVar(void* location) :
    lsst::daf::base::Citizen(typeid(*this)), _data(location) {
}

/** Copy constructor.
  */
dafPer::BoundVar::BoundVar(BoundVar const& src) :
    lsst::daf::base::Citizen(typeid(*this)),
    _type(src._type), _isNull(src._isNull), _isUnsigned(src._isUnsigned),
    _length(src._length), _data(src._data) {
}

///////////////////////////////////////////////////////////////////////////////
// CONSTRUCTORS
///////////////////////////////////////////////////////////////////////////////

/** Default constructor.
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

///////////////////////////////////////////////////////////////////////////////
// SESSIONS
///////////////////////////////////////////////////////////////////////////////

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
        error("Unable to connect to MySQL database: " + _location);
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
    if (_db == 0) error("Database session not initialized "
                        "in DbStorage::startTransaction()", false);
    if (mysql_autocommit(_db, false)) error("Unable to turn off autocommit");
}

/** End a transaction.
 */
void dafPer::DbStorageImpl::endTransaction(void) {
    if (_db == 0) error("Database session not initialized "
                        "in DbStorage::endTransaction()", false);
    if (mysql_commit(_db)) error("Unable to commit transaction");
    if (mysql_autocommit(_db, true)) error("Unable to turn on autocommit");
}

///////////////////////////////////////////////////////////////////////////////
// UTILITIES
///////////////////////////////////////////////////////////////////////////////

/** Execute a query string.
  */
void dafPer::DbStorageImpl::executeQuery(std::string const& query) {
    if (_db == 0) {
        error("No DB connection for query: " + query, false);
    }
    lsst::pex::logging::TTrace<5>("daf.persistence.DbStorage",
                                  "Query: " + query);
    if (mysql_query(_db, query.c_str()) != 0) {
        error("Unable to execute query: " + query);
    }
}

/** Quote a name in ANSI-standard fashion.
  */
std::string dafPer::DbStorageImpl::quote(std::string const& name) {
    std::string::size_type pos = name.find('.');
    if (pos == std::string::npos) return '`' + name + '`';
    return '`' + std::string(name, 0, pos) + "`.`" +
        std::string(name, pos + 1) + '`';
}

void dafPer::DbStorageImpl::stError(std::string const& text) {
    error(text + " - * " + mysql_stmt_error(_statement), false);
}

void dafPer::DbStorageImpl::error(std::string const& text, bool mysqlCause) {
    if (mysqlCause) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, text + " - * " + mysql_error(_db));
    }
    else {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeError, text);
    }
}

void* dafPer::DbStorageImpl::allocateMemory(size_t size) {
    boost::shared_array<char> mem(new char[size]);
    _bindingMemory.push_back(mem);
    return mem.get();
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

/** Execute an arbitrary SQL statement.  Use primarily to perform server-side
 * computations or complex DDL.
 * \param[in] sqlStatement SQL statement to be executed.  Must not end in ";".
 */
void dafPer::DbStorageImpl::executeSql(std::string const& sqlStatement) {
    executeQuery(sqlStatement);
}

///////////////////////////////////////////////////////////////////////////////
// PERSISTENCE
///////////////////////////////////////////////////////////////////////////////

/** Set the table to insert rows into.
 * @param[in] tableName Name of the table
 */
void dafPer::DbStorageImpl::setTableForInsert(std::string const& tableName) {
    if (_readonly) {
        error("Attempt to insert into read-only database", false);
    }
    _insertTable = tableName;
    _inputVars.clear();
}

/** Set the value to insert in a given column.
 * @param[in] columnName Name of the column
 * @param[in] value Value to set in the column
 */
template <typename T>
void dafPer::DbStorageImpl::setColumn(std::string const& columnName,
                                      T const& value) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    size_t size = sizeof(T);
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName,
                                    BoundVar(allocateMemory(size)))).first;
    }
    else if (bv->second._length != size) {
        bv->second._data = allocateMemory(size);
    }
    bv->second._type = BoundVarTraits<T>::mysqlType;
    bv->second._isNull = false;
    bv->second._isUnsigned = BoundVarTraits<T>::isUnsigned;
    bv->second._length = size;
    memcpy(bv->second._data, &value, size);
}

template <>
void dafPer::DbStorageImpl::setColumn(std::string const& columnName,
                                      std::string const& value) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    size_t size = value.length();
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName,
                                    BoundVar(allocateMemory(size)))).first;
    }
    else if (bv->second._length != size) {
        bv->second._data = allocateMemory(size);
    }
    bv->second._type = BoundVarTraits<std::string>::mysqlType;
    bv->second._isNull = false;
    bv->second._isUnsigned = BoundVarTraits<std::string>::isUnsigned;
    bv->second._length = size;
    memcpy(bv->second._data, value.data(), size);
}

template <>
void dafPer::DbStorageImpl::setColumn(std::string const& columnName,
                                      dafBase::DateTime const& value) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    size_t size = sizeof(MYSQL_TIME);
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName,
                                    BoundVar(allocateMemory(size)))).first;
    }
    else if (bv->second._length != size) {
        bv->second._data = allocateMemory(size);
    }
    bv->second._type = BoundVarTraits<dafBase::DateTime>::mysqlType;
    bv->second._isNull = false;
    bv->second._isUnsigned = BoundVarTraits<dafBase::DateTime>::isUnsigned;
    bv->second._length = size;
    struct tm v = value.gmtime();
    MYSQL_TIME* t = reinterpret_cast<MYSQL_TIME*>(bv->second._data);
    t->year = v.tm_year + 1900;
    t->month = v.tm_mon + 1;
    t->day = v.tm_mday;
    t->hour = v.tm_hour;
    t->minute = v.tm_min;
    t->second = v.tm_sec;
    t->neg = false;
    t->second_part =
        static_cast<unsigned long>((value.nsecs() % 1000000000LL) / 1000);
}

/** Set a given column to NULL.
 * @param[in] columnName Name of the column
 */
void dafPer::DbStorageImpl::setColumnToNull(std::string const& columnName) {
    BoundVarMap::iterator bv = _inputVars.find(columnName);
    if (bv == _inputVars.end()) {
       bv = _inputVars.insert(
            BoundVarMap::value_type(columnName,
                                    BoundVar(allocateMemory(1)))).first;
    }
    bv->second._isNull = true;
    bv->second._length = 1;
}

/** Insert the row.
 * Row values must have been set with setColumn() calls.
 */
void dafPer::DbStorageImpl::insertRow(void) {
    if (_readonly) {
        error("Attempt to insert into read-only database", false);
    }
    if (_insertTable.empty()) error("Insert table not initialized in DbStorage::insertRow()", false);
    if (_inputVars.empty()) error("No values to insert", false);

    std::string query = "INSERT INTO " + quote(_insertTable) + " (";

    std::unique_ptr<MYSQL_BIND[]> binder(new MYSQL_BIND[_inputVars.size()]);
    memset(binder.get(), 0, _inputVars.size() * sizeof(MYSQL_BIND));

    int i = 0;
    for (BoundVarMap::iterator it = _inputVars.begin();
         it != _inputVars.end(); ++it) {
        if (it != _inputVars.begin()) {
            query += ", ";
        }
        query += quote(it->first);

        // Bind variables
        MYSQL_BIND& bind(binder[i]);
        BoundVar& bv(it->second);
        if (bv._isNull) {
            bind.buffer_type = MYSQL_TYPE_NULL;
        }
        else {
            bind.buffer_type = bv._type;
            bind.buffer = bv._data;
            bind.buffer_length = bv._length;
            bind.length = &(bv._length);
            bind.is_null = 0;
            bind.is_unsigned = bv._isUnsigned;
            bind.error = 0;
        }
        ++i;
    }
    query += ") VALUES (";
    for (size_t i = 0; i < _inputVars.size(); ++i) {
        if (i != 0) {
            query += ", ";
        }
        query += "?";
    }
    query += ")";

    // Execute statement
    // Guard statement with mysql_stmt_close()
    _statement = mysql_stmt_init(_db);
    if (_statement == 0) {
        error("Unable to initialize statement: " + query);
    }
    if (mysql_stmt_prepare(_statement, query.c_str(), query.length()) != 0) {
        stError("Unable to prepare statement: " + query);
    }
    if (mysql_stmt_bind_param(_statement, binder.get())) {
        stError("Unable to bind variables in: " + query);
    }
    if (mysql_stmt_execute(_statement) != 0) {
        stError("Unable to execute statement: " + query);
    }
    mysql_stmt_close(_statement);
    _statement = 0;
}

///////////////////////////////////////////////////////////////////////////////
// RETRIEVAL
///////////////////////////////////////////////////////////////////////////////

/** Set the table to query (single-table queries only).
 * @param[in] tableName Name of the table
 * @param[in] isExpr True if the name is actually a table expression
 */
void dafPer::DbStorageImpl::setTableForQuery(std::string const& tableName,
                                             bool isExpr) {
    if (_db == 0) error("Database session not initialized in DbStorage::setTableForQuery()", false);
    _queryTables.clear();
    _queryTables.push_back(isExpr ? tableName : quote(tableName));
    _inputVars.clear();
    _outputVars.clear();
    _outColumns.clear();
    _whereClause.clear();
    _groupBy.clear();
    _orderBy.clear();
    _statement = 0;
}

/** Set a list of tables to query (multiple-table queries).
 * @param[in] tableNameList Vector of names of tables
 */
void dafPer::DbStorageImpl::setTableListForQuery(
    std::vector<std::string> const& tableNameList) {
    if (_db == 0) error("Database session not initialized in DbStorage::setTableListForQuery()", false);
    for (std::vector<std::string>::const_iterator it = tableNameList.begin();
         it != tableNameList.end(); ++it) {
        _queryTables.push_back(quote(*it));
    }
    _inputVars.clear();
    _outputVars.clear();
    _outColumns.clear();
    _whereClause.clear();
    _groupBy.clear();
    _orderBy.clear();
    _statement = 0;
}

/** Request a column in the query output.
 * @param[in] columnName Name of the column
 * @param[in] isExpr True if the name is actually an expression
 *
 * The order of outColumn() calls is the order of appearance in the output
 * row.  Use either outColumn() or outParam() but not both.
 */
void dafPer::DbStorageImpl::outColumn(std::string const& columnName,
                                      bool isExpr) {
    std::string col = isExpr ? columnName : quote(columnName);
    _outColumns.push_back(col);
}

/** Request a column in the query output and bind a destination location.
 * @param[in] columnName Name of the column
 * @param[in] location Pointer to the destination
 * @param[in] isExpr True if the name is actually an expression
 *
 * The order of outParam() calls is the order of appearance in the output row.
 * Use either outColumn() or outParam() but not both.
 */
template <typename T>
void dafPer::DbStorageImpl::outParam(std::string const& columnName,
                                     T* location, bool isExpr) {
    std::string col = isExpr ? columnName : quote(columnName);
    _outColumns.push_back(col);
    size_t size = sizeof(T);
    std::pair<BoundVarMap::iterator, bool> pair = _outputVars.insert(
        BoundVarMap::value_type(col, BoundVar(location)));
    if (!pair.second) {
        error("Duplicate column name requested: " + columnName, false);
    }
    BoundVar& bv = pair.first->second;
    bv._type = BoundVarTraits<T>::mysqlType;
    bv._isNull = false;
    bv._isUnsigned = BoundVarTraits<T>::isUnsigned;
    bv._length = size;
}

template <>
void dafPer::DbStorageImpl::outParam(std::string const& columnName,
                                     std::string* location, bool isExpr) {
    std::string col = isExpr ? columnName : quote(columnName);
    _outColumns.push_back(col);
    size_t size = 4096;
    std::pair<BoundVarMap::iterator, bool> pair = _outputVars.insert(
        BoundVarMap::value_type(
            col, BoundVar(allocateMemory(size + sizeof(std::string*)))));
    if (!pair.second) {
        error("Duplicate column name requested: " + columnName, false);
    }
    BoundVar& bv = pair.first->second;
    *reinterpret_cast<std::string**>(bv._data) = location;
    bv._type = BoundVarTraits<std::string>::mysqlType;
    bv._isNull = false;
    bv._isUnsigned = BoundVarTraits<std::string>::isUnsigned;
    bv._length = size;
}

template <>
void dafPer::DbStorageImpl::outParam(std::string const& columnName,
                                     dafBase::DateTime* location,
                                     bool isExpr) {
    std::string col = isExpr ? columnName : quote(columnName);
    _outColumns.push_back(col);
    size_t size = sizeof(MYSQL_TIME);
    std::pair<BoundVarMap::iterator, bool> pair = _outputVars.insert(
        BoundVarMap::value_type(
            col, BoundVar(allocateMemory(size + sizeof(dafBase::DateTime*)))));
    if (!pair.second) {
        error("Duplicate column name requested: " + columnName, false);
    }
    BoundVar& bv = pair.first->second;
    *reinterpret_cast<dafBase::DateTime**>(bv._data) = location;
    bv._type = BoundVarTraits<dafBase::DateTime>::mysqlType;
    bv._isNull = false;
    bv._isUnsigned = BoundVarTraits<dafBase::DateTime>::isUnsigned;
    bv._length = size;
}

/** Bind a value to a WHERE condition parameter.
 * @param[in] paramName Name of the parameter (prefixed by ":" in the WHERE
 * clause)
 * @param[in] value Value to be bound to the parameter.
 */
template <typename T>
void dafPer::DbStorageImpl::condParam(std::string const& paramName, T const& value) {
    setColumn<T>(paramName, value);
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
    _whereClause = whereClause;
}

/** Execute the query.
 */
void dafPer::DbStorageImpl::query(void) {
    if (_outColumns.empty()) error("No output columns for query", false);

    // SELECT outVars FROM queryTables WHERE whereClause GROUP BY groupBy
    // ORDER BY orderBy

    // SELECT clause
    std::string query = "SELECT ";
    for (std::vector<std::string>::const_iterator it = _outColumns.begin();
         it != _outColumns.end(); ++it) {
        if (it != _outColumns.begin()) {
            query += ", ";
        }
        query += *it;
    }

    // FROM clause
    query += " FROM ";
    for (std::vector<std::string>::const_iterator it = _queryTables.begin();
         it != _queryTables.end(); ++it) {
        if (it != _queryTables.begin()) {
            query += ", ";
        }
        query += *it;
    }

    // WHERE clause
    std::vector<std::string> whereBindings;
    if (!_whereClause.empty()) {
        boost::regex re(":([A-Za-z_]+)");
        std::string result;
        std::back_insert_iterator<std::string> out(result);
        boost::regex_iterator<std::string::iterator> m; 
        for (boost::regex_iterator<std::string::iterator> i(
                _whereClause.begin(), _whereClause.end(), re);
             i != boost::regex_iterator<std::string::iterator>(); ++i) {
            m = i;
            std::copy(m->prefix().first, m->prefix().second, out);
            *out++ = '?';
            assert(m->size() == 2);
            whereBindings.push_back(m->str(1));
        }
        if (m != boost::regex_iterator<std::string::iterator>()) {
            std::copy(m->suffix().first, m->suffix().second, out);
        }
        else {
            std::copy(_whereClause.begin(), _whereClause.end(), out);
        }
        query += " WHERE " + result;
    }

    // GROUP BY clause
    if (!_groupBy.empty()) query += " GROUP BY " + _groupBy;

    // ORDER BY clause
    if (!_orderBy.empty()) query += " ORDER BY " + _orderBy;


    // Create bindings for input WHERE clause variables, if any

    std::unique_ptr<MYSQL_BIND[]> inBinder(
        new MYSQL_BIND[whereBindings.size()]);
    memset(inBinder.get(), 0, whereBindings.size() * sizeof(MYSQL_BIND));
    for (size_t i = 0; i < whereBindings.size(); ++i) {
        MYSQL_BIND& bind(inBinder[i]);
        BoundVarMap::iterator it = _inputVars.find(whereBindings[i]);
        if (it == _inputVars.end()) {
            error("Unbound variable in WHERE clause: " + whereBindings[i],
                  false);
        }
        BoundVar& bv = it->second;
        bind.buffer_type = bv._type;
        bind.buffer = bv._data;
        bind.buffer_length = bv._length;
        bind.is_null = 0;
        bind.is_unsigned = bv._isUnsigned;
        bind.error = 0;
    }


    // Initialize and prepare statement

    _statement = mysql_stmt_init(_db);
    if (!_statement) {
        error("Unable to initialize prepared statement");
    }

    if (mysql_stmt_prepare(_statement, query.c_str(), query.length()) != 0) {
        stError("Unable to prepare statement: " + query);
    }


    // Check number of input parameters and bind them
    unsigned int params = mysql_stmt_param_count(_statement);
    if (_whereClause.empty()) {
        if (params != 0) {
            error("Unbound WHERE clause parameters: " + query, false);
        }
    }
    else {
        if (params != whereBindings.size()) {
            error("Mismatch in number of WHERE clause parameters: " + query,
                  false);
        }
        if (mysql_stmt_bind_param(_statement, inBinder.get())) {
            stError("Unable to bind WHERE parameters: " + query);
        }
    }

    // Check number of result columns
    MYSQL_RES* queryMetadata = mysql_stmt_result_metadata(_statement);
    if (!queryMetadata) {
        stError("No query metadata: " + query);
    }
    _numResultFields = mysql_num_fields(queryMetadata);
    if (static_cast<unsigned int>(_numResultFields) != _outColumns.size()) {
        error("Mismatch in number of SELECT items: " + query, false);
    }


    // Execute query

    if (mysql_stmt_execute(_statement) != 0) {
        stError("MySQL query failed: " + query);
    }


    // Create bindings for output variables

    _resultFields = mysql_fetch_fields(queryMetadata);

    std::unique_ptr<MYSQL_BIND[]> outBinder(new MYSQL_BIND[_numResultFields]);
    memset(outBinder.get(), 0, _numResultFields * sizeof(MYSQL_BIND));
    _fieldLengths.reset(new unsigned long[_numResultFields]);
    _fieldNulls.reset(new my_bool[_numResultFields]);

    for (int i = 0; i < _numResultFields; ++i) {
        MYSQL_BIND& bind(outBinder[i]);
        if (_outputVars.empty()) {
            bind.buffer_type = MYSQL_TYPE_STRING;
            bind.buffer = 0;
            bind.buffer_length = 0;
            bind.length = &(_fieldLengths[i]);
            bind.is_null = &(_fieldNulls[i]);
            bind.is_unsigned = (_resultFields[i].flags & UNSIGNED_FLAG) != 0;
            bind.error = 0;
        }
        else {
            BoundVarMap::iterator it = _outputVars.find(_outColumns[i]);
            if (it == _outputVars.end()) {
                error("Unbound variable in SELECT clause: " + _outColumns[i],
                      false);
            }
            BoundVar& bv = it->second;

            bind.buffer_type = bv._type;
            if (bv._type == BoundVarTraits<std::string>::mysqlType) {
                bind.buffer = reinterpret_cast<char*>(bv._data) +
                    sizeof(std::string*);
            }
            else if (bv._type == BoundVarTraits<dafBase::DateTime>::mysqlType) {
                bind.buffer = reinterpret_cast<char*>(bv._data) +
                    sizeof(std::string*);
            }
            else {
                bind.buffer = bv._data;
            }
            bind.buffer_length = bv._length;
            bind.length = &(_fieldLengths[i]);
            bind.is_null = &(_fieldNulls[i]);
            bind.is_unsigned = bv._isUnsigned;
            bind.error = 0;
        }
    }
    if (mysql_stmt_bind_result(_statement, outBinder.get())) {
        stError("Unable to bind results: " + query);
    }
}

/** Move to the next (first) row of the query result.
 * @return false if no more rows
 */
bool dafPer::DbStorageImpl::next(void) {
    if (_statement == 0) {
        error("Statement not initialized in DbStorage::next()", false);
    }
    int ret = mysql_stmt_fetch(_statement);
    if (ret == 0) {
        // Fix up strings and DateTimes
        if (!_outputVars.empty()) {
            for (size_t i = 0; i < _outColumns.size(); ++i) {
                BoundVarMap::iterator bvit = _outputVars.find(_outColumns[i]);
                if (bvit == _outputVars.end()) {
                    error("Unbound variable in SELECT clause: " +
                          _outColumns[i], false);
                }
                BoundVar& bv = bvit->second;
                if (bv._type == BoundVarTraits<std::string>::mysqlType) {
                    **reinterpret_cast<std::string**>(bv._data) =
                        std::string(reinterpret_cast<char*>(bv._data) +
                                    sizeof(std::string*), _fieldLengths[i]);
                }
                else if (bv._type ==
                         BoundVarTraits<dafBase::DateTime>::mysqlType) {
                    char* cp = reinterpret_cast<char*>(bv._data) +
                        sizeof(dafBase::DateTime*);
                    MYSQL_TIME* t = reinterpret_cast<MYSQL_TIME*>(cp);
                    **reinterpret_cast<dafBase::DateTime**>(bv._data) =
                        dafBase::DateTime(t->year, t->month, t->day,
                                          t->hour, t->minute, t->second,
                                          dafBase::DateTime::UTC);
                }
            }
        }
        return true;
    }
    if (ret == MYSQL_NO_DATA) return false;
    if (ret == MYSQL_DATA_TRUNCATED && _outputVars.empty()) return true;
    stError("Error fetching next row");
    return false;
}

/** Get the value of a column of the query result row by position.
 * @param[in] pos Position of the column (starts at 0)
 * @return Reference to the value of the column
 */
template <typename T>
T const& dafPer::DbStorageImpl::getColumnByPos(int pos) {
    if (pos > _numResultFields) {
        std::ostringstream os;
        os << "Nonexistent column: " << pos;
        error(os.str(), false);
    }
    MYSQL_BIND bind;
    memset(&bind, 0, sizeof(MYSQL_BIND));
    static T t;
    bind.buffer_type = BoundVarTraits<T>::mysqlType;
    bind.is_unsigned = BoundVarTraits<T>::isUnsigned;
    bind.buffer = &t;
    bind.buffer_length = sizeof(T);
    bind.length = &(_fieldLengths[pos]);
    bind.is_null = &(_fieldNulls[pos]);
    if (mysql_stmt_fetch_column(_statement, &bind, pos, 0)) {
        std::ostringstream os;
        os << "Error fetching column: " << pos;
        error(os.str(), false);
    }
    return t;
}

template <>
std::string const& dafPer::DbStorageImpl::getColumnByPos(int pos) {
    if (pos > _numResultFields) {
        std::ostringstream os;
        os << "Nonexistent column: " << pos;
        error(os.str(), false);
    }
    MYSQL_BIND bind;
    memset(&bind, 0, sizeof(MYSQL_BIND));
    if (_resultFields[pos].type == MYSQL_TYPE_BIT) {
        error("Invalid type for string retrieval", false);
    }
    std::unique_ptr<char[]> t(new char[_fieldLengths[pos]]);
    bind.buffer_type = BoundVarTraits<std::string>::mysqlType;
    bind.is_unsigned = BoundVarTraits<std::string>::isUnsigned;
    bind.buffer = t.get();
    bind.buffer_length = _fieldLengths[pos];
    bind.length = &(_fieldLengths[pos]);
    bind.is_null = &(_fieldNulls[pos]);
    if (mysql_stmt_fetch_column(_statement, &bind, pos, 0)) {
        std::ostringstream os;
        os << "Error fetching string column: " << pos;
        stError(os.str());
    }
    static std::string s;
    s = std::string(t.get(), _fieldLengths[pos]);
    return s;
}

template <>
dafBase::DateTime const& dafPer::DbStorageImpl::getColumnByPos(int pos) {
    if (pos > _numResultFields) {
        std::ostringstream os;
        os << "Nonexistent column: " << pos;
        error(os.str(), false);
    }
    MYSQL_BIND bind;
    memset(&bind, 0, sizeof(MYSQL_BIND));
    if (_resultFields[pos].type != MYSQL_TYPE_TIME &&
        _resultFields[pos].type != MYSQL_TYPE_DATE &&
        _resultFields[pos].type != MYSQL_TYPE_DATETIME &&
        _resultFields[pos].type != MYSQL_TYPE_TIMESTAMP) {
        error("Invalid type for DateTime retrieval", false);
    }
    static MYSQL_TIME t;
    bind.buffer_type = BoundVarTraits<dafBase::DateTime>::mysqlType;
    bind.is_unsigned = BoundVarTraits<dafBase::DateTime>::isUnsigned;
    bind.buffer = &t;
    bind.buffer_length = sizeof(MYSQL_TIME);
    bind.length = &(_fieldLengths[pos]);
    bind.is_null = &(_fieldNulls[pos]);
    if (mysql_stmt_fetch_column(_statement, &bind, pos, 0)) {
        std::ostringstream os;
        os << "Error fetching DateTime column: " << pos;
        stError(os.str());
    }
    static dafBase::DateTime v;
    v = dafBase::DateTime(t.year, t.month, t.day, t.hour, t.minute, t.second,
                          dafBase::DateTime::UTC);
    return v;
}

/** Determine if the value of a column is NULL.
 * @param[in] pos Position of the column (starts at 0)
 * @return true if value is NULL
 */
bool dafPer::DbStorageImpl::columnIsNull(int pos) {
    if (pos > _numResultFields) {
        std::ostringstream os;
        os << "Nonexistent column: " << pos;
        error(os.str(), false);
    }
    return _fieldNulls[pos];
}

/** Indicate that query processing is finished.
 */
void dafPer::DbStorageImpl::finishQuery(void) {
    mysql_stmt_close(_statement);
    _statement = 0;
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

template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, char* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, short* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, int* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, long* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, long long* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, float* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, double* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, std::string* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, bool* location, bool isExpr);
template void dafPer::DbStorageImpl::outParam<>(std::string const& columnName, dafBase::DateTime* location, bool isExpr);

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
