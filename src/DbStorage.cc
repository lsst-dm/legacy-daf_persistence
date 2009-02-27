// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of DbStorage class
 *
 * Forwards all methods to implementation class.
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

#include "lsst/daf/base/DateTime.h"
#include "lsst/daf/persistence/DbStorage.h"
#include "lsst/daf/persistence/DbStorageImpl.h"

using lsst::daf::base::DateTime;

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
*/
DbStorage::DbStorage(void) : Storage(typeid(*this)), _impl(new DbStorageImpl) {
}

/** Constructor with subclass type.
 * \param[in] type typeid() of subclass
 */
DbStorage::DbStorage(std::type_info const& type) :
    Storage(type), _impl(new DbStorageImpl) {
}

/** Minimal destructor.
 */
DbStorage::~DbStorage(void) {
}

/** Allow a policy to be used to configure the DbStorage.
 * \param[in] policy
 */
void DbStorage::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
    _impl->setPolicy(policy);
}

/** Set the database location to persist to.
 * \param[in] location Database connection string to insert to.
 */
void DbStorage::setPersistLocation(LogicalLocation const& location) {
    _impl->setPersistLocation(location);
}

/** Set the database location to retrieve from.
 * \param[in] location Database connection string to query.
 */
void DbStorage::setRetrieveLocation(LogicalLocation const& location) {
    _impl->setRetrieveLocation(location);
}

/** Start a transaction.
 */
void DbStorage::startTransaction(void) {
    _impl->startTransaction();
}

/** End a transaction.
 */
void DbStorage::endTransaction(void) {
    _impl->endTransaction();
}

/** Create a new table from an existing template table.
 * \param[in] tableName Name of the new table
 * \param[in] templateName Name of the existing template table
 * \param[in] mayAlreadyExist False (default) if the table must not be present
 *
 * Note: currently works with MySQL only.
 */
void DbStorage::createTableFromTemplate(std::string const& tableName,
                                        std::string const& templateName,
                                        bool mayAlreadyExist) {
    _impl->createTableFromTemplate(tableName, templateName, mayAlreadyExist);
}

/** Drop a table.
 * \param[in] tableName Name of the table to drop
 */
void DbStorage::dropTable(std::string const& tableName) {
    _impl->dropTable(tableName);
}

/** Truncate a table.
 * \param[in] tableName Name of the table to truncate
 */
void DbStorage::truncateTable(std::string const& tableName) {
    _impl->truncateTable(tableName);
}

/** Execute an arbitrary SQL statement.  Use primarily to perform server-side
  * computations or complex DDL.
 * \param[in] sqlStatement SQL statement to be executed.  Must not end in ";".
 */
void DbStorage::executeSql(std::string const& sqlStatement) {
    _impl->executeSql(sqlStatement);
}

/** Set the table to insert rows into.
 * \param[in] tableName Name of the table
 */
void DbStorage::setTableForInsert(std::string const& tableName) {
    _impl->setTableForInsert(tableName);
}

/** Set the value to insert in a given column.
 * \param[in] columnName Name of the column
 * \param[in] value Value to set in the column
 */
template <typename T>
void DbStorage::setColumn(std::string const& columnName, T const& value) {
    _impl->setColumn(columnName, value);
}

/** Set a given column to NULL.
 * \param[in] columnName Name of the column
 */
void DbStorage::setColumnToNull(std::string const& columnName) {
    _impl->setColumnToNull(columnName);
}

/** Insert the row.
 * Row values must have been set with setColumn() calls.
 */
void DbStorage::insertRow(void) {
    _impl->insertRow();
}


/** Set the table to query (single-table queries only).
 * \param[in] tableName Name of the table
 */
void DbStorage::setTableForQuery(std::string const& tableName) {
    _impl->setTableForQuery(tableName);
}

/** Set a list of tables to query (multiple-table queries).
 * \param[in] tableNameList Vector of names of tables
 */
void DbStorage::setTableListForQuery(
    std::vector<std::string> const& tableNameList) {
    _impl->setTableListForQuery(tableNameList);
}

/** Request a column in the query output.
 * \param[in] columnName Name of the column
 *
 * The order of outColumn() calls is the order of appearance in the output
 * row.  Use either outColumn() or outParam() but not both.
 */
void DbStorage::outColumn(std::string const& columnName) {
    _impl->outColumn(columnName);
}

/** Request a column in the query output and bind a destination location.
 * \param[in] columnName Name of the column
 * \param[in] location Pointer to the destination
 *
 * The order of outParam() calls is the order of appearance in the output row.
 * Use either outColumn() or outParam() but not both.
 */
template <typename T>
void DbStorage::outParam(std::string const& columnName, T* location) {
    _impl->outParam(columnName, location);
}

/** Bind a value to a WHERE condition parameter.
 * \param[in] paramName Name of the parameter (prefixed by ":" in the WHERE
 * clause)
 * \param[in] value Value to be bound to the parameter.
 */
template <typename T>
void DbStorage::condParam(std::string const& paramName, T const& value) {
    _impl->condParam(paramName, value);
}

/** Request that the query output be sorted by an expression.  Multiple
 * expressions may be specified, in order.
 * \param[in] expression Text of the SQL expression
 */
void DbStorage::orderBy(std::string const& expression) {
    _impl->orderBy(expression);
}

/** Request that the query output be grouped by an expression.
 * \param[in] expression Text of the SQL expression
 */
void DbStorage::groupBy(std::string const& expression) {
    _impl->groupBy(expression);
}

/** Set the condition for the WHERE clause of the query.
 * \param[in] whereClause SQL text of the WHERE clause
 *
 * May include join conditions.
 */
void DbStorage::setQueryWhere(std::string const& whereClause) {
    _impl->setQueryWhere(whereClause);
}

/** Execute the query.
 */
void DbStorage::query(void) {
    _impl->query();
}

/** Move to the next (first) row of the query result.
 * \return false if no more rows
 */
bool DbStorage::next(void) {
    return _impl->next();
}

/** Get the value of a column of the query result row by position.
 * \param[in] pos Position of the column (starts at 0)
 */
template <typename T>
T const& DbStorage::getColumnByPos(int pos) {
    return _impl->getColumnByPos<T>(pos);
}

/** Determine if the value of a column is NULL.
 * \param[in] pos Position of the column (starts at 0)
 */
bool DbStorage::columnIsNull(int pos) {
    return _impl->columnIsNull(pos);
}

/** Indicate that query processing is finished.
 * Must be called after next() returns false; no getColumnByPos() or
 * columnIsNull() calls may be made after this method is called.
 */
void DbStorage::finishQuery(void) {
    _impl->finishQuery();
}

// Explicit template member function instantiations.
// Ignore for doxygen processing.
//! \cond
template void DbStorage::setColumn<>(std::string const& columnName, char const& value);
template void DbStorage::setColumn<>(std::string const& columnName, short const& value);
template void DbStorage::setColumn<>(std::string const& columnName, int const& value);
template void DbStorage::setColumn<>(std::string const& columnName, long const& value);
template void DbStorage::setColumn<>(std::string const& columnName, long long const& value);
template void DbStorage::setColumn<>(std::string const& columnName, float const& value);
template void DbStorage::setColumn<>(std::string const& columnName, double const& value);
template void DbStorage::setColumn<>(std::string const& columnName, std::string const& value);
template void DbStorage::setColumn<>(std::string const& columnName, bool const& value);
template void DbStorage::setColumn<>(std::string const& columnName, DateTime const& value);

template void DbStorage::outParam<>(std::string const& columnName, char* location);
template void DbStorage::outParam<>(std::string const& columnName, short* location);
template void DbStorage::outParam<>(std::string const& columnName, int* location);
template void DbStorage::outParam<>(std::string const& columnName, long* location);
template void DbStorage::outParam<>(std::string const& columnName, long long* location);
template void DbStorage::outParam<>(std::string const& columnName, float* location);
template void DbStorage::outParam<>(std::string const& columnName, double* location);
template void DbStorage::outParam<>(std::string const& columnName, std::string* location);
template void DbStorage::outParam<>(std::string const& columnName, bool* location);
template void DbStorage::outParam<>(std::string const& columnName, DateTime* location);

template void DbStorage::condParam<>(std::string const& paramName, char const& value);
template void DbStorage::condParam<>(std::string const& paramName, short const& value);
template void DbStorage::condParam<>(std::string const& paramName, int const& value);
template void DbStorage::condParam<>(std::string const& paramName, long const& value);
template void DbStorage::condParam<>(std::string const& paramName, long long const& value);
template void DbStorage::condParam<>(std::string const& paramName, float const& value);
template void DbStorage::condParam<>(std::string const& paramName, double const& value);
template void DbStorage::condParam<>(std::string const& paramName, std::string const& value);
template void DbStorage::condParam<>(std::string const& paramName, bool const& value);
template void DbStorage::condParam<>(std::string const& paramName, DateTime const& value);

template char const& DbStorage::getColumnByPos<>(int pos);
template short const& DbStorage::getColumnByPos<>(int pos);
template int const& DbStorage::getColumnByPos<>(int pos);
template long const& DbStorage::getColumnByPos<>(int pos);
template long long const& DbStorage::getColumnByPos<>(int pos);
template float const& DbStorage::getColumnByPos<>(int pos);
template double const& DbStorage::getColumnByPos<>(int pos);
template std::string const& DbStorage::getColumnByPos<>(int pos);
template bool const& DbStorage::getColumnByPos<>(int pos);
template DateTime const& DbStorage::getColumnByPos<>(int pos);
//! \endcond

}}} // namespace lsst::daf::persistence
