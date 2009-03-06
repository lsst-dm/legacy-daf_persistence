// -*- lsst-c++ -*-

/** \file
 * \brief Implementation of DbTsvStorage class
 *
 * Writes rows to file, then uses "LOAD DATA INFILE" to load.
 *
 * \author $Author: ktlim $
 * \version $Revision: 2336 $
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

#include "lsst/daf/persistence/DbTsvStorage.h"

#include <iomanip>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <mysql/mysql.h>

#include "lsst/pex/exceptions.h"
#include "lsst/daf/base/DateTime.h"
#include "lsst/daf/persistence/DbStorageLocation.h"
#include "lsst/daf/persistence/LogicalLocation.h"

using lsst::daf::base::DateTime;

namespace lsst {
namespace daf {
namespace persistence {

/** Constructor.
*/
DbTsvStorage::DbTsvStorage(void) : _saveTemp(false) {
}

/** Minimal destructor.
 */
DbTsvStorage::~DbTsvStorage(void) {
}

/** Allow a policy to be used to configure the DbTsvStorage.
 * \param[in] policy
 */
void DbTsvStorage::setPolicy(lsst::pex::policy::Policy::Ptr policy) {
    _tempPath = "/tmp";
    if (policy && policy->exists("TempPath")) {
        _tempPath = policy->getString("TempPath");
    }
    if (policy && policy->exists("SaveTemp") && policy->getBool("SaveTemp")) {
        _saveTemp = true;
    }
}

/** Set the database location to persist to.
 * \param[in] location Database connection string to insert to.
 */
void DbTsvStorage::setPersistLocation(LogicalLocation const& location) {
    _persisting = true;
    _location = location.locString();
    // Set the timezone for any DATE/TIME/DATETIME fields.
    setenv("TZ", "UTC", 1);
}

/** Set the database location to retrieve from.
 * \param[in] location Database connection string to retrieve from.
 */
void DbTsvStorage::setRetrieveLocation(LogicalLocation const& location) {
    _persisting = false;
    DbStorage::setRetrieveLocation(location);
}

/** Start a transaction.
 */
void DbTsvStorage::startTransaction(void) {
    if (!_persisting) DbStorage::startTransaction();
}

/** End a transaction.
 */
void DbTsvStorage::endTransaction(void) {
    if (!_persisting) {
        DbStorage::endTransaction();
        return;
    }

    // close stream
    _osp->close();

    MYSQL* db = mysql_init(0);
    if (db == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException, "Unable to allocate MySQL connection");
    }
    DbStorageLocation dbLoc(_location);
    unsigned int port = strtoul(dbLoc.getPort().c_str(), 0, 10);
    if (mysql_real_connect(db,
                           dbLoc.getHostname().c_str(),
                           dbLoc.getUsername().c_str(),
                           dbLoc.getPassword().c_str(),
                           dbLoc.getDbName().c_str(),
                           port, 0,
                           CLIENT_COMPRESS | CLIENT_LOCAL_FILES) == 0) {
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException,
            "Unable to connect to MySQL database: " + _location);
    }

    std::string query = "LOAD DATA LOCAL INFILE";
    query += " '";
    query += _fileName;
    query += "'";
    query += " REPLACE";
    query += " INTO TABLE `" + _tableName;
    query += "` (";
    for (std::map<std::string, int>::const_iterator it = _colMap.begin();
         it != _colMap.end(); ++it) {
        _rowBuffer[it->second] = it->first;
    }
    for (std::vector<std::string>::const_iterator it = _rowBuffer.begin();
         it != _rowBuffer.end(); ++it) {
        if (it != _rowBuffer.begin()) query += " ,";
        query += *it;
    }
    query += ")";

    if (mysql_query(db, query.c_str()) != 0) {
        mysql_close(db);
        throw LSST_EXCEPT(lsst::pex::exceptions::RuntimeErrorException,
            "Unable to load data into database table: " + _tableName);
    }
    mysql_close(db);

    // unlink file
    if (!_saveTemp) {
        unlink(_fileName);
    }
    delete[] _fileName;
}

/** Create a new table from an existing template table.
 * \param[in] tableName Name of the new table
 * \param[in] templateName Name of the existing template table
 * \param[in] mayAlreadyExist False (default) if the table must not be present
 */
void DbTsvStorage::createTableFromTemplate(std::string const& tableName,
                                        std::string const& templateName,
                                        bool mayAlreadyExist) {
    if (_persisting) {
        DbStorage dbs;
        dbs.setPersistLocation(LogicalLocation(_location));
        dbs.startTransaction();
        dbs.createTableFromTemplate(tableName, templateName, mayAlreadyExist);
        dbs.endTransaction();
    }
    else {
        DbStorage::createTableFromTemplate(tableName, templateName,
                                           mayAlreadyExist);
    }
}

/** Drop a table.
 * \param[in] tableName Name of the table to drop
 */
void DbTsvStorage::dropTable(std::string const& tableName) {
    if (_persisting) {
        DbStorage dbs;
        dbs.setPersistLocation(LogicalLocation(_location));
        dbs.startTransaction();
        dbs.dropTable(tableName);
        dbs.endTransaction();
    }
    else {
        DbStorage::dropTable(tableName);
    }
}

/** Truncate a table.
 * \param[in] tableName Name of the table to truncate
 */
void DbTsvStorage::truncateTable(std::string const& tableName) {
    if (_persisting) {
        DbStorage dbs;
        dbs.setPersistLocation(LogicalLocation(_location));
        dbs.startTransaction();
        dbs.truncateTable(tableName);
        dbs.endTransaction();
    }
    else {
        DbStorage::truncateTable(tableName);
    }
}

/** Set the table to insert rows into.
 * \param[in] tableName Name of the table
 */
void DbTsvStorage::setTableForInsert(std::string const& tableName) {
    _tableName = tableName;
    std::string templ = _tempPath + "/" + tableName + ".XXXXXX";
    _fileName = new char[templ.size() + 1];
    strncpy(_fileName, templ.c_str(), templ.size());
    _fileName[templ.size()] = '\0';
    int fd = mkstemp(_fileName);
    // \todo check for errors
    close(fd);
    _osp.reset(new std::ofstream(_fileName));
}

/** Get the index of a given column.  Create a new entry in the row buffer if
 * the column hasn't already been seen.  May modify the row buffer, so do not
 * call this inside "_rowBuffer[]".
 * \param[in] columnName Name of the column
 * \return Index of the column in the row buffer
 */
int DbTsvStorage::_getColumnIndex(std::string const& columnName) {
    std::map<std::string, int>::iterator i = _colMap.find(columnName);
    if (i == _colMap.end()) {
        _colMap.insert(std::pair<std::string, int>(columnName,
                                                   _rowBuffer.size()));
        _rowBuffer.push_back(std::string());
        return _rowBuffer.size() - 1;
    }
    else {
        return i->second;
    }
}

/** Set the value to insert in a given column.
 * \param[in] columnName Name of the column
 * \param[in] value Value to set in the column
 */
template <typename T>
void DbTsvStorage::setColumn(std::string const& columnName, T const& value) {
    int colIndex = _getColumnIndex(columnName);

    // set value in row buffer
    // \todo quote value as appropriate
    _convertStream.str(std::string());
    _convertStream << value;
    _rowBuffer[colIndex] = _convertStream.str();

    // \todo Optimization: if next column, output now, plus any others saved
}

// Specialization for char to persist as TINYINT instead of [VAR]CHAR(1).
template<>
void DbTsvStorage::setColumn(std::string const& columnName,
                             char const& value) {
    int colIndex = _getColumnIndex(columnName);
    _convertStream.str(std::string());
    _convertStream << static_cast<int>(value);
    _rowBuffer[colIndex] = _convertStream.str();
}

// Specializations for float and double to set precision correctly.
template<>
void DbTsvStorage::setColumn(std::string const& columnName,
                             double const& value) {
    int colIndex = _getColumnIndex(columnName);
    _convertStream.str(std::string());
    _convertStream << std::setprecision(17) << value;
    _rowBuffer[colIndex] = _convertStream.str();
}

template<>
void DbTsvStorage::setColumn(std::string const& columnName,
                             float const& value) {
    int colIndex = _getColumnIndex(columnName);
    _convertStream.str(std::string());
    _convertStream << std::setprecision(9) << value;
    _rowBuffer[colIndex] = _convertStream.str();
}

// Specialization for DateTime.
template<>
void DbTsvStorage::setColumn(std::string const& columnName,
                             DateTime const& value) {
    int colIndex = _getColumnIndex(columnName);
    _convertStream.str(std::string());
    struct tm t = value.gmtime();
    char buf[20];
    strftime(buf, sizeof(buf), "%F %T", &t);
    _rowBuffer[colIndex] = std::string(buf);
}

/** Set a given column to NULL.
 * \param[in] columnName Name of the column
 */
void DbTsvStorage::setColumnToNull(std::string const& columnName) {
    int colIndex = _getColumnIndex(columnName);
    _rowBuffer[colIndex] = "\\N";
}

/** Insert the row.
 * Row values must have been set with setColumn() calls.
 */
void DbTsvStorage::insertRow(void) {
    // Output row to stream
    for (std::vector<std::string>::const_iterator i = _rowBuffer.begin();
         i != _rowBuffer.end(); ++i) {
        if (i != _rowBuffer.begin()) *_osp << '\t';
        *_osp << *i;
    }
    // \todo Optimization: if columns all outputted, just put out endl
    *_osp << std::endl;
}


// Forward template member functions to the base class.

/** Request a column in the query output and bind a destination location.
 * \param[in] columnName Name of the column
 * \param[in] location Pointer to the destination
 *
 * The order of outParam() calls is the order of appearance in the output row.
 * Use either outColumn() or outParam() but not both.
 */
template <typename T>
void DbTsvStorage::outParam(std::string const& columnName, T* location) {
    DbStorage::outParam<T>(columnName, location);
}

/** Bind a value to a WHERE condition parameter.
 * \param[in] paramName Name of the parameter (prefixed by ":" in the WHERE
 * clause)
 * \param[in] value Value to be bound to the parameter.
 */
template <typename T>
void DbTsvStorage::condParam(std::string const& paramName, T const& value) {
    DbStorage::condParam<T>(paramName, value);
}

/** Get the value of a column of the query result row by position.
 * \param[in] pos Position of the column (starts at 0)
 */
template <typename T>
T const& DbTsvStorage::getColumnByPos(int pos) {
    return DbStorage::getColumnByPos<T>(pos);
}


// Explicit template member function instantiations.
// Ignore for doxygen processing.
//! \cond
template void DbTsvStorage::setColumn<>(std::string const& columnName, char const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, short const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, int const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, long const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, long long const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, float const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, double const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, std::string const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, bool const& value);
template void DbTsvStorage::setColumn<>(std::string const& columnName, DateTime const& value);

template void DbTsvStorage::outParam<>(std::string const& columnName, char* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, short* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, int* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, long* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, long long* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, float* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, double* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, std::string* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, bool* location);
template void DbTsvStorage::outParam<>(std::string const& columnName, DateTime* location);

template void DbTsvStorage::condParam<>(std::string const& paramName, char const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, short const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, int const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, long const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, long long const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, float const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, double const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, std::string const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, bool const& value);
template void DbTsvStorage::condParam<>(std::string const& paramName, DateTime const& value);

template char const& DbTsvStorage::getColumnByPos<>(int pos);
template short const& DbTsvStorage::getColumnByPos<>(int pos);
template int const& DbTsvStorage::getColumnByPos<>(int pos);
template long const& DbTsvStorage::getColumnByPos<>(int pos);
template long long const& DbTsvStorage::getColumnByPos<>(int pos);
template float const& DbTsvStorage::getColumnByPos<>(int pos);
template double const& DbTsvStorage::getColumnByPos<>(int pos);
template std::string const& DbTsvStorage::getColumnByPos<>(int pos);
template bool const& DbTsvStorage::getColumnByPos<>(int pos);
template DateTime const& DbTsvStorage::getColumnByPos<>(int pos);
//! \endcond

}}} // namespace lsst::daf::persistence
