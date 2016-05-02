// -*- lsst-c++ -*-

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
 
#ifndef LSST_MWI_PERSISTENCE_DBTSVSTORAGE_H
#define LSST_MWI_PERSISTENCE_DBTSVSTORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for DbTsvStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision: 2336 $
  * @date $Date$
  */

/** @class lsst::daf::persistence::DbTsvStorage
  * @brief Class for database storage with data loading from TSV files.
  *
  * Subclass of DbStorage, overriding persistence methods.
  *
  * Persists data to a database using TSV files as an intermediary for
  * performance.  Provides methods for writing rows to a table and retrieving
  * rows from a query.
  *
  * @ingroup daf_persistence
  */


#include "lsst/daf/persistence/DbStorage.h"

#include <memory>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "lsst/daf/persistence/LogicalLocation.h"

namespace lsst {
namespace daf {
namespace persistence {

class DbTsvStorage : public DbStorage {
public:
    typedef std::shared_ptr<DbTsvStorage> Ptr;

    DbTsvStorage(void);
    ~DbTsvStorage(void);

    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual void createTableFromTemplate(std::string const& tableName,
                                         std::string const& templateName,
                                         bool mayAlreadyExist = false);
    virtual void dropTable(std::string const& tableName);
    virtual void truncateTable(std::string const& tableName);

    virtual void setTableForInsert(std::string const& tableName);
    template <typename T>
    void setColumn(std::string const& columnName, T const& value);
    virtual void setColumnToNull(std::string const& columnName);
    virtual void insertRow(void);

    // Templates for forwarding to the base class.
    template <typename T> void outParam(std::string const& columnName,
                                        T* location, bool isExpr = false);
    template <typename T> void condParam(std::string const& paramName,
                                         T const& value);
    template <typename T> T const& getColumnByPos(int pos);

private:
    bool _persisting;
    bool _saveTemp;         ///< Do not delete temporary TSV file if true.
    std::string _tempPath;  ///< Directory pathname for temporary TSV file.
    char* _fileName;        ///< Full pathname for temporary TSV file.
    std::string _location;  ///< Database location URL.
    std::string _tableName;
    std::map<std::string, int> _colMap; ///< Map from column names to positions.
    std::ostringstream _convertStream;  ///< Stream to convert to text.
    std::vector<std::string> _rowBuffer;
    std::unique_ptr<std::ofstream> _osp;  ///< Output TSV stream.

    int _getColumnIndex(std::string const& columnName);
};

}}} // namespace lsst::daf::persistence

#endif
