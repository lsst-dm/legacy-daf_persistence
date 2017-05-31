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

#ifndef LSST_MWI_PERSISTENCE_DBSTORAGE_H
#define LSST_MWI_PERSISTENCE_DBSTORAGE_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for DbStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  *
  */

/** @class lsst::daf::persistence::DbStorage
  * @brief Class for database storage.
  *
  * Persists data to a database.  Provides methods for writing rows to a
  * table and retrieving rows from a query.
  *
  * @ingroup daf_persistence
  */


#include "lsst/daf/persistence/FormatterStorage.h"

#include <string>
#include <vector>
#include <memory>

namespace lsst {
namespace daf {
namespace persistence {

// Forward declarations
class DbStorageImpl;
class DbStorageLocation;

class DbStorage : public FormatterStorage {
public:
    typedef std::shared_ptr<DbStorage> Ptr;

    DbStorage(void);
    ~DbStorage(void);

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

    virtual void executeSql(std::string const& sqlStatement);

    virtual void setTableForInsert(std::string const& tableName);
    template <typename T>
    void setColumn(std::string const& columnName, T const& value);
    virtual void setColumnToNull(std::string const& columnName);
    virtual void insertRow(void);

    virtual void setTableForQuery(std::string const& tableName,
                                  bool isExpr = false);
    virtual void setTableListForQuery(
        std::vector<std::string> const& tableNameList);
    virtual void outColumn(std::string const& columnName, bool isExpr = false);
    template <typename T> void outParam(std::string const& columnName,
                                        T* location, bool isExpr = false);
    template <typename T> void condParam(std::string const& paramName,
                                         T const& value);
    virtual void orderBy(std::string const& expression);
    virtual void groupBy(std::string const& expression);
    virtual void setQueryWhere(std::string const& whereClause);
    virtual void query(void);
    virtual bool next(void);
    template <typename T> T const& getColumnByPos(int pos);
    bool columnIsNull(int pos);
    virtual void finishQuery(void);

protected:
    explicit DbStorage(std::type_info const& type);

private:
    std::unique_ptr<DbStorageImpl> _impl;
        ///< Implementation class for isolation.
};

}}} // namespace lsst::daf::persistence

#endif
