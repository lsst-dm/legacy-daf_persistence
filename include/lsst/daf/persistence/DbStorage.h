// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_DBSTORAGE_H
#define LSST_MWI_PERSISTENCE_DBSTORAGE_H

/** @file
  * @ingroup mwi
  *
  * @brief Interface for DbStorage class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  *
  */

/** @class lsst::mwi::persistence::DbStorage
  * @brief Class for database storage.
  *
  * Persists data to a database.  Provides methods for writing rows to a
  * table and retrieving rows from a query.
  *
  * @ingroup mwi
  */


#include "lsst/mwi/persistence/Storage.h"

#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

namespace lsst {
namespace mwi {
namespace persistence {

// Forward declarations
class DbStorageImpl;
class DbStorageLocation;

class DbStorage : public Storage {
public:
    typedef boost::shared_ptr<DbStorage> Ptr;

    DbStorage(void);
    ~DbStorage(void);

    virtual void setPolicy(lsst::mwi::policy::Policy::Ptr policy);
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

    virtual void setTableForQuery(std::string const& tableName);
    virtual void setTableListForQuery(
        std::vector<std::string> const& tableNameList);
    virtual void outColumn(std::string const& columnName);
    template <typename T> void outParam(std::string const& columnName,
                                        T* location);
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
    boost::scoped_ptr<DbStorageImpl> _impl;
        ///< Implementation class for isolation.
};

}}} // namespace lsst::mwi::persistence

#endif
