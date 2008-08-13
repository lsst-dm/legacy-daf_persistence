// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_DBSTORAGEIMPL_H
#define LSST_MWI_PERSISTENCE_DBSTORAGEIMPL_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for DbStorageImpl class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::DbStorageImpl
  * @brief Class for implementation of database storage.
  *
  * Use via DbStorage class only.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_array.hpp>
#include <mysql/mysql.h>
#include <string>
#include <vector>


#include "lsst/tr1/unordered_map.h"
#include "lsst/daf/base/Citizen.h"
#include "lsst/pex/policy/Policy.h"

namespace lsst {
namespace daf {
namespace persistence {

class LogicalLocation;

class BoundVar : public lsst::daf::base::Citizen {
public:
    BoundVar(void);
    explicit BoundVar(size_t size);
    explicit BoundVar(void* location);
    BoundVar(BoundVar const& src);

    enum_field_types _type;
    bool _isNull;
    bool _isUnsigned;
    unsigned long _length;
    boost::shared_ptr<char> _data;
};

class DbStorageImpl : public lsst::daf::base::Citizen {
public:
    virtual ~DbStorageImpl(void);


private:
    friend class DbStorage;

    DbStorageImpl(void);

    virtual void setPolicy(lsst::pex::policy::Policy::Ptr policy);
    virtual void setPersistLocation(LogicalLocation const& location);
    virtual void setRetrieveLocation(LogicalLocation const& location);

    virtual void startTransaction(void);
    virtual void endTransaction(void);

    virtual void startSession(std::string const& location);

    virtual void createTableFromTemplate(std::string const& tableName,
                                         std::string const& templateName,
                                         bool mayAlreadyExist);
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

    // MySQL-specific functions for implementation.
    void executeQuery(std::string const& query);
    std::string quote(std::string const& name);
    void error(std::string const& text, bool mysqlCaused = true);

    bool _readonly;
        ///< Remember if we are supposed to be read-only.
    std::string _location;
        ///< Database location string saved for use by raw MySQL interface.
    MYSQL* _db;
        ///< MySQL database connection pointer.

    std::string _insertTable;
        ///< Name of table into which to insert.
    std::vector<std::string> _queryTables;
        ///< Names of tables to select from.

    typedef std::tr1::unordered_map<std::string, BoundVar> BoundVarMap;
    BoundVarMap _inputVars;
        ///< Input variable bindings.
    BoundVarMap _outputVars;
        ///< Output variable bindings.

    // Parts of SQL statement.
    std::vector<std::string> _outColumns;
    std::string _whereClause;
    std::string _groupBy;
    std::string _orderBy;

    MYSQL_STMT* _statement;
        ///< Prepared query statement.
    MYSQL_FIELD* _resultFields;
        ///< Query result field metadata.
    int _numResultFields;
        ///< Number of result fields.
    boost::shared_array<unsigned long> _fieldLengths;
        ///< Space for lengths of result fields.
    boost::shared_array<my_bool> _fieldNulls;
        ///< Space for null flags of result fields.
};

template <>
void DbStorageImpl::setColumn<std::string>(std::string const& columnName,
                                           std::string const& value);

template <>
void DbStorageImpl::outParam<std::string>(std::string const& columnName,
                                          std::string* location);

template <>
std::string const& DbStorageImpl::getColumnByPos<std::string>(int pos);

}}} // namespace lsst::daf::persistence

#endif
