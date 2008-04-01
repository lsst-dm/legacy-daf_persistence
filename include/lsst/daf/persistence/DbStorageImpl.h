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
  * Implements database using Coral library for DBMS-independence.
  * Use via DbStorage class only.
  *
  * @ingroup daf_persistence
  */

#include <boost/scoped_ptr.hpp>
#include <string>
#include <vector>

#include "SealKernel/ComponentLoader.h"
#include "CoralBase/AttributeList.h"
#include "RelationalAccess/AccessMode.h"

#include "lsst/daf/base/Citizen.h"
#include "lsst/pex/policy/Policy.h"

namespace coral {
    class IConnection;
    class ISession;
    class ITable;
    class IQuery;
    class ICursor;
} // namespace coral

namespace lsst {
namespace daf {
namespace persistence {

class LogicalLocation;

class DbStorageImpl : private lsst::daf::base::Citizen {
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

    virtual void startSession(std::string const& location,
                              coral::AccessMode am);

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

    std::string _location;
        ///< Database location string saved for use by raw MySQL interface.

    boost::scoped_ptr<coral::IConnection> _connection;
        ///< Coral database connection.
    boost::scoped_ptr<coral::ISession> _session;
        ///< Coral database session.

    coral::AttributeList _rowBuffer;    ///< Row buffer for writing.
    coral::ITable* _table;              ///< Table pointer for writing.

    boost::scoped_ptr<coral::IQuery> _query;
                                        ///< Query object pointer for reading.
    coral::ICursor* _cursor;            ///< Cursor pointer for reading.
    boost::scoped_ptr<coral::AttributeList> _condAttributeList;
        ///< List of bound variables for WHERE clause (input).
    boost::scoped_ptr<coral::AttributeList> _outAttributeList;
        ///< List of bound variables for SELECT clause (output).


    enum State { UNINITIALIZED, PENDING, INITIALIZED };
        ///< Possible states of underlying Seal/Coral infrastructure.

    static State initialized;           ///< Seal/Coral initialization state.
    static seal::Handle<seal::Context> context; ///< Seal context.
    static seal::Handle<seal::ComponentLoader> loader;
                                        ///< Seal component loader.
};

}}} // namespace lsst::daf::persistence

#endif
