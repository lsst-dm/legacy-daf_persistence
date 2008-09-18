// -*- lsst-c++ -*-
#ifndef LSST_MWI_PERSISTENCE_DBSTORAGELOCATION_H
#define LSST_MWI_PERSISTENCE_DBSTORAGELOCATION_H

/** @file
  * @ingroup daf_persistence
  *
  * @brief Interface for DbStorageLocation class
  *
  * @author Kian-Tat Lim (ktl@slac.stanford.edu)
  * @version $Revision$
  * @date $Date$
  */

/** @class lsst::daf::persistence::DbStorageLocation
  * @brief Location of a persisted Persistable instance in a database
  *
  * Provides database connection information for DbStorage.  Can be initialized
  * with either an all-in-one URL containing username and password information
  * or a CORAL-style connection string URL with separate username and password.
  *
  * @ingroup daf_persistence
  */

#include <boost/shared_ptr.hpp>
#include <string>

#include "lsst/daf/base/Citizen.h"
#include "lsst/daf/persistence/DbAuth.h"

namespace lsst {
namespace daf {
namespace persistence {

class DbStorageLocation : public lsst::daf::base::Citizen {
public:
    typedef boost::shared_ptr<DbStorageLocation> Ptr;

    DbStorageLocation(void);
    DbStorageLocation(std::string const& url,
                      std::string const& userName = DbAuth::username(),
                      std::string const& password = DbAuth::password());
    virtual ~DbStorageLocation(void);
    virtual std::string toString(void) const;
    virtual std::string getConnString(void) const;

    virtual std::string const& getDbType(void) const;
    virtual std::string const& getHostname(void) const;
    virtual std::string const& getPort(void) const;
    virtual std::string const& getUsername(void) const;
    virtual std::string const& getPassword(void) const;
    virtual std::string const& getDbName(void) const;

private:
    std::string _dbType;    ///< Database type (e.g. "mysql").
    std::string _hostname;
    std::string _port;
    std::string _username;
    std::string _password;
    std::string _dbName;    ///< Database (not server) name.
};

}}} // namespace lsst::daf::persistence

#endif
