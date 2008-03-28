// -*- lsst-c++ -*-

/** \file
 * \brief Implementation for DbStorageLocation class
 *
 * \author $Author$
 * \version $Revision$
 * \date $Date$
 *
 * Contact: Kian-Tat Lim (ktl@slac.stanford.edu)
 * \ingroup mwi
 */

#ifndef __GNUC__
#  define __attribute__(x) /*NOTHING*/
#endif
static char const* SVNid __attribute__((unused)) = "$Id$";

#include "lsst/mwi/persistence/DbStorageLocation.h"

#include <boost/regex.hpp>

#include "lsst/mwi/exceptions.h"
#include "lsst/mwi/persistence/DbAuth.h"

namespace lsst {
namespace mwi {
namespace persistence {

/** Default constructor.
 */
DbStorageLocation::DbStorageLocation(void) :
    lsst::mwi::data::Citizen(typeid(*this)),
    _dbType(), _hostname(), _port(), _username(), _password(), _dbName() {
}

/** Constructor from CORAL-style URL plus separate username and password.
 * \param[in] url CORAL-style connection string (database type, hostname,
 * port, database name)
 * \param[in] userName User to connect as
 * \param[in] password Password for user
 */
DbStorageLocation::DbStorageLocation(std::string const& url,
                                     std::string const& userName,
                                     std::string const& password) :
    lsst::mwi::data::Citizen(typeid(*this)),
    _username(userName), _password(password) {
    boost::smatch what;
    boost::regex
        expression("(\\w+)://(\\S+):(\\d+)/(\\S+)");
    if (boost::regex_match(url, what, expression)) {
        _dbType = what[1];
        _hostname = what[2];
        _port = what[3];
        _dbName = what[4];
    }
    else {
        throw lsst::mwi::exceptions::InvalidParameter(
            "Unparseable connection string passed to DbStorageLocation: " +
            url);
    }
}

/** Destructor.
 */
DbStorageLocation::~DbStorageLocation(void) {
}

/** Produce a string (URL) representation of the DbStorageLocation.
 * \return String suitable for constructing another DbStorageLocation
 */
std::string DbStorageLocation::toString(void) const {
    return _dbType + "://" + _username + ":" + _password + "@" +
        _hostname + ":" + _port + "/" + _dbName;
}

/** Produce a CORAL-style connection string representation of the
 * DbStorageLocation.
 * \return String suitable for passing to CORAL functions
 */
std::string DbStorageLocation::getConnString(void) const {
    return _dbType + "://" + _hostname + ":" + _port + "/" + _dbName;
}

/** Accessor for database type.
 * \return Reference to database type string.
 */
std::string const& DbStorageLocation::getDbType(void) const {
    return _dbType;
}

/** Accessor for database hostname.
 * \return Reference to database hostname string.
 */
std::string const& DbStorageLocation::getHostname(void) const {
    return _hostname;
}

/** Accessor for database port number.
 * \return Reference to database port number string.
 */
std::string const& DbStorageLocation::getPort(void) const {
    return _port;
}

/** Accessor for username.
 * \return Reference to username string.
 */
std::string const& DbStorageLocation::getUsername(void) const {
    return _username;
}

/** Accessor for password.
 * \return Reference to password string.
 */
std::string const& DbStorageLocation::getPassword(void) const {
    return _password;
}

/** Accessor for database name.
 * \return Reference to database name string.
 */
std::string const& DbStorageLocation::getDbName(void) const {
    return _dbName;
}

}}} // namespace lsst::mwi::persistence
